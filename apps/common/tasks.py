import csv
import logging
from typing import Optional, List, Tuple, Dict, Any
import chardet
import requests
import mysql.connector
from datetime import datetime, timedelta, date
from constance import config
from django.db import OperationalError
from django.utils import timezone
from apps.common import UpdatingTypes, ReportTypes, UpdateStatuses
from apps.common.models import UpdateHistory, UpdateHistoryReport, UpdateHistoryRollback
from apps.common.services import get_connection_to_esep_db
from apps.common.utils import safe_int_or_none, _safe_split_ws, normalize_phone
from config.celery_app import cel_app
import time  # <<< CHANGE

logger = logging.getLogger("common")


# ==========
# DIFF HELPER
# ==========
def _diff_dict(before: dict, after: dict) -> List[Dict[str, str]]:
    """
    Возвращает список изменений в формате:
    [{"field": ..., "old": ..., "new": ...}, ...]
    Сравниваем только ключи, присутствующие в AFTER.
    Значения приводим к строке, None -> "" для удобства отображения.
    """
    def _s(v):
        return "" if v is None else str(v)

    changes: List[Dict[str, str]] = []
    after = after or {}
    before = before or {}
    for k, new_val in after.items():
        old_val = before.get(k, None)
        if old_val != new_val:
            changes.append({"field": k, "old": _s(old_val), "new": _s(new_val)})
    return changes


# ==========
# TASKS
# ==========
@cel_app.task
def run_rollback_task(update_obj_pk):
    update_instance = UpdateHistory.objects.filter(pk=update_obj_pk).first()
    if not update_instance:
        print('No instance')
        return
    update_instance.run_rollback()


@cel_app.task
def update_outdated_esep_devices_periodic_task():
    update_instance = UpdateHistory.objects.create(is_automatic=True, company_id=7)
    try:
        connection = get_connection_to_esep_db()
        try:
            connection.ping(reconnect=True, attempts=3, delay=5)
        except Exception:
            pass

        if not connection.is_connected():
            logger.error("ESEP DB connection failed")
            return

        cursor = connection.cursor(buffered=True)
        limit_date = (datetime.now() - timedelta(days=config.OUTDATED_DEVICES_PERIOD)).strftime('%Y-%m-%d')

        cursor.execute("""
            SELECT b.number AS account_number
              FROM bank_books AS b
              LEFT JOIN devices AS d ON b.id = d.bankbook_id
             WHERE b.company_id = %s
               AND (
                    (d.updated_at <= %s AND d.deleted_at IS NULL)
                    OR d.id IS NULL
               )
          GROUP BY b.number
          ORDER BY MIN(d.updated_at)
             LIMIT 100000
        """, (update_instance.company_id, limit_date))
        rows = cursor.fetchall()
        cursor.close()
        connection.close()

        numeric_accounts = [str(acc).strip() for (acc,) in rows if str(acc).strip().isdigit()]

        if numeric_accounts:
            update_esep_db_task.delay(update_obj_pk=update_instance.pk, account_numbers=numeric_accounts)
        else:
            logger.info("No outdated accounts")
    except Exception as e:
        msg = str(e)
        logger.error(msg)
        update_instance.status = UpdateStatuses.FAILED
        update_instance.status_reason = msg
        update_instance.completed_at = timezone.now()
        update_instance.save()


@cel_app.task(bind=True, max_retries=5)
def update_esep_db_task(self, update_obj_pk: int, account_numbers: list = None):
    update_instance = UpdateHistory.objects.filter(pk=update_obj_pk).first()
    if not update_instance:
        raise self.retry(exc=OperationalError("Instance not found"))

    try:
        total_report_json = None

        if update_instance.updating_type == UpdatingTypes.BY_API:
            if not account_numbers:
                account_numbers = read_accounts_csv_file(update_instance.data_file.path)

            accounts_data: Dict[str, Dict[str, Any]] = {}
            empty_accounts: List[str] = []

            for account_number in account_numbers:
                ACA_SERVICE_URL = (
                    config.ACA_SERVICE_HOST
                    + "/api/getValueByAccountNumber?accountNumber={account_number}&apikey={api_key}"
                )
                api_url = ACA_SERVICE_URL.format(
                    account_number=str(account_number).strip(),
                    api_key=config.ACA_SERVICE_API_KEY
                )

                try:
                    response = requests.get(api_url, verify=False, timeout=10)
                except requests.exceptions.RequestException as req_err:
                    logger.error(f"ACA API request error: {req_err}")
                    continue

                if response.status_code != 200:
                    # 404 — пустой аккаунт
                    if response.status_code == 404:
                        empty_accounts.append(account_number)
                    else:
                        # возможно wrong apikey в JSON
                        try:
                            error_data = response.json()
                            if error_data.get("error") == "wrong apikey":
                                raise Exception("wrong apikey")
                        except ValueError:
                            pass
                    continue

                # ожидаем JSON
                if not (response.headers.get('Content-Type') or '').startswith('application/json'):
                    logger.error(f"Unexpected Content-Type from ACA: {response.headers.get('Content-Type')}")
                    continue

                try:
                    account_data = response.json()
                except ValueError:
                    logger.error("ACA API returned invalid JSON")
                    continue

                # нет счётчиков — фиксируем пустые
                if not account_data or not account_data.get('counters'):
                    empty_accounts.append(account_number)
                    continue

                # поддержка телефона
                phone_from_api = account_data.get('phone') or account_data.get('phoneOwner')
                phone_from_api = str(phone_from_api) if phone_from_api is not None else None

                devices = [
                    {
                        "device_number": c.get('factory', ''),
                        "modem": c.get('modem', ""),
                        "source": c.get('source', ''),
                        "last_check": c['previousVerificationDate'][:10] if c.get('previousVerificationDate') else None,
                        "next_check": c['nextVerificationDate'][:10] if c.get('nextVerificationDate') else None,
                        "water_type": 1 if c.get('isCold', True) else 2,
                        "value": safe_int_or_none(c.get('value')),
                    }
                    for c in account_data.get('counters')
                ]

                acc_key = str(account_data.get('accountNumber') or account_number).strip()
                accounts_data[acc_key] = {
                    "devices": devices,
                    "phone": phone_from_api
                }

            # синхронизация (название функции историческое)
            total_report_json = synchronize_esep_db_with_aca_data(accounts_data, update_obj_pk, company_id=update_instance.company_id)
            total_report_json[ReportTypes.EMPTY_BODY_FROM_VODOKANAL_API] = empty_accounts

        elif update_instance.updating_type == UpdatingTypes.BY_FILE:
            csv_data = get_aca_devices_from_csv_file(update_instance.data_file.path)
            total_report_json = synchronize_esep_db_with_aca_data(csv_data, update_obj_pk, company_id=update_instance.company_id)

        else:
            raise Exception(f"Unknown updating_type: {update_instance.updating_type}")

        # Сохранение отчётов
        if total_report_json:
            UpdateHistoryRollback.objects.create(
                update_history=update_instance,
                data_json=total_report_json.pop('rollback_data', None)
            )
            UpdateHistoryReport.objects.bulk_create([
                UpdateHistoryReport(
                    update_history=update_instance,
                    report_type=str(report_type).upper(),
                    data_json={
                        "amount_total": len(total_report_json[report_type]),
                        "items": list(total_report_json[report_type])
                    },
                ) for report_type in total_report_json
            ])

        update_instance.status = UpdateStatuses.SUCCESS

    except Exception as e:
        msg = str(e)
        logger.error(msg)
        update_instance.status = UpdateStatuses.FAILED
        if "HTTPSConnectionPool" in msg:
            msg = "Не доходит запрос до АСА API сервиса: " + msg
        update_instance.status_reason = msg

    finally:
        update_instance.completed_at = timezone.now()
        update_instance.save()


def get_params_for_update(aca_device, current_next_check=None):
    """Формирует пары ключ-значение для UPDATE devices; пустой набор не допускается к UPDATE выше."""
    params_dict = {"keys": [], "values": []}

    if aca_device.get('device_number'):
        params_dict['keys'].append("number")
        params_dict['values'].append(str(aca_device['device_number']))

    if aca_device.get('next_check') and str(aca_device['next_check']) not in ["(пусто)", "None"]:
        params_dict['keys'].append("next_check")
        params_dict['values'].append(str(aca_device['next_check']))

    if aca_device.get('last_check') and str(aca_device['last_check']) not in ["(пусто)", "None"]:
        params_dict['keys'].append("last_check")
        params_dict['values'].append(str(aca_device['last_check']))

    if aca_device.get('modem') and str(aca_device['modem']) != "(пусто)":
        params_dict['keys'].append("modem_number")
        raw = aca_device['modem']
        if isinstance(raw, (list, tuple)):
            raw = " ".join(map(str, raw))
        elif not isinstance(raw, (str, bytes, int, float)):
            raw = str(raw)
        modem_str = str(raw)
        parts = _safe_split_ws(modem_str)
        modem_norm = (parts[0].strip() if parts else modem_str.strip())
        params_dict['values'].append(modem_norm)

    if aca_device.get('water_type') is not None:
        try:
            water_type = int(aca_device['water_type'])
        except Exception:
            water_type = 1 if str(aca_device['water_type']).strip() == "1" else 2
        water_name = "ХВС" if water_type == 1 else "ГВС"
        params_dict['keys'].extend(["name_ru", "name_kk", "name_en"])
        params_dict['values'].extend([water_name, water_name, water_name])
        params_dict['keys'].append("resource_type_id")
        params_dict['values'].append(water_type)

    return params_dict

def synchronize_esep_db_with_aca_data(
    aca_loaded_data: Dict[str, Any],
    update_obj_pk: int,
    company_id: Optional[int] = None
):
    """
    Синхронизация ESEP с внешними данными (унифицированный формат).
    Если задан company_id — ограничиваемся ЛС/устройствами этой компании.
    """
    total_report_json = {
        ReportTypes.IN_VODOKANAL_NOT_IN_ESEP: [],
        ReportTypes.IN_VODOKANAL_NO_ACCOUNT_IN_ESEP: set(),
        ReportTypes.ZERO_IN_VODOKANAL_NOT_IN_ESEP: [],
        ReportTypes.IN_ESEP_NOT_IN_VODOKANAL: [],
        ReportTypes.ADDED_NEW_DEVICES: [],
        ReportTypes.REMOVED_REDUNDANT_DEVICES: [],
        ReportTypes.SUCCESSFULLY_UPDATED_DEVICES: [],
        ReportTypes.EMPTY_BODY_FROM_VODOKANAL_API: [],
        'rollback_data': {
            'bank_books': [],
            'update': {'devices': [], 'indications': []},
            'delete': {'devices': [], 'indications': []},
            'reset_deletion': {'devices': [], 'indications': []},
            'create': {'devices': [], 'indications': []},
        },
        # Новые детализированные отчёты
        'DETAILED_DEVICE_UPDATES': [],     # [{ account_number, device_id, device_number, before, after, changes }]
        'DETAILED_DEVICE_DELETIONS': [],   # [{ account_number, device_id, device_number, before, after=None, changes, action=deleted }]
        'DETAILED_DEVICE_ADDED': [],       # [{ account_number, device_id, device_number, before=None, after, changes, action=created }]
        'DETAILED_BANKBOOK_UPDATES': [],   # [{ account_number, bankbook_id, before, after, changes }]
    }

    def remove_first_occurrence(device_list, device_number):
        for i, device in enumerate(device_list):
            if str(device.get('device_number')) == str(device_number):
                del device_list[i]
                break

    # --- Транзакция с мягкими сессионными настройками и retry ---  # <<< CHANGE
    MAX_RETRIES = 3
    backoff = 1.0

    for attempt in range(1, MAX_RETRIES + 1):
        connection = None
        cursor = None
        try:
            # подключение
            connection = get_connection_to_esep_db()
            if not connection.is_connected():
                raise Exception("DB not connected")
            cursor = connection.cursor(buffered=True)
            connection.autocommit = False

            # важные сессионные настройки против блокировок
            cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
            cursor.execute("SET SESSION innodb_lock_wait_timeout = %s", (60,))

            cursor.execute("SELECT DATABASE();")
            _ = cursor.fetchone()

            # Проходим аккаунты в стабильном порядке (уменьшает конфликты lock-order)
            items_iter = sorted((aca_loaded_data or {}).items(), key=lambda kv: str(kv[0]))

            for aca_account_number, payload in items_iter:
                # формат источника
                if isinstance(payload, dict) and 'devices' in payload:
                    aca_devices = payload.get('devices', []) or []
                    aca_phone = normalize_phone(payload.get('phone') or payload.get('telefon'))
                    new_name = (str(payload.get('fio') or '').strip() or None)
                    new_people = safe_int_or_none(payload.get('people'))
                else:
                    aca_devices = payload or []
                    aca_phone, new_name, new_people = None, None, None

                if not aca_account_number or not str(aca_account_number).strip().isdigit():
                    logger.warning(f"Skip invalid account key: '{aca_account_number}'")
                    continue

                account_id = int(str(aca_account_number).strip())

                # банкбук
                if company_id is None:
                    cursor.execute(
                        "SELECT id, company_id, phone, name, people FROM bank_books WHERE number = %s",
                        (account_id,)
                    )
                else:
                    cursor.execute(
                        "SELECT id, company_id, phone, name, people FROM bank_books WHERE number = %s AND company_id = %s",
                        (account_id, company_id)
                    )
                bankbook = cursor.fetchone()
                if not bankbook:
                    total_report_json[ReportTypes.IN_VODOKANAL_NO_ACCOUNT_IN_ESEP].add(str(aca_account_number))
                    continue

                bankbook_id, bb_company_id, current_phone, current_name, current_people = (
                    bankbook[0],
                    bankbook[1],
                    normalize_phone(bankbook[2]),
                    (str(bankbook[3]) or '').strip() if bankbook[3] is not None else None,
                    bankbook[4],
                )

                # устройства (строго компании bankbook'а)
                cursor.execute("""
                    SELECT b.number  AS account_number,
                           d.id      AS device_id,
                           i.id      AS indication_id,
                           d.number  AS device_number,
                           i.value,
                           i.created_at, i.updated_at,
                           d.last_check, d.next_check,
                           d.company_id, d.resource_type_id,
                           d.modem_number,
                           d.name_ru, d.name_kk, d.name_en,
                           d.comment, d.updated_by, d.updated_at,
                           i.deleted_at
                      FROM devices d
                 LEFT JOIN indications i ON d.id = i.device_id
                 INNER JOIN bank_books b ON b.id = d.bankbook_id
                     WHERE b.number = %s
                       AND b.company_id = %s
                       AND d.company_id = %s
                       AND d.deleted_at IS NULL
                       AND (
                            i.id = (SELECT i2.id
                                      FROM indications i2
                                     WHERE i2.device_id = d.id
                                  ORDER BY i2.updated_at DESC, i2.id DESC
                                     LIMIT 1)
                            OR i.id IS NULL
                       )
                """, (account_id, bb_company_id, bb_company_id))
                esep_db_data = cursor.fetchall()

                esep_devices, devices_without_indications = [], []
                devices_for_delete, excluding_changed = {}, []

                for el in esep_db_data:
                    if el[2] is None or el[18] is not None:
                        devices_without_indications.append({
                            "id": el[1],
                            "device_number": el[3],
                            "last_check": el[7],
                            "next_check": el[8],
                            "company_id": el[9],
                            "resource_type_id": el[10],
                            "modem_number": el[11],
                            "name_ru": el[12],
                            "name_kk": el[13],
                            "name_en": el[14],
                            "comment": el[15],
                            "updated_by": el[16],
                            "updated_at": el[17],
                        })
                    else:
                        esep_devices.append({
                            "id": el[1],
                            "indication_id": el[2],
                            "device_number": el[3],
                            "value": safe_int_or_none(el[4]),
                            "last_check": el[7],
                            "next_check": el[8],
                            "company_id": el[9],
                            "resource_type_id": el[10],
                            "modem_number": el[11],
                            "name_ru": el[12],
                            "name_kk": el[13],
                            "name_en": el[14],
                            "comment": el[15],
                            "updated_by": el[16],
                            "updated_at": el[17],
                        })

                # пары
                device_pairs, account_report_data = make_pairs(aca_devices, esep_devices, str(aca_account_number))

                # обновление bank_books (phone/name/people): детальный дифф
                sets, values = [], []
                before_bank = {
                    "phone": current_phone,
                    "name": current_name,
                    "people": current_people,
                }
                after_bank_delta = {}
                if aca_phone and aca_phone != current_phone:
                    sets.append("phone = %s")
                    values.append(aca_phone)
                    after_bank_delta["phone"] = aca_phone
                if new_name is not None and new_name != current_name:
                    sets.append("name = %s")
                    values.append(new_name)
                    after_bank_delta["name"] = new_name
                if new_people is not None and new_people != current_people:
                    sets.append("people = %s")
                    values.append(new_people)
                    after_bank_delta["people"] = new_people

                if sets:
                    after_bank = before_bank.copy()
                    after_bank.update(after_bank_delta)
                    changes = _diff_dict(before_bank, after_bank)
                    if changes:
                        total_report_json['DETAILED_BANKBOOK_UPDATES'].append({
                            "account_number": str(aca_account_number),
                            "bankbook_id": bankbook_id,
                            "before": before_bank,
                            "after": after_bank,
                            "changes": changes,
                        })

                    # rollback для bank_books
                    total_report_json['rollback_data']['bank_books'].append((
                        bankbook_id, current_phone, current_name, current_people,
                        f"ninja_update_rollback {str(update_obj_pk)}", 693, "now()"
                    ))
                    sets.append("updated_by = 693")
                    sets.append("updated_at = NOW()")
                    sql = f"UPDATE bank_books SET {', '.join(sets)} WHERE id = %s"
                    values.append(bankbook_id)
                    cursor.execute(sql, tuple(values))

                # апдейты пар устройств (детальные before/after)
                # выполняем UPDATE по id в возр. порядке — стабильный lock-order
                for aca_device, esep_device in sorted(device_pairs, key=lambda p: p[1]['id']):
                    current_next_check = esep_device['next_check'].strftime('%Y-%m-%d') if esep_device['next_check'] else None
                    params_dict = get_params_for_update(aca_device, current_next_check=current_next_check)
                    if not params_dict['keys']:
                        continue  # минимальная проверка: не делаем пустых апдейтов

                    before_dev = {
                        "number": esep_device['device_number'],
                        "modem_number": esep_device['modem_number'],
                        "last_check": esep_device['last_check'].strftime('%Y-%m-%d') if esep_device['last_check'] else None,
                        "next_check": current_next_check,
                        "name_ru": esep_device['name_ru'],
                        "name_kk": esep_device['name_kk'],
                        "name_en": esep_device['name_en'],
                        "resource_type_id": esep_device['resource_type_id'],
                    }
                    after_dev = before_dev.copy()
                    for k, v in zip(params_dict['keys'], params_dict['values']):
                        after_dev[k] = v

                    changes = _diff_dict(before_dev, after_dev)
                    if changes:
                        total_report_json['DETAILED_DEVICE_UPDATES'].append({
                            "account_number": str(aca_account_number),
                            "device_id": esep_device['id'],
                            "device_number": esep_device['device_number'],
                            "before": before_dev,
                            "after": after_dev,
                            "changes": changes,
                        })

                    set_clause = ",".join(f"`{k}` = %s" for k in params_dict['keys'])
                    cursor.execute(
                        f"UPDATE devices SET {set_clause}, comment = %s, updated_by = 693, updated_at = NOW() WHERE id = %s",
                        (*params_dict['values'], f"ninja_update {str(update_obj_pk)}", esep_device['id'])
                    )

                    total_report_json['rollback_data']['update']['devices'].append((
                        esep_device['id'],
                        esep_device['last_check'].strftime('%Y-%m-%d') if esep_device['last_check'] else None,
                        current_next_check,
                        esep_device['modem_number'],
                        esep_device['name_ru'],
                        esep_device['name_kk'],
                        esep_device['name_en'],
                        esep_device['resource_type_id'],
                        esep_device['device_number'],
                        f"ninja_update_rollback {str(update_obj_pk)}", 693, "now()",
                    ))
                    total_report_json[ReportTypes.SUCCESSFULLY_UPDATED_DEVICES].append(
                        (str(aca_account_number), aca_device.get('device_number'))
                    )

                # устройства без показаний (детальные before/after при апдейте)
                if devices_without_indications:
                    for dwi in sorted(devices_without_indications, key=lambda x: x['id']):
                        first_match_device = None
                        needed_rtype = None
                        if account_report_data[ReportTypes.IN_VODOKANAL_NOT_IN_ESEP]:
                            needed_rtype = ReportTypes.IN_VODOKANAL_NOT_IN_ESEP
                            first_match_device = next(
                                (t for t in account_report_data[needed_rtype]
                                 if str(t.get('device_number')) == str(dwi['device_number'])), None)

                        if not first_match_device and account_report_data[ReportTypes.ADDED_NEW_DEVICES]:
                            needed_rtype = ReportTypes.ADDED_NEW_DEVICES
                            first_match_device = next(
                                (t for t in account_report_data[needed_rtype]
                                 if str(t.get('device_number')) == str(dwi['device_number'])), None)

                        if not first_match_device:
                            devices_for_delete[dwi['id']] = dwi['device_number']
                            continue

                        current_next_check = dwi['next_check'].strftime('%Y-%m-%d') if dwi['next_check'] else None
                        params_dict = get_params_for_update(first_match_device, current_next_check=current_next_check)
                        if not params_dict['keys']:
                            continue

                        before_dev = {
                            "number": dwi['device_number'],
                            "modem_number": dwi['modem_number'],
                            "last_check": dwi['last_check'].strftime('%Y-%m-%d') if dwi['last_check'] else None,
                            "next_check": current_next_check,
                            "name_ru": dwi['name_ru'],
                            "name_kk": dwi['name_kk'],
                            "name_en": dwi['name_en'],
                            "resource_type_id": dwi['resource_type_id'],
                        }
                        after_dev = before_dev.copy()
                        for k, v in zip(params_dict['keys'], params_dict['values']):
                            after_dev[k] = v

                        changes = _diff_dict(before_dev, after_dev)
                        if changes:
                            total_report_json['DETAILED_DEVICE_UPDATES'].append({
                                "account_number": str(aca_account_number),
                                "device_id": dwi['id'],
                                "device_number": dwi['device_number'],
                                "before": before_dev,
                                "after": after_dev,
                                "changes": changes,
                            })

                        set_clause = ",".join(f"`{k}` = %s" for k in params_dict['keys'])
                        cursor.execute(
                            f"UPDATE devices SET {set_clause}, comment = %s, updated_by = 693, updated_at = NOW() WHERE id = %s",
                            (*params_dict['values'], f"ninja_update {str(update_obj_pk)}", dwi['id'])
                        )

                        total_report_json['rollback_data']['update']['devices'].append((
                            dwi['id'],
                            dwi['last_check'].strftime('%Y-%m-%d') if dwi['last_check'] else None,
                            current_next_check,
                            dwi['modem_number'],
                            dwi['name_ru'],
                            dwi['name_kk'],
                            dwi['name_en'],
                            dwi['resource_type_id'],
                            dwi['device_number'],
                            f"ninja_update_rollback {str(update_obj_pk)}", 693, "now()",
                        ))
                        total_report_json[ReportTypes.SUCCESSFULLY_UPDATED_DEVICES].append(
                            (str(aca_account_number), dwi['device_number'])
                        )
                        remove_first_occurrence(account_report_data[needed_rtype], dwi['device_number'])
                        excluding_changed.append(dwi['id'])

                    if devices_for_delete:
                        # Детальный отчёт по пометке на удаление — снимок "до"
                        for dwi in devices_without_indications:
                            if dwi['id'] in devices_for_delete:
                                total_report_json['DETAILED_DEVICE_DELETIONS'].append({
                                    "account_number": str(aca_account_number),
                                    "device_id": dwi['id'],
                                    "device_number": dwi['device_number'],
                                    "before": {
                                        "number": dwi['device_number'],
                                        "modem_number": dwi['modem_number'],
                                        "last_check": dwi['last_check'].strftime('%Y-%m-%d') if dwi['last_check'] else None,
                                        "next_check": dwi['next_check'].strftime('%Y-%m-%d') if dwi['next_check'] else None,
                                        "name_ru": dwi['name_ru'],
                                        "name_kk": dwi['name_kk'],
                                        "name_en": dwi['name_en'],
                                        "resource_type_id": dwi['resource_type_id'],
                                    },
                                    "after": None,
                                    "changes": [{"field": "deleted_at", "old": "", "new": "NOW()"}],
                                    "action": "deleted",
                                })

                        total_report_json['rollback_data']['reset_deletion']['devices'].extend(list(devices_for_delete.keys()))
                        placeholders = ', '.join(['%s'] * len(devices_for_delete))
                        # фиксируем порядок по id в IN через сортировку ключей — детерминированный план  # <<< CHANGE
                        ids_sorted = sorted(devices_for_delete.keys())
                        sql_query = (
                            f"UPDATE devices SET deleted_by = 693, deleted_at = NOW(), "
                            f"comment = 'ninja_update {str(update_obj_pk)}' "
                            f"WHERE id IN ({', '.join(['%s'] * len(ids_sorted))})"
                        )
                        cursor.execute(sql_query, ids_sorted)
                        for did in ids_sorted:
                            dnumber = devices_for_delete[did]
                            total_report_json[ReportTypes.REMOVED_REDUNDANT_DEVICES].append((str(aca_account_number), dnumber, did))

                # добавления новых устройств (детальный отчёт "после")
                if account_report_data[ReportTypes.ADDED_NEW_DEVICES]:
                    exclude_ids = total_report_json['rollback_data']['delete']['devices'] + excluding_changed
                    excluding_ids = f"AND d.id NOT IN ({', '.join(['%s'] * len(exclude_ids))})" if exclude_ids else ""
                    for new_device in account_report_data[ReportTypes.ADDED_NEW_DEVICES]:
                        params_dict = get_params_for_update(new_device)
                        if not params_dict['keys']:
                            continue

                        cursor.execute(f"""
                            SELECT d.id
                              FROM devices d
                              JOIN bank_books b ON b.id = d.bankbook_id
                             WHERE d.number = %s
                               AND b.number = %s
                               AND b.company_id = %s
                               AND d.company_id = %s
                               AND d.deleted_at IS NULL
                               {excluding_ids}
                             ORDER BY d.id DESC
                        """, (new_device.get('device_number'), str(aca_account_number), bb_company_id, bb_company_id, *exclude_ids))
                        device_row = cursor.fetchone()

                        if device_row:
                            device_id = device_row[0]
                        else:
                            cursor.execute(
                                "INSERT INTO devices (" + ", ".join(
                                    f"`{key}`" for key in params_dict['keys']) +
                                ", bankbook_id, company_id, comment, area_type, created_by, created_at, updated_at) "
                                "VALUES(" + ", ".join("%s" for _ in params_dict['values']) + ", %s, %s, %s, %s, 693, NOW(), NOW())",
                                (*params_dict['values'], bankbook_id, bb_company_id, f"ninja_update {str(update_obj_pk)}", 0)
                            )
                            device_id = cursor.lastrowid
                            total_report_json['rollback_data']['delete']['devices'].append(device_id)

                            # Детальный "added" снимок (after)
                            after_dev = {}
                            for k, v in zip(params_dict['keys'], params_dict['values']):
                                after_dev[k] = v
                            total_report_json['DETAILED_DEVICE_ADDED'].append({
                                "account_number": str(aca_account_number),
                                "device_id": device_id,
                                "device_number": new_device.get('device_number'),
                                "before": None,
                                "after": after_dev,
                                "changes": [{"field": k, "old": "", "new": str(v)} for k, v in after_dev.items()],
                                "action": "created",
                            })

                            total_report_json[ReportTypes.ADDED_NEW_DEVICES].append(
                                (str(aca_account_number), new_device.get('device_number'))
                            )

                # сбор хвостов отчётов
                total_report_json[ReportTypes.ZERO_IN_VODOKANAL_NOT_IN_ESEP].extend(
                    account_report_data[ReportTypes.ZERO_IN_VODOKANAL_NOT_IN_ESEP]
                )
                total_report_json[ReportTypes.IN_ESEP_NOT_IN_VODOKANAL].extend(
                    account_report_data[ReportTypes.IN_ESEP_NOT_IN_VODOKANAL]
                )
                for d in account_report_data[ReportTypes.IN_VODOKANAL_NOT_IN_ESEP]:
                    total_report_json[ReportTypes.IN_VODOKANAL_NOT_IN_ESEP].append(
                        (str(aca_account_number), d.get('device_number'))
                    )

            # commit
            connection.commit()
            break  # успех, выходим из retry-цикла

        except mysql.connector.Error as e:
            # полный откат для «всё или ничего»
            try:
                if connection:
                    connection.rollback()
            except Exception:
                pass

            # Deadlock (1213) / Lock wait timeout (1205): аккуратно повторим  # <<< CHANGE
            if getattr(e, "errno", None) in (1205, 1213) and attempt < MAX_RETRIES:
                logger.warning(f"Transaction retry {attempt}/{MAX_RETRIES} after MySQL error {e.errno}: {e}")
                time.sleep(backoff)
                backoff *= 2
                continue
            logger.error(f"SYNC error (no retry), rolled back: {e}")
            raise
        except Exception as e:
            try:
                if connection:
                    connection.rollback()
            except Exception:
                pass
            logger.error(f"SYNC error, rolled back: {e}")
            raise
        finally:
            try:
                if cursor:
                    cursor.close()
            except Exception:
                pass
            try:
                if connection and connection.is_connected():
                    connection.close()
            except Exception:
                pass

    return total_report_json


def make_pairs(aca_devices: list, esep_devices: list, account_number=''):
    """
    Сопоставление устройств по номеру, затем по близости value.
    Возвращает пары (aca_device, esep_device) и reports_data с новыми типами.
    """
    pairs: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    untitled_devices: List[Dict[str, Any]] = []

    reports_data = {
        ReportTypes.IN_VODOKANAL_NOT_IN_ESEP: [],
        ReportTypes.ZERO_IN_VODOKANAL_NOT_IN_ESEP: [],
        ReportTypes.IN_ESEP_NOT_IN_VODOKANAL: [],
        ReportTypes.ADDED_NEW_DEVICES: [],
    }

    if len(esep_devices) == 0:
        reports_data[ReportTypes.ADDED_NEW_DEVICES].extend(aca_devices)
        return [], reports_data

    # по номеру
    for aca_device in aca_devices:
        aca_dvc_num = str(aca_device.get('device_number')).lower()
        if any(aca_dvc_num == x for x in ["(пусто)", "0"]) or "без номера" in aca_dvc_num:
            untitled_devices.append(aca_device)
        else:
            matches = [x for x in esep_devices if x.get('device_number') == aca_device.get('device_number')]
            if matches:
                esep_device = matches[0]
                pairs.append((aca_device, esep_device))
                esep_devices.remove(esep_device)
            else:
                if aca_device.get('value') is None:
                    reports_data[ReportTypes.ZERO_IN_VODOKANAL_NOT_IN_ESEP].append((account_number, aca_device.get('device_number')))
                else:
                    untitled_devices.append(aca_device)

    # хвосты / сопоставление по value
    if len(untitled_devices) == 0 and len(esep_devices) > 0:
        for i in esep_devices:
            reports_data[ReportTypes.IN_ESEP_NOT_IN_VODOKANAL].append((account_number, i['device_number']))
    elif len(untitled_devices) > 0 and len(esep_devices) == 0:
        for i in untitled_devices:
            reports_data[ReportTypes.IN_VODOKANAL_NOT_IN_ESEP].append(i)
    elif len(untitled_devices) > 0 and len(esep_devices) > 0:
        def _dist(a, b):
            av = a.get('value') or 0
            bv = b.get('value') or 0
            try:
                return abs(int(av) - int(bv))
            except Exception:
                return 0

        while untitled_devices and esep_devices:
            esep_device = esep_devices.pop(0)
            fit = min(untitled_devices, key=lambda x: _dist(x, esep_device))
            pairs.append((fit, esep_device))
            untitled_devices.remove(fit)

        for device in esep_devices:
            reports_data[ReportTypes.IN_ESEP_NOT_IN_VODOKANAL].append((account_number, device['device_number']))

        for device in untitled_devices:
            reports_data[ReportTypes.IN_VODOKANAL_NOT_IN_ESEP].append(device)

    return pairs, reports_data


def check_source(source):
    return bool(str(source or '').strip().lower() not in ["system", "система"])


def read_accounts_csv_file(file_path) -> list:
    """Возвращает список строк-ЛС. Валидирует, что ЛС — число; иначе warning и пропуск."""
    accounts: List[str] = []
    try:
        detected_encoding = 'utf-8'
        with open(file_path, 'rb') as f:
            raw_data = f.read()
            detected_encoding = chardet.detect(raw_data)['encoding'] or 'utf-8'

        with open(file_path, 'r', encoding=detected_encoding) as file:
            reader = csv.reader(file, delimiter=';')
            for i, row in enumerate(reader):
                if i == 0:
                    continue
                acc = (row[0] or '').strip()
                if not acc or not acc.isdigit():
                    logger.warning(f"Пропуск строки {i + 1}: ЛС не число: '{row[0]}'")
                    continue
                accounts.append(acc)

    except FileNotFoundError:
        logger.error("File not found or path is incorrect.")
    except Exception as e:
        logger.error(f"Error occurred: {e}")

    return accounts


def get_aca_devices_from_csv_file(file_path):
    """
    Преобразует CSV ACA в унифицированный формат:
    {
      "<account_number>": {
         "devices": [{...}],
         "phone": None
      }
    }
    """
    csv_data: Dict[str, Dict[str, Any]] = {}
    try:
        with open(file_path, 'r', encoding='windows-1251') as file:
            reader = csv.reader(file, delimiter=';')
            for i, row in enumerate(reader):
                if i < 60:
                    continue

                account_number = (row[10] or '').strip()
                if not account_number or not account_number.isdigit():
                    logger.warning(f"CSV ACA: пропуск строки {i + 1}: ЛС некорректен: '{row[10]}'")
                    continue

                if row[12] and not row[22]:
                    logger.debug(f"CSV ACA: странная строка (есть номер, нет показаний): {row}")

                entry = csv_data.setdefault(account_number, {"devices": [], "phone": None})
                entry["devices"].append({
                    "device_number": row[12],
                    "modem": row[13],
                    "source": row[24],
                    "next_check": row[16],
                    "water_type": row[17],
                    "value": safe_int_or_none((row[22] or '').strip()),
                })

                # ограничитель по образцу (при необходимости убрать)
                if i == 110:
                    break

    except FileNotFoundError:
        logger.error("File not found or path is incorrect.")
    except Exception as e:
        logger.error(f"Error occurred: {e}")

    return csv_data


# ==========
# Поддержка Кокшетау (KOK)
# ==========
def _parse_kok_response_to_account_payload(json_obj: dict) -> dict:
    """Преобразует ответ КОК API в унифицированный payload."""
    raw_phone = json_obj.get("telefon")
    raw_phone = str(raw_phone).strip() if raw_phone is not None else ""
    phone = normalize_phone(raw_phone) if raw_phone else None

    fio = (str(json_obj.get("fio") or "").strip() or None)
    people = safe_int_or_none(json_obj.get("people"))

    devices = []
    counters = json_obj.get("counter", []) or []

    def _d(s):
        if not s:
            return None
        s = str(s).strip()
        for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
            except Exception:
                pass
        return None

    for c in counters:
        t = str(c.get("type_counter") or "").strip().lower()
        water_type = 1 if "холод" in t else 2
        devices.append({
            "device_number": str(c.get("technumber") or "").strip(),
            "modem": "",
            "last_check": None,
            "next_check": _d(c.get("date_poverka")),
            "water_type": water_type,
            "value": safe_int_or_none(c.get("pokaz")),
        })

    return {
        "devices": devices,
        "phone": phone,
        "fio": fio,
        "people": people,
    }


@cel_app.task(bind=True, max_retries=5)
def update_kok_db_task(self, update_obj_pk: int, account_numbers: list = None):
    COMPANY_ID = 10
    update_instance = UpdateHistory.objects.filter(pk=update_obj_pk).first()
    if not update_instance:
        raise self.retry(exc=OperationalError("Instance not found"))

    try:
        total_report_json = None

        if update_instance.updating_type == UpdatingTypes.BY_API:
            if not account_numbers:
                account_numbers = read_accounts_csv_file(update_instance.data_file.path) if update_instance.data_file else []

            headers = {"Authorization": "Basic MTox", "Accept": "application/json"}
            accounts_data, empty_accounts = {}, []

            for rca in account_numbers:
                url = f"http://suarnasik.hopto.org:4444/abon/hs/upr/people?rca={str(rca).strip()}"
                try:
                    resp = requests.get(url, headers=headers, timeout=10)
                except requests.exceptions.RequestException as e:
                    logger.error(f"[KOK] request error for {rca}: {e}")
                    continue

                if resp.status_code != 200:
                    if resp.status_code == 404:
                        empty_accounts.append(rca)
                    else:
                        logger.warning(f"[KOK] {resp.status_code}: {resp.text[:200]}")
                    continue

                # сервер может не выставлять Content-Type
                try:
                    data = resp.json()
                except ValueError:
                    import json
                    data = json.loads(resp.content.decode("utf-8-sig", errors="replace"))

                if not data or str(data.get("success")).lower() != "true":
                    empty_accounts.append(rca)
                    continue

                payload = _parse_kok_response_to_account_payload(data)
                acc_number = str(data.get("terminal") or rca).strip()
                accounts_data[acc_number] = payload

            total_report_json = synchronize_esep_db_with_aca_data(
                accounts_data, update_obj_pk, company_id=COMPANY_ID
            )
            total_report_json[ReportTypes.EMPTY_BODY_FROM_VODOKANAL_API] = empty_accounts

        elif update_instance.updating_type == UpdatingTypes.BY_FILE:
            csv_data = get_kok_devices_from_csv_file(update_instance.data_file.path)
            total_report_json = synchronize_esep_db_with_aca_data(
                csv_data, update_obj_pk, company_id=COMPANY_ID
            )

        else:
            raise Exception(f"Unknown updating_type: {update_instance.updating_type}")

        if total_report_json:
            UpdateHistoryRollback.objects.create(
                update_history=update_instance,
                data_json=total_report_json.pop('rollback_data', None)
            )
            UpdateHistoryReport.objects.bulk_create([
                UpdateHistoryReport(
                    update_history=update_instance,
                    report_type=str(report_type).upper(),
                    data_json={
                        "amount_total": len(total_report_json[report_type]),
                        "items": list(total_report_json[report_type])
                    },
                ) for report_type in total_report_json
            ])

        update_instance.status = UpdateStatuses.SUCCESS

    except Exception as e:
        msg = str(e)
        logger.error(msg)
        update_instance.status = UpdateStatuses.FAILED
        update_instance.status_reason = msg
    finally:
        update_instance.completed_at = timezone.now()
        update_instance.save()


@cel_app.task
def update_outdated_kok_devices_periodic_task():
    update_instance = UpdateHistory.objects.create(is_automatic=True, company_id=10)
    try:
        connection = get_connection_to_esep_db()
        try:
            connection.ping(reconnect=True, attempts=3, delay=5)
        except Exception:
            pass

        if not connection.is_connected():
            logger.error("ESEP DB connection failed")
            return

        cursor = connection.cursor(buffered=True)
        limit_date = (datetime.now() - timedelta(days=config.OUTDATED_DEVICES_PERIOD)).strftime('%Y-%m-%d')

        cursor.execute("""
            SELECT b.number AS account_number
              FROM bank_books AS b
              LEFT JOIN devices AS d ON b.id = d.bankbook_id
             WHERE b.company_id = %s
               AND (
                    (d.updated_at <= %s AND d.deleted_at IS NULL)
                    OR d.id IS NULL
               )
          GROUP BY b.number
          ORDER BY MIN(d.updated_at)
             LIMIT 100000
        """, (update_instance.company_id, limit_date))
        rows = cursor.fetchall()
        cursor.close()
        connection.close()

        accounts = [str(acc).strip() for (acc,) in rows if str(acc).strip().isdigit()]
        if accounts:
            update_kok_db_task.delay(update_obj_pk=update_instance.pk, account_numbers=accounts)
        else:
            logger.info("[Periodic KOK] No accounts to update")

    except Exception as e:
        msg = str(e)
        logger.error(msg)
        update_instance.status = UpdateStatuses.FAILED
        update_instance.status_reason = msg
        update_instance.completed_at = timezone.now()
        update_instance.save()


def get_kok_devices_from_csv_file(file_path: str):
    """
    Преобразует CSV КОК в унифицированный формат:
    {
      "<account_number>": {
          "devices": [ { device_number, modem, last_check, next_check, water_type, value }, ... ],
          "phone": None,
          "fio": str|None,
          "people": int|None
      },
      ...
    }
    """
    csv_data: Dict[str, Dict[str, Any]] = {}
    logger.info(f"[KOK CSV] {file_path}")

    # детект кодировки (часто windows-1251)
    detected = 'utf-8'
    try:
        with open(file_path, 'rb') as f:
            detected = chardet.detect(f.read()).get('encoding') or 'utf-8'
    except Exception:
        pass

    def _d(s):
        if not s:
            return None
        s = str(s).strip()
        for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
            except Exception:
                continue
        return None

    try:
        with open(file_path, 'r', encoding=detected, errors='ignore') as file:
            reader = csv.reader(file, delimiter=';')
            header = next(reader, None)

            # индексы под ваш файл (пример)
            IDX_ACC = 0
            IDX_FIO = 1
            IDX_PEOPLE = 2
            IDX_TYPE = 3
            IDX_TECH = 4
            IDX_POKAZ = 5
            IDX_POVERKA = 6

            for i, row in enumerate(reader, start=2):
                if len(row) < 5:
                    continue
                acc = (row[IDX_ACC] or "").strip()
                if not acc.isdigit():
                    logger.warning(f"[KOK CSV] пропуск строки {i}: ЛС '{acc}' не число")
                    continue

                t = str(row[IDX_TYPE] or "").strip().lower()
                water_type = 1 if "холод" in t else 2

                device_number = str(row[IDX_TECH] or "").strip()
                value = safe_int_or_none(row[IDX_POKAZ] if len(row) > IDX_POKAZ else None)
                next_check = _d(row[IDX_POVERKA] if len(row) > IDX_POVERKA else None)
                fio = (str(row[IDX_FIO]) or "").strip() if len(row) > IDX_FIO else None
                people = safe_int_or_none(row[IDX_PEOPLE] if len(row) > IDX_PEOPLE else None)

                entry = csv_data.setdefault(acc, {"devices": [], "phone": None, "fio": fio or None, "people": people})
                entry["devices"].append({
                    "device_number": device_number,
                    "modem": "",
                    "last_check": None,
                    "next_check": next_check,
                    "water_type": water_type,
                    "value": value,
                })

    except FileNotFoundError:
        logger.error("File not found or path is incorrect.")
    except Exception as e:
        logger.error(f"Error occurred: {e}")

    return csv_data
