import csv
import logging
import math
import re
from typing import Optional, List, Tuple, Dict

import chardet
import requests
import mysql.connector
from datetime import datetime, timedelta, date
from constance import config
from django.db import OperationalError, transaction, close_old_connections
from django.utils import timezone

from apps.common import UpdatingTypes, ReportTypes, UpdateStatuses
from apps.common.models import UpdateHistory, UpdateHistoryReport, UpdateHistoryRollback
from apps.common.services import get_connection_to_esep_db
from config.celery_app import cel_app
from celery import shared_task
from django.conf import settings

logger = logging.getLogger("common")


# ==========
# LOG HELPERS
# ==========
class LogCtx:
    """Контекстная метка для логов (аккаунт, компания, update_id)."""
    def __init__(self, account_number=None, company_id=None, update_id=None):
        self.account_number = str(account_number) if account_number is not None else "-"
        self.company_id = str(company_id) if company_id is not None else "-"
        self.update_id = str(update_id) if update_id is not None else "-"

    def __str__(self):
        return f"[acc={self.account_number} comp={self.company_id} upd={self.update_id}]"


def _none2str(v):
    return "" if v is None else str(v)


def diff_record(before: dict, after: dict) -> List[Tuple[str, str, str]]:
    """
    Возвращает список изменений: [(field, old, new), ...]
    Сравнивает только ключи, присутствующие в 'after'.
    """
    changes: List[Tuple[str, str, str]] = []
    for k, new_val in (after or {}).items():
        old_val = (before or {}).get(k, None)
        if old_val != new_val:
            changes.append((k, _none2str(old_val), _none2str(new_val)))
    return changes


def log_changes(label: str, ctx: LogCtx, changes: List[Tuple[str, str, str]]):
    """
    Логирует изменения полей в виде: field: old -> new
    """
    if not changes:
        return
    # logger.debug(f"{label} {ctx}: changes:")
    # for f, o, n in changes:
    #     logger.debug(f"    - {f}: '{o}' -> '{n}'")


def _safe_digits_only(x) -> str:
    """Вернёт только цифры из x. Никогда не кидает исключения."""
    try:
        return re.sub(r'\D', '', str(x))
    except Exception as e:
        # logger.warning(f"_safe_digits_only: type={type(x)} val={repr(x)} err={e}")
        return ""

def _safe_split_ws(x) -> list[str]:
    """Сплит по любым пробелам. Никогда не кидает исключения."""
    try:
        return [p for p in re.split(r'\s+', str(x)) if p]
    except Exception as e:
        # logger.warning(f"_safe_split_ws: type={type(x)} val={repr(x)} err={e}")
        return [str(x)]


def safe_int_or_none(val):
    if val is None:
        return None
    try:
        if isinstance(val, bool):
            return int(val)
        if isinstance(val, (int, float)):
            return int(val)
        s = str(val)
    except Exception:
        return None
    digits = _safe_digits_only(s)
    if not digits:
        return None
    try:
        return int(digits)
    except Exception:
        return None

def normalize_phone(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    digits = _safe_digits_only(s)
    return digits or None


@cel_app.task
def run_rollback_task(update_obj_pk):
    close_old_connections()
    update_instance = UpdateHistory.objects.filter(pk=update_obj_pk).first()
    if not update_instance:
        close_old_connections()
        print('No instance')
        return

    update_instance.run_rollback()


@cel_app.task
def update_outdated_esep_devices_periodic_task():
    update_instance = UpdateHistory.objects.create(is_automatic=True, company_id=7,)
    logger.info(f"[Periodic] Начало автоматического обновления (id={update_instance.pk})")
    try:
        connection = get_connection_to_esep_db()
        connection.ping(reconnect=True, attempts=3, delay=5)
        if not connection.is_connected():
            logger.error("Не удалось подключиться к ESEP БД")
            return

        cursor = connection.cursor(buffered=True)
        cursor.execute("SELECT DATABASE();")
        db_name = cursor.fetchone()
        logger.debug(f"Подключено к базе: {db_name}")

        limit_date = (datetime.now() - timedelta(days=config.OUTDATED_DEVICES_PERIOD)) \
                        .strftime('%Y-%m-%d')
        cursor.execute("""
            SELECT b.number AS account_number
              FROM bank_books AS b
              LEFT JOIN devices AS d ON b.id = d.bankbook_id
             WHERE b.company_id = 7
               AND (
                    (d.updated_at <= %s AND d.deleted_at IS NULL)
                    OR d.id IS NULL
               )
          GROUP BY b.number
          ORDER BY MIN(d.updated_at)
             LIMIT 100000
        """, (limit_date,))
        rows = cursor.fetchall()
        logger.info(f"Найдено записей для проверки: {len(rows)}")

        numeric_accounts = []
        skipped = 0
        for (acc,) in rows:
            acc_str = str(acc).strip()
            if acc_str.isdigit():
                numeric_accounts.append(acc_str)
            else:
                skipped += 1
                logger.warning(f"Пропускаю некорректный ЛС: '{acc}'")

        logger.info(f"Будут обработаны {len(numeric_accounts)} ЛС, пропущено {skipped}")

        cursor.close()
        connection.close()

        if numeric_accounts:
            update_esep_db_task.delay(update_obj_pk=update_instance.pk,
                                      account_numbers=numeric_accounts)
        # logger.info(f"Запущено {total_chunks} подзадач update_esep_db_task")
        else:
            logger.info("Нет устаревших ЛС для обновления")
    except Exception as e:
        error_msg = str(e)
        if isinstance(e, mysql.connector.Error):
            error_msg = f"Error while connecting to MySQL: {str(e)}"
        logger.error(error_msg)
        update_instance.status = UpdateStatuses.FAILED
        update_instance.status_reason = error_msg
        update_instance.completed_at = timezone.now()
        update_instance.save()


@cel_app.task(bind=True, max_retries=5)
def update_esep_db_task(self, update_obj_pk: int, account_numbers: list = None):
    # logger.info(f"Запускаю update_esep_db_task, id={update_obj_pk}")
    close_old_connections()
    update_instance = UpdateHistory.objects.filter(pk=update_obj_pk).first()
    COMPANY_ID = 7

    if not update_instance:
        raise self.retry(exc=OperationalError("Instance not found"))

    try:
        total_report_json = None

        if update_instance.updating_type == UpdatingTypes.BY_API:
            if not account_numbers:
                account_numbers = read_accounts_csv_file(update_instance.data_file.path)

            accounts_data = {}
            empty_accounts = []

            for account_number in account_numbers:
                ACA_SERVICE_URL = config.ACA_SERVICE_HOST + "/api/getValueByAccountNumber?accountNumber={account_number}&apikey={api_key}"
                api_url = ACA_SERVICE_URL.format(
                    account_number=str(account_number).strip(),
                    api_key=config.ACA_SERVICE_API_KEY
                )
                # logger.debug(f"Запрос к ACA API: {api_url}")

                try:
                    start_ts = datetime.now()
                    response = requests.get(api_url, verify=False, timeout=10)
                    latency_ms = int((datetime.now() - start_ts).total_seconds() * 1000)
                    # logger.debug(f"[ACA API] {account_number}: http={response.status_code} latency_ms={latency_ms} len={len(response.content) if response.content else 0}")

                    if response.status_code != 200:
                        # logger.warning(f"Некорректный статус ответа от ACA: {response.status_code}")
                        # logger.warning(f"Тело ответа: {response.text}")

                        if response.status_code == 404:
                            empty_accounts.append(account_number)
                        else:
                            try:
                                error_data = response.json()
                                if error_data.get("error") == "wrong apikey":
                                    raise Exception("wrong apikey")
                            except ValueError:
                                continue
                                # logger.error(f"ACA API вернул не-JSON тело при ошибке: {response.text}")
                        continue

                    if not (response.headers.get('Content-Type') or '').startswith('application/json'):
                        # logger.error(f"ACA API вернул неожиданный Content-Type: {response.headers.get('Content-Type')}")
                        # logger.error(f"Тело ответа: {response.text}")
                        continue

                    try:
                        account_data = response.json()
                    except ValueError:
                        # logger.error(f"Не удалось разобрать JSON от ACA API. Тело ответа: {response.text}")
                        continue

                    # нет счётчиков — запоминaем для отчёта
                    if not account_data or not account_data.get('counters'):
                        empty_accounts.append(account_number)
                        continue

                    # поддерживаем телефон и список устройств
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

                    accounts_data[str(account_data.get('accountNumber') or account_number)] = {
                        "devices": devices,
                        "phone": phone_from_api
                    }

                except requests.exceptions.RequestException as req_err:
                    # logger.error(f"Ошибка при запросе к ACA API: {req_err}")
                    continue

            # Вызов синхронизации после сбора всех данных
            total_report_json = synchronize_esep_db_with_aca_data(accounts_data, update_obj_pk, COMPANY_ID)
            total_report_json[ReportTypes.EMPTY_BODY_FROM_VODOKANAL_API] = empty_accounts

        elif update_instance.updating_type == UpdatingTypes.BY_FILE:
            csv_data = get_aca_devices_from_csv_file(update_instance.data_file.path)
            total_report_json = synchronize_esep_db_with_aca_data(csv_data, update_obj_pk, COMPANY_ID)

        else:
            raise Exception(f"Неизвестный тип обновления: {update_instance.updating_type}")

        # Сохраняем отчёты
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
        error_msg = str(e)
        logger.error(error_msg)
        update_instance.status = UpdateStatuses.FAILED
        if "HTTPSConnectionPool" in error_msg:
            error_msg = "Не доходит запрос до АСА API сервиса: " + error_msg
        update_instance.status_reason = error_msg

    finally:
        close_old_connections()
        update_instance.completed_at = timezone.now()
        update_instance.save()


def get_params_for_update(aca_device, current_next_check=None):
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
        # Нормализация в строку
        if isinstance(raw, (list, tuple)):
            raw = " ".join(map(str, raw))
        elif not isinstance(raw, (str, bytes, int, float)):
            raw = str(raw)
        modem_str = str(raw)

        # Безопасный split по пробелам — берём первый непустой кусок
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
    aca_loaded_data,
    update_obj_pk: int,
    company_id: Optional[int] = None
):
    """
    Синхронизация ESEP с внешним источником.
    Если company_id задан, то:
      - берем bank_book только этой компании,
      - обновляем/ищем devices только с этим company_id,
      - пометки на удаление/создание делаем только внутри этой компании.
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
        }
    }

    # logger.info(f"[SYNC] start: items={len(aca_loaded_data)} company_scope={company_id if company_id is not None else 'ALL'} upd={update_obj_pk}")

    def remove_first_occurrence(device_list, device_number):
        for i, device in enumerate(device_list):
            if str(device.get('device_number')) == str(device_number):
                del device_list[i]
                break

    try:
        connection = get_connection_to_esep_db()
        if connection.is_connected():
            cursor = connection.cursor(buffered=True)
            connection.autocommit = False
            cursor.execute("SELECT DATABASE();")
            _ = cursor.fetchone()
    except mysql.connector.Error as e:
        logger.error(f"Error while connecting to MySQL: {str(e)}")
        raise Exception(f"Error while connecting to MySQL: {str(e)}")

    try:
        for aca_account_number, aca_account_payload in (aca_loaded_data or {}).items():
            # payload в унифицированном формате
            if isinstance(aca_account_payload, dict) and 'devices' in aca_account_payload:
                aca_account_devices = aca_account_payload.get('devices', []) or []
                aca_account_phone = normalize_phone(
                    aca_account_payload.get('phone') or aca_account_payload.get('telefon')
                )
                new_name = (str(aca_account_payload.get('fio') or '').strip() or None)
                new_people = safe_int_or_none(aca_account_payload.get('people'))
            else:
                aca_account_devices = aca_account_payload or []
                aca_account_phone, new_name, new_people = None, None, None

            if not aca_account_number or not str(aca_account_number).strip().isdigit():
                # logger.warning(f"Skipping invalid account number key: '{aca_account_number}'")
                continue

            account_id = int(str(aca_account_number).strip())

            ctx = LogCtx(account_number=aca_account_number, company_id=company_id, update_id=update_obj_pk)
            # logger.debug(f"[SYNC] {ctx} processing... devices_from_source={len(aca_account_devices)} phone={aca_account_phone or '-'} name={new_name or '-'} people={new_people if new_people is not None else '-'}")

            # ---- bank_book (учитываем company_id если задан) ----
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
                logger.debug(f"[BANK_BOOK MISS] {ctx} bank_book not found for account")
                continue

            bankbook_id, bb_company_id, current_phone, current_name, current_people = (
                bankbook[0],
                bankbook[1],
                normalize_phone(bankbook[2]),
                (str(bankbook[3]) or '').strip() if bankbook[3] is not None else None,
                bankbook[4],
            )

            # ---- тянем устройства (строго этой компании) ----
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
                # el: см. SELECT (индексы соответствуют)
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


            # MAKE PAIRS ACA DEVICE WITH ESEP DEVICE
            device_pairs, account_report_data = make_pairs(aca_account_devices, esep_devices, aca_account_number)

            # ---- обновляем bank_books (phone / name / people) + rollback ----
            sets, values = [], []
            if aca_account_phone and aca_account_phone != current_phone:
                sets.append("phone = %s")
                values.append(aca_account_phone)
            if new_name is not None and new_name != current_name:
                sets.append("name = %s")
                values.append(new_name)
            if new_people is not None and new_people != current_people:
                sets.append("people = %s")
                values.append(new_people)

            if sets:
                before = {"phone": current_phone, "name": current_name, "people": current_people}
                after = {}
                if aca_account_phone and aca_account_phone != current_phone:
                    after["phone"] = aca_account_phone
                if new_name is not None and new_name != current_name:
                    after["name"] = new_name
                if new_people is not None and new_people != current_people:
                    after["people"] = new_people

                # log_changes("[BANK_BOOK UPDATE]", ctx, diff_record(before, after))

                total_report_json['rollback_data']['bank_books'].append((
                    bankbook_id, current_phone, current_name, current_people,
                    f"ninja_update_rollback {str(update_obj_pk)}", 693, "now()",
                ))
                sets.append("updated_by = 693")
                sets.append("updated_at = NOW()")
                sql = f"UPDATE bank_books SET {', '.join(sets)} WHERE id = %s"
                values.append(bankbook_id)
                cursor.execute(sql, tuple(values))

            # ---- апдейты сопоставленных устройств ----
            for aca_device, esep_device in device_pairs:
                # print(
                #     f"([{aca_device.get('device_number')}, {aca_device.get('value')}], "
                #     f"[{esep_device.get('device_number')}, {esep_device.get('value')}])"
                # )

                current_next_check = esep_device['next_check'].strftime('%Y-%m-%d') if esep_device['next_check'] else None
                params_dict = get_params_for_update(aca_device, current_next_check=current_next_check)
                if not params_dict['keys']:
                    continue

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
                after_dev = {}
                for k, v in zip(params_dict['keys'], params_dict['values']):
                    after_dev[k] = v

                # log_changes("[DEVICE UPDATE] " + f"(dev_id={esep_device['id']} dev_num={esep_device['device_number']})", ctx, diff_record(before_dev, after_dev))

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

            # ---- устройства без показаний: обновляем если нашли соответствие, иначе помечаем на удаление ----
            if devices_without_indications:
                for dwi in devices_without_indications:
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

                    # ОБНОВИТЬ ДАТУ ПОВЕРКИ ЕСЕПОВСКОМУ ПУ БЕЗ ПОКАЗАНИЯ
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
                    after_dev = {}
                    for k, v in zip(params_dict['keys'], params_dict['values']):
                        after_dev[k] = v

                    # log_changes("[DEVICE UPDATE (NO INDICATION)] " + f"(dev_id={dwi['id']} dev_num={dwi['device_number']})", ctx, diff_record(before_dev, after_dev))

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
                    total_report_json['rollback_data']['reset_deletion']['devices'].extend(list(devices_for_delete.keys()))
                    placeholders = ', '.join(['%s'] * len(devices_for_delete))
                    sql_query = (
                        f"UPDATE devices SET deleted_by = 693, deleted_at = NOW(), "
                        f"comment = 'ninja_update {str(update_obj_pk)}' "
                        f"WHERE id IN ({placeholders})"
                    )
                    # logger.info(f"[DEVICE DELETE MARK] {ctx}: ids={list(devices_for_delete.keys())} numbers={list(devices_for_delete.values())}")
                    cursor.execute(sql_query, list(devices_for_delete.keys()))
                    print("sql_query: ", sql_query, list(devices_for_delete.keys()))
                    # saves the list of ids marked as deleted
                    for did, dnumber in devices_for_delete.items():
                        total_report_json[ReportTypes.REMOVED_REDUNDANT_DEVICES].append((str(aca_account_number), dnumber, did))
                else:
                    print("No devices to delete.")

            # ---- добавление новых устройств ----
            if account_report_data[ReportTypes.ADDED_NEW_DEVICES]:
                for new_device in account_report_data[ReportTypes.ADDED_NEW_DEVICES]:
                    params_dict = get_params_for_update(new_device)
                    if not params_dict['keys']:
                        continue

                    exclude_ids = total_report_json['rollback_data']['delete']['devices'] + excluding_changed
                    excluding_ids = f"AND d.id NOT IN ({', '.join(['%s'] * len(exclude_ids))})" if exclude_ids else ""

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
                        # logger.info(f"[DEVICE INSERT] {ctx}: account={aca_account_number} will_insert={params_dict}")
                        cursor.execute(
                            "INSERT INTO devices (" + ", ".join(
                                f"`{key}`" for key in params_dict['keys']) +
                            ", bankbook_id, company_id, comment, area_type, created_by, created_at, updated_at) "
                            "VALUES(" + ", ".join("%s" for _ in params_dict['values']) + ", %s, %s, %s, %s, 693, NOW(), NOW())",
                            (*params_dict['values'], bankbook_id, bb_company_id, f"ninja_update {str(update_obj_pk)}", 0)
                        )
                        device_id = cursor.lastrowid
                        # logger.info(f"[DEVICE INSERTED] {ctx}: new_device_id={device_id} number={new_device.get('device_number')}")
                        total_report_json['rollback_data']['delete']['devices'].append(device_id)
                        print(device_id)

                        total_report_json[ReportTypes.ADDED_NEW_DEVICES].append(
                            (str(aca_account_number), new_device.get('device_number'))
                        )

            # ---- отчеты-хвосты ----
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

        # SAVE ALL CHANGES INTO DATABASE
        logger.info(f"[SYNC] committing changes upd={update_obj_pk}")
        connection.commit()

    except Exception as e:
        connection.rollback()
        logger.error(f"[SYNC] error, rolling back upd={update_obj_pk}: {str(e)}")
        print("Changes rolled back! The error occured: ", str(e))
        raise
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
        logger.info(f"[SYNC] done upd={update_obj_pk}")

    return total_report_json


def make_pairs(aca_devices: list, esep_devices: list, account_number=''):
    """
    Mapping two lists of aca devices and esep devices by device_number or closest value
    Returns list of (aca_device, esep_device) pairs and report_data
    """
    pairs = []
    untitled_devices = []

    # report arrays
    reports_data = {
        ReportTypes.IN_VODOKANAL_NOT_IN_ESEP: [],
        ReportTypes.ZERO_IN_VODOKANAL_NOT_IN_ESEP: [],
        ReportTypes.IN_ESEP_NOT_IN_VODOKANAL: [],
        ReportTypes.ADDED_NEW_DEVICES: [],
    }

    if len(esep_devices) == 0:
        reports_data[ReportTypes.ADDED_NEW_DEVICES].extend(aca_devices)
        return [], reports_data

    # logger.info(f"[PAIR] start account={account_number} aca_len={len(aca_devices)} esep_len={len(esep_devices)}")
    # print("initial aca_devices: ", aca_devices)
    # print("initial esep_devices: ", esep_devices)

    # MAPPING BY DEVICE NUMBER
    for aca_device in aca_devices:
        aca_dvc_num = str(aca_device.get('device_number')).lower()
        if any(aca_dvc_num == x for x in ["(пусто)", "0"]) or "без номера" in aca_dvc_num:
            untitled_devices.append(aca_device)
        else:
            esep_device = list(filter(lambda x: x.get('device_number') == aca_device.get('device_number'), esep_devices))
            if esep_device:
                esep_device = esep_device[0]
                pairs.append((aca_device, esep_device))
                esep_devices.remove(esep_device)
                # logger.info(f"[PAIR BY NUMBER] account={account_number} device={aca_device.get('device_number')} -> esep_id={esep_device['id']}")
            else:
                if aca_device.get('value') is None:
                    reports_data[ReportTypes.ZERO_IN_VODOKANAL_NOT_IN_ESEP].append((account_number, aca_device.get('device_number')))
                    # logger.info(f"[PAIR ZERO] account={account_number} device={aca_device.get('device_number')} => ZERO_IN_VODOKANAL_NOT_IN_ESEP")
                else:
                    untitled_devices.append(aca_device)

    # если все сопоставили по номеру, разнести хвосты
    if len(untitled_devices) == 0 and len(esep_devices) > 0:
        for i in esep_devices:
            # logger.info(f"[PAIR TAIL ESEP] account={account_number} not_matched_esep={i['device_number']}")
            reports_data[ReportTypes.IN_ESEP_NOT_IN_VODOKANAL].append((account_number, i['device_number']))
    elif len(untitled_devices) > 0 and len(esep_devices) == 0:
        for i in untitled_devices:
            # logger.info(f"[PAIR TAIL ACA] account={account_number} not_matched_aca={i.get('device_number')}")
            reports_data[ReportTypes.IN_VODOKANAL_NOT_IN_ESEP].append(i)
    elif len(untitled_devices) > 0 and len(esep_devices) > 0:
        # MAPPING BY CLOSEST VALUES (без падений на None)
        def distance(a, b):
            av = a.get('value')
            bv = b.get('value')
            av = 0 if av is None else av
            bv = 0 if bv is None else bv
            try:
                return abs(av - bv)
            except Exception:
                return 0

        # logger.info(f"[PAIR BY VALUE] account={account_number}: trying to match {len(untitled_devices)} untitled vs {len(esep_devices)} esep")
        while untitled_devices and esep_devices:
            esep_device = esep_devices.pop(0)
            fit_device = min(untitled_devices, key=lambda x: distance(x, esep_device))
            pairs.append((fit_device, esep_device))
            # logger.info(
            #     f"[PAIR BY VALUE] account={account_number} fit aca_dev={fit_device.get('device_number')} (val={fit_device.get('value')}) "
            #     f"with esep_dev={esep_device.get('device_number')} (val={esep_device.get('value')}) "
            #     f"dist={abs((fit_device.get('value') or 0) - (esep_device.get('value') or 0))}"
            # )
            untitled_devices.remove(fit_device)

        # хвосты в отчёты
        for device in esep_devices:
            # logger.info(f"[PAIR TAIL ESEP] account={account_number} not_matched_esep={device['device_number']}")
            reports_data[ReportTypes.IN_ESEP_NOT_IN_VODOKANAL].append((account_number, device['device_number']))

        for device in untitled_devices:
            # logger.info(f"[PAIR TAIL ACA] account={account_number} not_matched_aca={device.get('device_number')}")
            reports_data[ReportTypes.IN_VODOKANAL_NOT_IN_ESEP].append(device)

    return pairs, reports_data


def check_source(source):
    return bool(str(source or '').strip().lower() not in ["system", "система"])


def read_accounts_csv_file(file_path) -> list:
    """ Returns list of account_number strings """
    # print("file_path: ", file_path)

    accounts = []
    try:
        detected_encoding = 'utf-8'
        with open(file_path, 'rb') as f:
            raw_data = f.read()
            detected_encoding = chardet.detect(raw_data)['encoding']

        # /
        with open(file_path, 'r', encoding=detected_encoding) as file:
            reader = csv.reader(file, delimiter=';')
            for i, row in enumerate(reader):
                if i == 0:
                    continue
                acc = (row[0] or '').strip()
                if not acc or not acc.strip().isdigit():
                    logger.warning(f"Пропускаю пустой или не-числовой ЛС (строка {i + 1}): '{row[0]}'")
                    continue
                accounts.append(acc)

    except FileNotFoundError:
        print("File not found or path is incorrect.")
    except Exception as e:
        print("Error occurred:", e)

    return accounts


def get_aca_devices_from_csv_file(file_path):
    csv_data = {}
    print("file_path: ", file_path)
    try:
        # detected_encoding = 'utf-8'
        # with open(file_path, 'rb') as f:
        #     raw_data = f.read()
        #     detected_encoding = chardet.detect(raw_data)['encoding']

        with open(file_path, 'r', encoding='windows-1251') as file:
            reader = csv.reader(file, delimiter=';')
            for i, row in enumerate(reader):
                if i < 60:
                    continue

                account_number = (row[10] or '').strip()
                if not account_number or not account_number.strip().isdigit():
                    logger.warning(f"Пропускаю некорректный ЛС (строка {i + 1}): '{row[10]}'")
                    continue

                if row[12] and not row[22]:
                    print("strange device: ", row)

                csv_data.setdefault(account_number, []).append({
                    "device_number": row[12],
                    "modem": row[13],
                    "source": row[24],
                    "next_check": row[16],
                    "water_type": row[17],
                    "value": safe_int_or_none((row[22] or '').strip()),
                })

                # ограничитель на случай теста
                if i == 110:
                    break

    except FileNotFoundError:
        print("File not found or path is incorrect.")
    except Exception as e:
        print("Error occurred:", e)

    return csv_data


def _parse_kok_response_to_account_payload(json_obj: dict) -> dict:
    # телефон может называться "telefon"
    raw_phone = json_obj.get("telefon")
    raw_phone = str(raw_phone).strip() if raw_phone is not None else ""
    phone = normalize_phone(raw_phone) if raw_phone else None

    # ФИО и кол-во людей
    fio = str(json_obj.get("fio") or "").strip() or None
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
    logger.info(f"Запускаю update_kok_db_task, id={update_obj_pk}")
    update_instance = UpdateHistory.objects.filter(pk=update_obj_pk).first()
    if not update_instance:
        raise self.retry(exc=OperationalError("Instance not found"))

    try:
        total_report_json = None

        # ---- BY_API: берем список ЛС и идем в API Кокшетау ----
        if update_instance.updating_type == UpdatingTypes.BY_API:
            if not account_numbers:
                account_numbers = read_accounts_csv_file(
                    update_instance.data_file.path) if update_instance.data_file else []
            logger.info(f"[KOK] Буду запрашивать {len(account_numbers)} ЛС (пример: {account_numbers[:5]})")

            headers = {"Authorization": "Basic MTox", "Accept": "application/json"}
            accounts_data, empty_accounts = {}, []

            for rca in account_numbers:
                url = f"http://suarnasik.hopto.org:4444/abon/hs/upr/people?rca={str(rca).strip()}"
                # logger.info(f"[KOK] GET {url}")
                try:
                    start_ts = datetime.now()
                    resp = requests.get(url, headers=headers, timeout=10)
                    latency_ms = int((datetime.now() - start_ts).total_seconds() * 1000)
                    # logger.info(f"[KOK API] rca={rca} http={resp.status_code} latency_ms={latency_ms} len={len(resp.content) if resp.content else 0}")

                    if resp.status_code != 200:
                        if resp.status_code == 404:
                            empty_accounts.append(rca)
                        else:
                            logger.warning(f"[KOK] {resp.status_code}: {resp.text[:200]}")
                        continue

                    # сервер не ставит Content-Type — парсим вручную при необходимости
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

                except requests.exceptions.RequestException as e:
                    logger.error(f"[KOK] request error for {rca}: {e}")
                    continue

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
            raise Exception(f"Неизвестный тип обновления: {update_instance.updating_type}")

        # ---- Сохраняем отчеты (как у ACA) ----
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
    logger.info(f"[Periodic KOK] Начало автоматического обновления (id={update_instance.pk})")
    try:
        connection = get_connection_to_esep_db()
        connection.ping(reconnect=True, attempts=3, delay=5)
        if not connection.is_connected():
            logger.error("Не удалось подключиться к ESEP БД")
            return

        cursor = connection.cursor(buffered=True)
        limit_date = (datetime.now() - timedelta(days=config.OUTDATED_DEVICES_PERIOD)).strftime('%Y-%m-%d')

        cursor.execute("""
            SELECT b.number AS account_number
              FROM bank_books AS b
              LEFT JOIN devices AS d ON b.id = d.bankbook_id
             WHERE b.company_id = 10
               AND (
                    (d.updated_at <= %s AND d.deleted_at IS NULL)
                    OR d.id IS NULL
               )
          GROUP BY b.number
          ORDER BY MIN(d.updated_at)
             LIMIT 100000
        """, (limit_date,))
        rows = cursor.fetchall()
        logger.info(f"[Periodic KOK] Найдено записей для проверки: {len(rows)}")
        cursor.close()
        connection.close()

        accounts = [str(acc).strip() for (acc,) in rows if str(acc).strip().isdigit()]

        if accounts:
            update_kok_db_task.delay(update_obj_pk=update_instance.pk, account_numbers=accounts)
        else:
            logger.info("[Periodic KOK] Нет ЛС для обновления")

    except Exception as e:
        msg = str(e)
        logger.error(msg)
        update_instance.status = UpdateStatuses.FAILED
        update_instance.status_reason = msg
        update_instance.completed_at = timezone.now()
        update_instance.save()


def get_kok_devices_from_csv_file(file_path: str):
    """
    Парсит CSV Кокшетау в универсальный формат:
    {
      "<account_number>": {
          "devices": [
              {
                "device_number": "...",      # technumber
                "modem": "",
                "source": "kok_file",
                "last_check": None,
                "next_check": "YYYY-MM-DD" or None,  # date_poverka
                "water_type": 1|2,           # по type_counter
                "value": int|None,           # pokaz
              },
              ...
          ],
          "phone": None,
          "fio": "...",                      # если есть колонка
          "people": int|None                 # если есть колонка
      },
      ...
    }
    """
    csv_data = {}
    logger.warning(file_path)

    # если кодировка неизвестна — детектим, но по опыту часто windows-1251
    detected = 'utf-8'
    try:
        with open(file_path, 'rb') as f:
            detected = chardet.detect(f.read()).get('encoding') or 'utf-8'
    except Exception:
        pass
    logger.warning(f"encoding: {detected}")

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

            # ---- Настрой индексы под свой файл ----
            # Пример ожиданий:
            # 0: account_number(rca), 1: fio, 2: people, 3: type_counter, 4: technumber, 5: pokaz, 6: date_poverka
            IDX_ACC = 0
            IDX_FIO = 1
            IDX_PEOPLE = 2
            IDX_TYPE = 3
            IDX_TECH = 4
            IDX_POKAZ = 5
            IDX_POVERKA = 6

            for i, row in enumerate(reader, start=2):
                # пропускаем пустые/короткие строки
                if len(row) < 5:
                    continue
                acc = (row[IDX_ACC] or "").strip()
                if not acc.isdigit():
                    logger.warning(f"[KOK CSV] пропуск строки {i}: ЛС '{acc}' не число")
                    continue

                # вода
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
