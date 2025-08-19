import csv
import logging
import re

import chardet
import requests
import mysql.connector
from datetime import datetime, timedelta, date
from constance import config
from django.db import OperationalError, close_old_connections
from django.utils import timezone

from apps.common import UpdatingTypes, ReportTypes, UpdateStatuses
from apps.common.models import UpdateHistory, UpdateHistoryReport, UpdateHistoryRollback
from apps.common.services import get_connection_to_esep_db
from config.celery_app import cel_app
from celery import shared_task
from django.conf import settings

logger = logging.getLogger("common")


@cel_app.task
def run_rollback_task(update_obj_pk):
    update_instance = UpdateHistory.objects.filter(pk=update_obj_pk).first()
    if not update_instance:
        print('No instance')
        return

    update_instance.run_rollback()


@cel_app.task
def update_outdated_esep_devices_periodic_task():
    update_instance = UpdateHistory.objects.create(is_automatic=True)
    try:
        connection = get_connection_to_esep_db()
        if not connection.is_connected():
            return
        cursor = connection.cursor(buffered=True)
        cursor.execute("select database();")
        _ = cursor.fetchone()

        limit_date = (datetime.now() - timedelta(days=config.OUTDATED_DEVICES_PERIOD)).strftime('%Y-%m-%d')
        # print(limitе_date)
        cursor.execute("""
            SELECT b.number AS account_number
            FROM `bank_books` AS b
            LEFT JOIN `devices` AS d ON b.id = d.bankbook_id
            WHERE (d.updated_at <= %s AND d.deleted_at IS NULL) or d.id is null
            GROUP BY b.number
            ORDER BY MIN(d.updated_at)
            LIMIT 100000
        """, (limit_date,))
        account_numbers_db = cursor.fetchall()
        account_numbers = []
        for a in account_numbers_db:
            if a[0].isnumeric():
                account_numbers.append(a[0])
            else:
                print(f'This account_number is skipped: {a[0]}')

        if connection.is_connected():
            cursor.close()
            connection.close()

        if len(account_numbers_db) > 0:
            update_esep_db_task(update_obj_pk=update_instance.pk, account_numbers=account_numbers)
        else:
            print('There are no outdated account numbers')
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
    close_old_connections()
    print("Update instance: ", update_obj_pk)
    update_instance = UpdateHistory.objects.filter(pk=update_obj_pk).first()
    # print('update_instance: ', update_instance)
    if not update_instance:
        raise self.retry(exc=OperationalError("Instance not found"))

    try:
        if update_instance.updating_type == UpdatingTypes.BY_API:
            if not account_numbers:
                account_numbers = read_accounts_csv_file(update_instance.data_file.path)
            accounts_data = {}
            empty_accounts = []

            for account_number in account_numbers:
                ACA_SERVICE_URL = config.ACA_SERVICE_HOST + "/api/getValueByAccountNumber?accountNumber={account_number}&apikey={api_key}"
                api_url = ACA_SERVICE_URL.format(
                    account_number=account_number.strip(),
                    api_key=config.ACA_SERVICE_API_KEY
                )
                logger.info(f"api_url: {api_url}")

                response = requests.get(api_url, verify=False)
                if response.status_code != 200:
                    logger.info(f"invalid response from ACA: {response}")
                    if response.status_code == 404:
                        empty_accounts.append(account_number)
                    if response.json().get("error") == "wrong apikey":
                        raise Exception("wrong apikey")
                    continue

                account_data = response.json()
                if not account_data or not account_data.get('counters'):
                    empty_accounts.append(account_number)
                    continue

                accounts_data[account_data['accountNumber']] = []
                for c in account_data.get('counters'):
                    accounts_data[account_data['accountNumber']].append(
                        {
                            "device_number": c.get('factory', ''),
                            "modem": c.get('modem', ""),
                            "source": c.get('source', ''),
                            "last_check": c['previousVerificationDate'][:10] if c.get('previousVerificationDate') else None,
                            "next_check": c['nextVerificationDate'][:10] if c.get('nextVerificationDate') else None,
                            "water_type": 1 if c.get('isCold', True) else 2,
                            "value": c.get('value'),
                        }
                    )

            print(accounts_data)
            total_report_json = synchronize_esep_db_with_aca_data(accounts_data, update_obj_pk)
            total_report_json[ReportTypes.EMPTY_BODY_FROM_ACA_API] = empty_accounts
            print(total_report_json)

        elif update_instance.updating_type == UpdatingTypes.BY_FILE:
            csv_data = get_aca_devices_from_csv_file(update_instance.data_file.path)
            print(csv_data)
            total_report_json = synchronize_esep_db_with_aca_data(csv_data, update_obj_pk)

        # SAVE TO DB TOTAL REPORTS
        UpdateHistoryRollback.objects.create(
            update_history=update_instance,
            data_json=total_report_json.pop('rollback_data')
        )
        UpdateHistoryReport.objects.bulk_create([
            UpdateHistoryReport(
                update_history=update_instance,
                report_type=report_type.upper(),
                data_json={
                    "amount_total": len(total_report_json[report_type]),
                    "items": list(total_report_json[report_type])
                },
            ) for report_type, report in total_report_json.items()
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
        params_dict['values'].append(aca_device['device_number'])
    if aca_device.get('next_check') and aca_device['next_check'] not in ["(пусто)", "None"]:
        params_dict['keys'].append("next_check")
        params_dict['values'].append(aca_device['next_check'])
        # if datetime.strptime(aca_device['next_check'], '%Y-%m-%d') > datetime.now():
        #     params_dict['values'].append(aca_device['next_check'])
        # else:
        #     params_dict['values'].append(current_next_check)

    if aca_device.get('last_check') and aca_device['last_check'] not in ["(пусто)", "None"]:
        params_dict['keys'].append("last_check")
        params_dict['values'].append(aca_device['last_check'])
    if aca_device.get('modem') and aca_device['modem'] != "(пусто)":
        params_dict['keys'].append("modem_number")
        modem_str = next(part.strip() for part in re.split(r'\t+|\n+|\s+', aca_device['modem']) if part.strip())
        params_dict['values'].append(modem_str)
    if aca_device.get('water_type'):
        water_name = "ХВС" if aca_device['water_type'] == 1 else "ГВС"
        params_dict['keys'].extend(["name_ru", "name_kk", "name_en"])
        params_dict['values'].extend([water_name, water_name, water_name])
        params_dict['keys'].append("resource_type_id")
        params_dict['values'].append(aca_device['water_type'])
    return params_dict


def synchronize_esep_db_with_aca_data(aca_loaded_data, update_obj_pk: int):
    total_report_json = {
        ReportTypes.IN_ACA_NOT_IN_ESEP: [],
        ReportTypes.IN_ACA_NO_ACCOUNT_IN_ESEP: set(),
        ReportTypes.ZERO_IN_ACA_NOT_IN_ESEP: [],
        ReportTypes.IN_ESEP_NOT_IN_ACA: [],
        ReportTypes.ADDED_NEW_DEVICES: [],
        ReportTypes.REMOVED_REDUNDANT_DEVICES: [],
        ReportTypes.SUCCESSFULLY_UPDATED_DEVICES: [],
        ReportTypes.EMPTY_BODY_FROM_ACA_API: [],
        'rollback_data': {
            'update': {
                'devices': [],
                'indications': [],
            },
            'delete': {
                'devices': [],
                'indications': [],
            },
            'reset_deletion': {
                'devices': [],
                'indications': [],
            },
            'create': {
                'devices': [],
                'indications': [],
            }
        }
    }

    def remove_first_occurrence(device_list, device_number):
        for i, device in enumerate(device_list):
            if str(device['device_number']) == str(device_number):
                del device_list[i]
                break

    try:
        connection = get_connection_to_esep_db()
        if connection.is_connected():
            cursor = connection.cursor(buffered=True)
            connection.autocommit = False
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to database: ", record)
            cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")

    except mysql.connector.Error as e:
        logger.error(f"Error while connecting to MySQL: {str(e)}")
        raise Exception(f"Error while connecting to MySQL: {str(e)}")

    try:
        for aca_account_number, aca_account_devices in aca_loaded_data.items():
            # MAIN QUERY
            cursor.execute("""
                SELECT b.number 'account_number', d.id 'device_id', i.id 'indication_id', d.number 'device_number',
                 i.value, i.created_at, i.updated_at, d.last_check, d.next_check, d.company_id, d.resource_type_id, 
                 d.modem_number, d.name_ru, d.name_kk, d.name_en, d.comment, d.updated_by, d.updated_at, i.deleted_at
                FROM devices as d
                LEFT JOIN indications as i ON d.id = i.device_id
                LEFT JOIN bank_books as b on b.id = d.bankbook_id
                WHERE (i.id = (
                    SELECT i2.id
                    FROM indications i2
                    WHERE i2.device_id = d.id
                    ORDER BY i2.updated_at DESC, i2.id DESC
                    LIMIT 1
                ) OR i.id IS NULL) and d.deleted_at IS NULL and b.number = %s
            """, (int(aca_account_number),))
            esep_db_data = cursor.fetchall()
            # print(esep_db_data)

            esep_devices = []
            devices_without_indications = []
            devices_for_delete = {}
            excluding_changed = []

            for el in esep_db_data:
                if el[0].strip() == aca_account_number:
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
                            "value": int(re.sub(r'\D', '', el[4])),
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

            print('======')
            print(aca_account_number)

            # MAKE PAIRS ACA DEVICE WITH ESEP DEVICE
            device_pairs, account_report_data = make_pairs(aca_account_devices, esep_devices, aca_account_number)
            print(account_report_data)

            # SYNCHRONIZE DATA IN DB ACCORDING TO (ACA + ESEP) DEVICES PAIR
            for aca_device, esep_device in device_pairs:
                print(
                    f"([{aca_device['device_number']}, {aca_device['value']}], [{esep_device['device_number']}, {esep_device['value']}])")

                current_next_check = esep_device['next_check'].strftime('%Y-%m-%d') if esep_device['next_check'] else None
                params_dict = get_params_for_update(aca_device, current_next_check=current_next_check)
                cursor.execute(
                    "UPDATE devices SET " + ",".join(
                        f"`{key}` = %s" for key in params_dict['keys']
                    ) + ",comment = %s,updated_by=693,updated_at=now() WHERE id = %s",
                    (*params_dict['values'], f"ninja_update {str(update_obj_pk)}", esep_device['id'],)
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
                    f"ninja_update_rollback {str(update_obj_pk)}",
                    693,  # updated_by
                    "now()",  # updated_at
                ))

                # if check_source(aca_device['source']) and aca_device['value'] > esep_device['value']:
                #     print('changing value to: ', aca_device['value'])
                #     cursor.execute(
                #         "UPDATE indications SET value = %s WHERE id = %s",
                #         (aca_device['value'], esep_device['indication_id'],)
                #     )
                #     total_report_json['rollback_data']['update']['indications'].append((
                #         esep_device['indication_id'], esep_device['value']
                #     ))
                total_report_json[ReportTypes.SUCCESSFULLY_UPDATED_DEVICES].append(
                    (aca_account_number, aca_device['device_number']))

            if devices_without_indications:
                for dwi in devices_without_indications:
                    first_match_device = None
                    if account_report_data[ReportTypes.IN_ACA_NOT_IN_ESEP]:
                        needed_rtype = ReportTypes.IN_ACA_NOT_IN_ESEP
                        first_match_device = next(
                            (t for t in account_report_data[needed_rtype] if str(t['device_number']) == str(dwi['device_number'])), None)
                    if not first_match_device and account_report_data[ReportTypes.ADDED_NEW_DEVICES]:
                        needed_rtype = ReportTypes.ADDED_NEW_DEVICES
                        first_match_device = next(
                            (t for t in account_report_data[needed_rtype] if str(t['device_number']) == str(dwi['device_number'])), None)
                    if not first_match_device:
                        devices_for_delete[dwi['id']] = dwi['device_number']
                        continue

                    # ОБНОВИТЬ ДАТУ ПОВЕРКИ ЕСЕПОВСКОМУ ПУ БЕЗ ПОКАЗАНИЯ
                    current_next_check = dwi['next_check'].strftime('%Y-%m-%d') if dwi['next_check'] else None
                    params_dict = get_params_for_update(first_match_device, current_next_check=current_next_check)
                    cursor.execute(
                        "UPDATE devices SET " + ",".join(
                            f"`{key}` = %s" for key in params_dict['keys']
                        ) + ",comment = %s,updated_by=693,updated_at=now() WHERE id = %s",
                        (*params_dict['values'], f"ninja_update {str(update_obj_pk)}", dwi['id'],)
                    )
                    total_report_json['rollback_data']['update']['devices'].append((
                        dwi['id'],
                        dwi['last_check'].strftime('%Y-%m-%d') if dwi['last_check'] else None,  # last_check
                        current_next_check,
                        dwi['modem_number'],
                        dwi['name_ru'],
                        dwi['name_kk'],
                        dwi['name_en'],
                        dwi['resource_type_id'],
                        dwi['device_number'],
                        f"ninja_update_rollback {str(update_obj_pk)}",  # comment
                        693,  # updated_by
                        "now()",  # updated_at
                    ))
                    total_report_json[ReportTypes.SUCCESSFULLY_UPDATED_DEVICES].append(
                        (aca_account_number, dwi['device_number']))
                    remove_first_occurrence(account_report_data[needed_rtype], dwi['device_number'])
                    excluding_changed.append(dwi['id'])

                if devices_for_delete:
                    total_report_json['rollback_data']['reset_deletion']['devices'].extend(list(devices_for_delete.keys()))
                    placeholders = ', '.join(['%s'] * len(devices_for_delete.keys()))
                    sql_query = (
                        f"UPDATE devices SET deleted_by=693, deleted_at=now(), comment='ninja_update {str(update_obj_pk)}' "
                        f"WHERE id IN ({placeholders})"
                    )
                    cursor.execute(sql_query, list(devices_for_delete.keys()))
                    print("sql_query: ", sql_query, list(devices_for_delete.keys()))
                    # saves the list of ids marked as deleted
                    for did, dnumber in devices_for_delete.items():
                        total_report_json[ReportTypes.REMOVED_REDUNDANT_DEVICES].append((aca_account_number, dnumber, did))
                else:
                    print("No devices to delete.")

            # ADD NEW ACCOUNT/DEVICES/INDICATIONS IF NEEDED
            if account_report_data[ReportTypes.ADDED_NEW_DEVICES]:
                excluding_ids = ""
                for new_device in account_report_data[ReportTypes.ADDED_NEW_DEVICES]:
                    params_dict = get_params_for_update(new_device)
                    cursor.execute("SELECT id, company_id from bank_books where number = %s", (aca_account_number,))
                    account_info = cursor.fetchone()
                    if not account_info:
                        total_report_json[ReportTypes.IN_ACA_NO_ACCOUNT_IN_ESEP].add(aca_account_number)
                        continue

                    if total_report_json['rollback_data']['delete']['devices'] or excluding_changed:
                        excluding_ids = f"and d.id not in ({', '.join(['%s'] * len(total_report_json['rollback_data']['delete']['devices'] + excluding_changed))})"
                    sql_query = f"""
                        SELECT d.id FROM `devices` d
                        join `bank_books` b on b.id=d.bankbook_id
                        where d.number = %s and b.number= %s
                        AND d.deleted_at IS NULL 
                        {excluding_ids}
                        ORDER BY d.`id` DESC;
                    """
                    print(sql_query)
                    cursor.execute(sql_query, (new_device['device_number'], aca_account_number, *total_report_json['rollback_data']['delete']['devices'], *excluding_changed))
                    device_id = cursor.fetchone()
                    print("device_id: ", device_id)
                    if device_id:
                        device_id = device_id[0]
                    else:
                        cursor.execute(
                            "INSERT INTO `devices` (" + ", ".join(
                                f"`{key}`" for key in params_dict['keys']) + ", `bankbook_id`, `company_id`, `comment`, `area_type`, `created_by`, `created_at`, `updated_at`) "
                            "VALUES(" + ", ".join("%s" for i in params_dict['values']) + ", %s, %s, %s, %s, 693, now(), now())",
                            (*params_dict['values'], account_info[0], account_info[1], f"ninja_update {str(update_obj_pk)}", 0),
                            )
                        device_id = cursor.lastrowid  # last inserted row id
                        total_report_json['rollback_data']['delete']['devices'].append(device_id)
                        print(device_id)

                        total_report_json[ReportTypes.ADDED_NEW_DEVICES].append(
                            (aca_account_number, new_device['device_number'])
                        )
                    # indication_status = 0
                    # cursor.execute(
                    #     "INSERT INTO `indications` (`device_id`, `value`, `status`, `created_at`, `updated_at`) "
                    #     "VALUES(%s, %s, %s, now(), now())",
                    #     (device_id, new_device['value'], indication_status),
                    # )
                    # new_indication_id = cursor.lastrowid  # last inserted row id
                    # total_report_json['rollback_data']['delete']['indications'].append(new_indication_id)

            # STORE REPORTS FROM account_number
            total_report_json[ReportTypes.ZERO_IN_ACA_NOT_IN_ESEP].extend(
                account_report_data[ReportTypes.ZERO_IN_ACA_NOT_IN_ESEP]
            )
            total_report_json[ReportTypes.IN_ESEP_NOT_IN_ACA].extend(
                account_report_data[ReportTypes.IN_ESEP_NOT_IN_ACA]
            )
            for d in account_report_data[ReportTypes.IN_ACA_NOT_IN_ESEP]:
                total_report_json[ReportTypes.IN_ACA_NOT_IN_ESEP].append(
                    (aca_account_number, d['device_number'])
                )

            connection.commit()

    except Exception as e:
        connection.rollback()
        print("Changes rolled back! The error occured: ", str(e))
        raise e

    if connection.is_connected():
        cursor.close()
        connection.close()

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
        ReportTypes.IN_ACA_NOT_IN_ESEP: [],
        ReportTypes.ZERO_IN_ACA_NOT_IN_ESEP: [],
        ReportTypes.IN_ESEP_NOT_IN_ACA: [],
        ReportTypes.ADDED_NEW_DEVICES: [],
    }

    if len(esep_devices) == 0:
        reports_data[ReportTypes.ADDED_NEW_DEVICES].extend(aca_devices)
        return [], reports_data

    print("initial aca_devices: ", aca_devices)
    print("initial esep_devices: ", esep_devices)

    # MAPPING BY DEVICE NUMBER
    for aca_device in aca_devices:
        aca_dvc_num = str(aca_device['device_number']).lower()
        if any(aca_dvc_num == x for x in ["(пусто)", "0"]) or "без номера" in aca_dvc_num:
            untitled_devices.append(aca_device)
        else:
            esep_device = list(filter(lambda x: x.get('device_number') == aca_device['device_number'], esep_devices))
            if esep_device:
                esep_device = esep_device[0]
                pairs.append((aca_device, esep_device))
                esep_devices.remove(esep_device)
            else:
                if aca_device['value'] is None:
                    reports_data[ReportTypes.ZERO_IN_ACA_NOT_IN_ESEP].append((account_number, aca_device['device_number']))
                else:
                    untitled_devices.append(aca_device)

    # print("untitled_devices: ", untitled_devices)

    if len(untitled_devices) == 0 and len(esep_devices) > 0:
        for i in esep_devices:
            reports_data[ReportTypes.IN_ESEP_NOT_IN_ACA].append((account_number, i['device_number']))
    elif len(untitled_devices) > 0 and len(esep_devices) == 0:
        for i in untitled_devices:
            reports_data[ReportTypes.IN_ACA_NOT_IN_ESEP].append(i)
    elif len(untitled_devices) > 0 and len(esep_devices) > 0:
        # MAPPING BY CLOSEST VALUES
        while untitled_devices and esep_devices:
            esep_device = esep_devices.pop(0)
            fit_device = min(untitled_devices, key=lambda x: abs(x['value'] - esep_device['value']))
            pairs.append((fit_device, esep_device))
            untitled_devices.remove(fit_device)

        # NOT TO LOSE SINGLE ELEMENTS(THAT WEREN'T ADDED TO PAIRS) FROM ESEP_DEVICES OR UNTITLED_DEVICES
        for device in esep_devices:
            reports_data[ReportTypes.IN_ESEP_NOT_IN_ACA].append((account_number, device['device_number']))

        for device in untitled_devices:
            reports_data[ReportTypes.IN_ACA_NOT_IN_ESEP].append(device)

    return pairs, reports_data


def check_source(source):
    return bool(source.strip().lower() not in ["system", "система"])


def read_accounts_csv_file(file_path) -> list:
    """ Returns list of account_number strings """
    print("file_path: ", file_path)

    accounts = []
    try:
        detected_encoding = 'utf-8'
        with open(file_path, 'rb') as f:
            raw_data = f.read()
            detected_encoding = chardet.detect(raw_data)['encoding']

        print('encoding: ', detected_encoding)
        with open(file_path, 'r', encoding=detected_encoding) as file:
            reader = csv.reader(file, delimiter=';')
            for i, row in enumerate(reader):
                if i == 0: continue
                accounts.append(row[0].strip())

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

                account_number = row[10]  # ЛС
                if not account_number in csv_data:
                    csv_data[account_number] = []

                if row[12] and not row[22]:
                    print("strange device: ", row)

                csv_data[account_number].append({
                    "device_number": row[12],
                    "modem": row[13],
                    "source": row[24],
                    "next_check": row[16],
                    "water_type": row[17],
                    "value": int(row[22] if row[22].isnumeric() else 0),
                })

                if i == 110:
                    break

    except FileNotFoundError:
        print("File not found or path is incorrect.")
    except Exception as e:
        print("Error occurred:", e)

    return csv_data
