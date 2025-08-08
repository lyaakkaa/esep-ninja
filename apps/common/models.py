import logging

import mysql.connector

from uuid import uuid4
from django.db import models
from django.utils import timezone
from django.utils.translation import gettext_lazy as _  # noqa
from apps.common.services import get_connection_to_esep_db

from . import UpdatingTypes, UpdateStatuses, ReportTypes
logger = logging.getLogger("common")
devices_columns = [
    "id",
    "company_id",
    "resource_type_id",
    "bankbook_id",
    "number",
    "uid",
    "area_type",
    "name_ru",
    "name_kk",
    "name_en",
    "manufacturer",
    "manufacture_date",
    "diameter",
    "modem_number",
    "last_check",
    "next_check",
    "seal_number",
    "seal_setting_date",
    "anti_magnetic_seal_number",
    "anti_magnetic_seal_setting_date",
    "ast_su_ind",
    "comment",
    "created_by",
    "updated_by",
    "deleted_by",
    "created_at",
    "updated_at",
    "deleted_at"
]


class UUIDModel(models.Model):
    uuid = models.UUIDField(
        "Идентификатор",
        default=uuid4,
        unique=True,
        editable=False,
        db_index=True
    )

    class Meta:
        abstract = True


class TimestampModel(models.Model):
    created_at = models.DateTimeField("Время создания", default=timezone.now, db_index=True)
    updated_at = models.DateTimeField("Время последнего изменения", auto_now=True, db_index=True)

    class Meta:
        abstract = True

    @property
    def created_at_pretty(self):
        return self.created_at.strftime("%d/%m/%Y %H:%M:%S")  # noqa

    @property
    def updated_at_pretty(self):
        return self.updated_at.strftime("%d/%m/%Y %H:%M:%S")  # noqa


class HandledException(TimestampModel):
    code = models.CharField("Код ошибки", max_length=256)
    message = models.CharField("Описание ошибки", max_length=256)
    stack_trace = models.TextField("Traceback", null=True, blank=True)


class UpdateHistory(TimestampModel):
    updating_type = models.CharField(
        max_length=20,
        choices=UpdatingTypes.choices,
        default=UpdatingTypes.BY_API
    )
    data_file = models.FileField(upload_to="data", null=True, blank=True)
    is_automatic = models.BooleanField(default=False)
    status = models.CharField(
        max_length=20,
        choices=UpdateStatuses.choices,
        default=UpdateStatuses.IN_PROCESS
    )
    status_reason = models.TextField(null=True, blank=True)
    completed_at = models.DateTimeField("Время окончания запуска", null=True, blank=True)
    rolled_back_at = models.DateTimeField("Время отката", null=True, blank=True)

    class Meta:
        verbose_name = "Обновление"
        verbose_name_plural = "История Обновлений"

    def run_rollback(self):
        if getattr(self, 'rollback'):
            data = self.rollback.data_json

            try:
                connection = get_connection_to_esep_db()
                if connection.is_connected():
                    cursor = connection.cursor(buffered=True)
                    cursor.execute("select database();")
                    record = cursor.fetchone()
                    print("You're connected to database: ", record)

            except mysql.connector.Error as e:
                logger.error(f"Error while connecting to MySQL: {str(e)}")
                raise Exception(f"Error while connecting to MySQL: {str(e)}")

            if len(data.get('create', {}).get('devices', [])) > 0:
                cursor.executemany(f"""
                    INSERT INTO devices 
                        ({", ".join(devices_columns)})
                    VALUES 
                        ({', '.join(['%s'] * len(devices_columns))})
                """, data['create']['devices'])

            if len(data.get('reset_deletion', {}).get('devices', [])) > 0:
                cursor.execute(f"""
                    UPDATE devices SET deleted_by=NULL, deleted_at=NULL, updated_at=now(), comment='ninja_update_rollback {str(self.id)}' 
                    WHERE id IN ({', '.join(['%s'] * len(data['reset_deletion']['devices']))})
                """, data['reset_deletion']['devices'])

            if len(data.get('update', {}).get('devices', [])) > 0:
                if len(data['update']['devices'][0]) == 7:  # legacy
                    values_placeholders = "(%s, %s, %s, %s, %s, %s, %s)"
                    columns_placeholders = "(id, last_check, next_check, modem_number, name_ru, name_kk, name_en)"
                    columns_update = ""
                    data_update_devices = data['update']['devices']
                elif len(data['update']['devices'][0]) == 11:
                    values_placeholders = "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    columns_placeholders = ("(id, last_check, next_check, modem_number, name_ru, name_kk, name_en, "
                                            "number, comment, updated_by)")
                    columns_update = """number = VALUES(number),
                        comment = VALUES(comment),
                        updated_by = VALUES(updated_by),
                        updated_at = NOW()
                    """
                    data_update_devices = [device[:-1] for device in data['update']['devices']]
                else:
                    values_placeholders = "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    columns_placeholders = ("(id, last_check, next_check, modem_number, name_ru, name_kk, name_en, "
                                            "resource_type_id, number, comment, updated_by)")
                    columns_update = """resource_type_id = VALUES(resource_type_id),
                        number = VALUES(number),
                        comment = VALUES(comment),
                        updated_by = VALUES(updated_by),
                        updated_at = NOW()
                    """
                    data_update_devices = [device[:-1] for device in data['update']['devices']]

                cursor.executemany(f"""
                    INSERT INTO devices 
                        {columns_placeholders}
                    VALUES 
                        {values_placeholders} 
                    ON DUPLICATE KEY UPDATE 
                        modem_number = VALUES(modem_number),
                        name_ru = VALUES(name_ru),
                        name_kk = VALUES(name_kk),
                        name_en = VALUES(name_en),
                        last_check = VALUES(last_check),
                        next_check = VALUES(next_check),
                        {columns_update}
                """, data_update_devices)

            if len(data.get('update', {}).get('indications', [])) > 0:
                cursor.executemany("""
                    INSERT INTO indications 
                        (id, value)
                    VALUES 
                        (%s, %s) 
                    ON DUPLICATE KEY UPDATE 
                        value = VALUES(value)
                """, data['update']['indications'])

            if len(data.get('delete', {}).get('devices', [])) > 0:
                placeholders = ', '.join(['%s'] * len(data['delete']['devices']))
                sql_query = f"DELETE FROM devices WHERE id IN ({placeholders})"
                cursor.execute(sql_query, data['delete']['devices'])

            if len(data.get('delete', {}).get('indications', [])) > 0:
                placeholders = ', '.join(['%s'] * len(data['delete']['indications']))
                sql_query = f"DELETE FROM indications WHERE id IN ({placeholders})"
                cursor.execute(sql_query, data['delete']['indications'])

            connection.commit()
            if connection.is_connected():
                cursor.close()
                connection.close()

            self.status = UpdateStatuses.ROLLED_BACK
            self.rolled_back_at = timezone.now()
            self.save()


class UpdateHistoryReport(TimestampModel):
    update_history = models.ForeignKey(
        UpdateHistory,
        on_delete=models.SET_NULL,
        related_name="reports",
        null=True, blank=True
    )
    report_type = models.CharField(
        max_length=30,
        choices=ReportTypes.choices,
        default=ReportTypes.IN_ACA_NOT_IN_ESEP
    )
    data_json = models.JSONField(null=True, blank=True)


class UpdateHistoryRollback(TimestampModel):
    update_history = models.OneToOneField(
        UpdateHistory,
        on_delete=models.CASCADE,
        related_name="rollback"
    )
    data_json = models.JSONField(null=True, blank=True)
