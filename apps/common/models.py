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
    company_id = models.PositiveIntegerField("Компания", choices=[
        (7, "ACA"),
        (10, "Кокшетау"),
    ], default=7)
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
        rb = getattr(self, "rollback", None)
        if not rb or not rb.data_json:
            logger.warning(f"[ROLLBACK] upd={self.pk}: rollback payload is empty — nothing to do")
            return

        data = rb.data_json or {}
        logger.info(f"[ROLLBACK] start upd={self.pk}")

        conn = None
        cur = None
        try:
            conn = get_connection_to_esep_db()
            conn.autocommit = False
            cur = conn.cursor(buffered=True)
            cur.execute("SELECT DATABASE()")
            db = cur.fetchone()
            logger.warning("You're connected to database: ")
            logger.warning(db)

            # ===== 1) bank_books: вернуть phone/name/people =====
            bank_books = data.get("bank_books") or []
            for i, row in enumerate(bank_books, start=1):
                try:
                    if isinstance(row, (list, tuple)) and len(row) >= 4:
                        bankbook_id = row[0]
                        phone_old = row[1]
                        name_old = row[2]
                        people_old = row[3]
                    elif isinstance(row, dict):
                        bankbook_id = row.get("id")
                        phone_old = row.get("phone")
                        name_old = row.get("name")
                        people_old = row.get("people")
                    else:
                        logger.warning(f"[ROLLBACK][BANK_BOOKS] skip malformed item #{i}: {row}")
                        continue

                    logger.info(
                        f"[ROLLBACK][BANK_BOOK] id={bankbook_id}: phone='{phone_old}', name='{name_old}', people={people_old}")
                    cur.execute(
                        """
                        UPDATE bank_books
                           SET phone=%s,
                               name=%s,
                               people=%s,
                               updated_by=%s,
                               updated_at=NOW()
                         WHERE id=%s
                        """,
                        (phone_old, name_old, people_old, 693, bankbook_id)
                    )
                except Exception as e:
                    logger.error(f"[ROLLBACK][BANK_BOOK] error on item #{i}: {e}")
                    raise

            # ===== 2) reset_deletion: оживить всё, что было помечено на удаление =====
            reset = (data.get("reset_deletion") or {})
            reset_dev_ids = list(reset.get("devices") or [])
            reset_ind_ids = list(reset.get("indications") or [])

            if reset_dev_ids:
                logger.info(f"[ROLLBACK][RESET DELETION] devices: {reset_dev_ids}")
                q = ", ".join(["%s"] * len(reset_dev_ids))
                cur.execute(
                    f"UPDATE devices SET deleted_at=NULL, deleted_by=NULL, updated_by=%s, updated_at=NOW(), comment=%s WHERE id IN ({q})",
                    (693, f"ninja_update_rollback {self.id}", *reset_dev_ids)
                )

            if reset_ind_ids:
                logger.info(f"[ROLLBACK][RESET DELETION] indications: {reset_ind_ids}")
                q = ", ".join(["%s"] * len(reset_ind_ids))
                cur.execute(
                    f"UPDATE indications SET deleted_at=NULL, deleted_by=NULL, updated_at=NOW() WHERE id IN ({q})",
                    (*reset_ind_ids,)
                )

            # ===== 3) удалить записи, созданные обновлением (delete.*) =====
            to_delete = (data.get("delete") or {})
            del_ind_ids = list(to_delete.get("indications") or [])
            del_dev_ids = list(to_delete.get("devices") or [])

            if del_ind_ids:
                logger.info(f"[ROLLBACK][DELETE] indications: {del_ind_ids}")
                q = ", ".join(["%s"] * len(del_ind_ids))
                cur.execute(f"DELETE FROM indications WHERE id IN ({q})", (*del_ind_ids,))

            if del_dev_ids:
                logger.info(f"[ROLLBACK][DELETE] devices: {del_dev_ids}")
                q = ", ".join(["%s"] * len(del_dev_ids))
                cur.execute(f"DELETE FROM devices WHERE id IN ({q})", (*del_dev_ids,))

            # ===== 4) откатить апдейты устройств (update.devices) =====
            upd = (data.get("update") or {})
            upd_devices = upd.get("devices") or []

            if upd_devices:
                sample = upd_devices[0]
                # Поддержка legacy/расширенных форматов:
                # len==7:  (id, last_check, next_check, modem_number, name_ru, name_kk, name_en)
                # len==11: (id, last_check, next_check, modem_number, name_ru, name_kk, name_en, number, comment, updated_by, 'now()')
                # else:    (id, last_check, next_check, modem_number, name_ru, name_kk, name_en, resource_type_id, number, comment, updated_by, 'now()')
                if isinstance(sample, (list, tuple)) and len(sample) == 7:
                    # минимальный набор
                    cur.executemany(
                        """
                        UPDATE devices
                           SET last_check=%s,
                               next_check=%s,
                               modem_number=%s,
                               name_ru=%s,
                               name_kk=%s,
                               name_en=%s,
                               updated_by=%s,
                               updated_at=NOW()
                         WHERE id=%s
                        """,
                        [
                            (row[1], row[2], row[3], row[4], row[5], row[6], 693, row[0])
                            for row in upd_devices
                        ]
                    )
                elif isinstance(sample, (list, tuple)) and len(sample) == 11:
                    # без resource_type_id
                    cleaned = [row[:-1] for row in upd_devices]  # убираем 'now()' в конце
                    cur.executemany(
                        """
                        UPDATE devices
                           SET last_check=%s,
                               next_check=%s,
                               modem_number=%s,
                               name_ru=%s,
                               name_kk=%s,
                               name_en=%s,
                               number=%s,
                               comment=%s,
                               updated_by=%s,
                               updated_at=NOW()
                         WHERE id=%s
                        """,
                        [
                            (row[1], row[2], row[3], row[4], row[5], row[6],
                             row[7], row[8], row[9], row[0])
                            for row in cleaned
                        ]
                    )
                else:
                    # полный набор c resource_type_id
                    cleaned = [row[:-1] if isinstance(row, (list, tuple)) else row for row in upd_devices]
                    cur.executemany(
                        """
                        UPDATE devices
                           SET last_check=%s,
                               next_check=%s,
                               modem_number=%s,
                               name_ru=%s,
                               name_kk=%s,
                               name_en=%s,
                               resource_type_id=%s,
                               number=%s,
                               comment=%s,
                               updated_by=%s,
                               updated_at=NOW()
                         WHERE id=%s
                        """,
                        [
                            (row[1], row[2], row[3], row[4], row[5], row[6],
                             row[7], row[8], row[9], row[10], row[0])
                            for row in cleaned
                        ]
                    )

            # ===== 5) откатить апдейты показаний (update.indications) =====
            upd_inds = upd.get("indications") or []
            if upd_inds:
                cur.executemany(
                    "UPDATE indications SET value=%s, updated_at=NOW() WHERE id=%s",
                    [
                        (row[1], row[0]) if isinstance(row, (list, tuple)) else (row.get("value"), row.get("id"))
                        for row in upd_inds
                    ]
                )

            # ===== 6) пересоздать записи, если есть слепки в create.devices =====
            create = (data.get("create") or {})
            create_devices = create.get("devices") or []
            if create_devices:
                logger.info(f"[ROLLBACK][REINSERT DEVICES] count={len(create_devices)}")
                cols = (
                    "id, company_id, resource_type_id, bankbook_id, number, uid, area_type, "
                    "name_ru, name_kk, name_en, manufacturer, manufacture_date, diameter, "
                    "modem_number, last_check, next_check, seal_number, seal_setting_date, "
                    "anti_magnetic_seal_number, anti_magnetic_seal_setting_date, ast_su_ind, "
                    "comment, created_by, updated_by, deleted_by, created_at, updated_at, deleted_at"
                )
                placeholders = ", ".join(["%s"] * 29)
                cur.executemany(
                    f"INSERT INTO devices ({cols}) VALUES ({placeholders})",
                    create_devices
                )

            # commit
            conn.commit()
            logger.info(f"[ROLLBACK] success upd={self.pk}")

        except Exception as e:
            logger.error(f"[ROLLBACK] error upd={self.pk}: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

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
        max_length=100,
        choices=ReportTypes.choices,
        default=ReportTypes.IN_VODOKANAL_NOT_IN_ESEP
    )
    data_json = models.JSONField(null=True, blank=True)


class UpdateHistoryRollback(TimestampModel):
    update_history = models.OneToOneField(
        UpdateHistory,
        on_delete=models.CASCADE,
        related_name="rollback"
    )
    data_json = models.JSONField(null=True, blank=True)
