from django.db.models import TextChoices

default_app_config = 'apps.common.apps.CommonConfig'


class UpdatingTypes(TextChoices):
    BY_API = "BY_API", "Через API"
    BY_FILE = "BY_FILE", "Через Файл"


class UpdateStatuses(TextChoices):
    SUCCESS = "SUCCESS", "Успешно"
    IN_PROCESS = "IN_PROCESS", "В процессе"
    FAILED = "FAILED", "Неуспешно"
    ROLLED_BACK = "ROLLED_BACK", "Откатили изменения"


class ReportTypes(TextChoices):
    ZERO_IN_VODOKANAL_NOT_IN_ESEP = "ZERO_IN_VODOKANAL_NOT_IN_ESEP", "0 в VODOKANAL, нет в Есеп"
    IN_VODOKANAL_NOT_IN_ESEP = "IN_VODOKANAL_NOT_IN_ESEP", "Есть в VODOKANAL, но нет в Есеп"
    IN_VODOKANAL_NO_ACCOUNT_IN_ESEP = "IN_VODOKANAL_NO_ACCOUNT_IN_ESEP", "Есть в VODOKANAL, но нет в Есепе даже ЛС"
    IN_ESEP_NOT_IN_VODOKANAL = "IN_ESEP_NOT_IN_VODOKANAL", "Есть в Есеп, но нет в VODOKANAL"
    ADDED_NEW_DEVICES = "ADDED_NEW_DEVICES", "Добавлены Новые Счетчики"
    REMOVED_REDUNDANT_DEVICES = "REMOVED_REDUNDANT_DEVICES", "Удалены Лишние Счетчики"
    SUCCESSFULLY_UPDATED_DEVICES = "SUCCESSFULLY_UPDATED_DEVICES", "Успешно обновленные Счетчики"
    EMPTY_BODY_FROM_VODOKANAL_API = "EMPTY_BODY_FROM_VODOKANAL_API", "Пустой объект от VODOKANAL API"
