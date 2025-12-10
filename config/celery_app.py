import os
from distutils.util import strtobool
from celery import Celery
from celery.schedules import crontab

if not ("DJANGO_SETTINGS_MODULE" in os.environ):
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

cel_app = Celery("updaterninja")

cel_app.config_from_object("django.conf:settings", namespace="CELERY")

cel_app.autodiscover_tasks()
cel_app.conf.broker_transport_options = {'visibility_timeout': 43200}  # 12 hours
cel_app.conf.result_backend_transport_options = {'visibility_timeout': 43200}
cel_app.conf.visibility_timeout = 43200
cel_app.conf.task_acks_late = True

disable_periodic_tasks = False
try:
    disable_periodic_tasks = bool(strtobool(os.getenv("DISABLE_PERIODIC_TASKS", "false")))
except (ValueError, AttributeError):
    disable_periodic_tasks = False

cel_app.conf.beat_schedule = {} if disable_periodic_tasks else {
    # ACA
    'update-outdated-esep-devices-periodic-task': {
        'task': 'apps.common.tasks.update_outdated_esep_devices_periodic_task',
        'schedule': crontab(hour=2, minute=0)
    },
    # Кокшетау
    'update-outdated-kok-devices-periodic-task': {
        'task': 'apps.common.tasks.update_outdated_kok_devices_periodic_task',
        'schedule': crontab(hour=3, minute=0)
    },
}

cel_app.conf.timezone = 'Asia/Almaty'
