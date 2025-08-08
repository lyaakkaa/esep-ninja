"""
Django 5.0.4 settings for config project.

UPDATER NINJA PROJECT

For more information on this file, see
https://docs.djangoproject.com/en/5.0/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/5.0/ref/settings/
"""
import os
from collections import OrderedDict
from pathlib import Path

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/5.0/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'django-insecure-gz&p7a6mc*m=9e6f8d*a14rz%j^6mhhc!92eq41_+9ov9#$*8^'

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

ALLOWED_HOSTS = ['*']


# Application definition

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',

    "django_celery_beat",
    "constance",
    "constance.backends.database",

    'apps.common.apps.CommonConfig',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'config.urls'

SETTINGS_PATH = os.path.dirname(os.path.dirname(__file__))
TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [os.path.join(SETTINGS_PATH, "templates")],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'config.wsgi.application'


# Database
# https://docs.djangoproject.com/en/5.0/ref/settings/#databases
print("Check")
print("DB HOST:", os.environ.get("ESEP_DB_HOST"))
print("DB USER:", os.environ.get("ESEP_DB_USER"))
print("DB PASSWORD:", os.environ.get("ESEP_DB_PASSWORD"))
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.mysql',
        "NAME": os.getenv("MYSQL_DATABASE", "updaterninja"),
        "USER": os.getenv("MYSQL_USER", "root"),
        "PASSWORD": os.getenv("MYSQL_ROOT_PASSWORD", "ninjapassword"),
        "HOST": os.getenv("MYSQL_HOST", "db"),
        "PORT": os.getenv("MYSQL_PORT", "3306"),
        'OPTIONS': {
            'init_command': "SET sql_mode='STRICT_TRANS_TABLES'"
        }
    }
}

ESEP_DB_HOST = os.getenv("ESEP_DB_HOST")
ESEP_DB_USER = os.getenv("ESEP_DB_USER")
ESEP_DB_PASSWORD = os.getenv("ESEP_DB_PASSWORD")
ESEP_DB_NAME = os.getenv("ESEP_DB_NAME")

REDIS_HOST = os.getenv("REDIS_HOST", "ninja-redis")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")
REDIS_DB_FOR_CELERY = os.getenv("REDIS_DB_FOR_CELERY", "0")
REDIS_DB_FOR_CACHE = os.getenv("REDIS_DB_FOR_CACHE", "1")
REDIS_AS_CACHE_URL = "redis://{host}:{port}/{db_index}".format(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db_index=REDIS_DB_FOR_CACHE,
)
REDIS_AS_BROKER_URL = "redis://{host}:{port}/{db_index}".format(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db_index=REDIS_DB_FOR_CELERY,
)


CELERY_BROKER_URL = REDIS_AS_BROKER_URL
CELERY_BEAT_SCHEDULER = "django_celery_beat.schedulers.DatabaseScheduler"

CELERY_RESULT_EXTENDED = False
CELERY_RESULT_EXPIRES = 3600
CELERY_ALWAYS_EAGER = False
CELERY_ACKS_LATE = True
CELERY_TASK_PUBLISH_RETRY = False
CELERY_DISABLE_RATE_LIMITS = False
CELERY_TASK_TRACK_STARTED = True


CONSTANCE_BACKEND = 'constance.backends.database.DatabaseBackend'
CONSTANCE_CONFIG = {
    "ACA_SERVICE_HOST": ("https://88.204.161.86:4321", "Использовать код по умолчанию", str),
    "ACA_SERVICE_API_KEY": ("", "Код по умолчанию", str),
    "OUTDATED_DEVICES_PERIOD": (100, "Период устаревших данных", int)
}
CONSTANCE_CONFIG_FIELDSETS = OrderedDict([
    ("Configs", (
        "ACA_SERVICE_HOST",
        "ACA_SERVICE_API_KEY",
        "OUTDATED_DEVICES_PERIOD"
    )),
])
CONSTANCE_IGNORE_ADMIN_VERSION_CHECK = True
CONSTANCE_REDIS_CONNECTION = {
    'host': REDIS_HOST,
    'port': REDIS_PORT,
    'db': REDIS_DB_FOR_CACHE,
}


# Password validation
# https://docs.djangoproject.com/en/5.0/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


# Internationalization
# https://docs.djangoproject.com/en/5.0/topics/i18n/

LANGUAGE_CODE = 'ru'
TIME_ZONE = 'Asia/Almaty'

USE_I18N = True
USE_L10N = True
USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/5.0/howto/static-files/

STATIC_URL = "static/"
STATIC_ROOT = os.path.join(PROJECT_DIR, "..", 'static')
# STATIC_DIR = os.path.join(BASE_DIR, 'static')
# STATICFILES_DIRS = [STATIC_DIR]
MEDIA_URL = "media/"
MEDIA_ROOT = os.path.join(PROJECT_DIR, "media")

# Default primary key field type
# https://docs.djangoproject.com/en/5.0/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'


LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '%(levelname)s -- %(asctime)s -- %(message)s',
        },
        'simple': {
            'format': '%(levelname)s -- %(message)s',
        }
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'simple',
        },
    },
    'loggers': {
        'django': {
            'handlers': ['console'],
            'propagate': True,
        },
        'common': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': True,
        }
    }
}
