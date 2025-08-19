import mysql.connector
from django.conf import settings


def get_connection_to_esep_db():
    return mysql.connector.connect(
        host=settings.ESEP_DB_HOST,
        user=settings.ESEP_DB_USER,
        password=settings.ESEP_DB_PASSWORD,
        database=settings.ESEP_DB_NAME
    )
