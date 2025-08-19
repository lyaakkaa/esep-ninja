import mysql.connector
from django.conf import settings


def get_connection_to_esep_db():
    return mysql.connector.connect(
        host="db",
        port="3306",
        user="root",
        password="ninjapassword",
        database="esep",
        connection_timeout=60,  # ждём до 60 секунд при handshake :contentReference[oaicite:1]{index=1}
        autocommit=True
    )
