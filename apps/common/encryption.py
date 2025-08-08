"""
    Данные функции используются при шифровании URL для оплаты, восстановлении пароля, подтверждения почты и тд.

    timestamp - 10-значное число
    В одних сутках 86400 секунд.
"""

from cryptography.fernet import Fernet
from django.conf import settings
import base64
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from django.contrib.auth import get_user_model
from django.utils import timezone

User = get_user_model()
minute = 60
twenty_four_hours = 86400


def to_byte(str):
    return f'{str}'.encode()


password_provided = settings.ENCRYPTION_KEY
password = password_provided.encode()
# print(os.urandom(16))
salt = b'2h\x8b\xb6\xa2\xe62\x7f\x0f\x89\xfe2\xa5\xbd\x89\xc7'
kdf = PBKDF2HMAC(
    algorithm=hashes.SHA256(),
    length=32,
    salt=salt,
    iterations=100000,
    backend=default_backend()
)
key = base64.urlsafe_b64encode(kdf.derive(password))
fernet_obj = Fernet(key)


def encrypt(data, with_timestamp=False):
    if with_timestamp:
        timestamp_str = str(int(timezone.now().timestamp()))
        data += timestamp_str
    return fernet_obj.encrypt(data.encode()).decode('ascii')


def decrypt(data, with_timestamp=False):
    data = fernet_obj.decrypt(data.encode('ascii')).decode('ascii')
    if with_timestamp:
        data_timestamp = int(data[-10:])
        curr_timestamp = timezone.now().timestamp()
        if curr_timestamp - data_timestamp <= twenty_four_hours:
            return data[:-10]
        return False
    return data


def encrypt_user(user: User) -> str:
    return encrypt(str(user.id))


def decrypt_and_get_user(encrypted_user: str) -> User:
    return User.objects.get(id=decrypt(encrypted_user))


# def decrypt_password_recovery(data):
#     credentials = decrypt(data)
#     if credentials:
#         index = credentials.find('password=')
#         email = credentials[:index]
#         password = credentials[index+9:]
#         print("==== decrypt password recovery =====")
#         print("email: ", email)
#         print("password: ", password)
#         user = utils.get_user_if_exists(email=email)
#         if user:
#             if user.password == password:
#                 return email
#
#     return False

