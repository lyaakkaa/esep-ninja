import base64
import json

from django.core.files.storage import FileSystemStorage


class OverwriteStorage(FileSystemStorage):
    def get_available_name(self, name, max_length=None):
        self.delete(name)
        return name


def b64_encode(raw_data: dict) -> str:
    return base64.b64encode(
        json.dumps(raw_data).encode('utf-8')
    ).decode('ascii')


def b64_decode(encoded_data: str) -> dict:
    return json.loads(
        base64.b64decode(encoded_data.encode('utf-8')).decode('utf-8')
    )
