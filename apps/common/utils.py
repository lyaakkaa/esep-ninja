import base64
import json
import csv
import logging
import re
from typing import Optional, List
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

def _safe_digits_only(x) -> str:
    """Возвращает строку, содержащую только цифры из x."""
    try:
        return re.sub(r'\D', '', str(x))
    except Exception:
        return ""


def _safe_split_ws(x) -> List[str]:
    """Безопасное разбиение по пробельным символам (минимум один элемент)."""
    try:
        return [p for p in re.split(r'\s+', str(x)) if p]
    except Exception:
        return [str(x)]


def safe_int_or_none(val):
    """Пытается привести val к int, если нельзя — возвращает None (без исключений)."""
    if val is None:
        return None
    try:
        if isinstance(val, bool):
            return int(val)
        if isinstance(val, (int, float)):
            return int(val)
        s = str(val)
    except Exception:
        return None
    digits = _safe_digits_only(s)
    if not digits:
        return None
    try:
        return int(digits)
    except Exception:
        return None


def normalize_phone(s: Optional[str]) -> Optional[str]:
    """Нормализует телефон до цифр или возвращает None."""
    if s is None:
        return None
    digits = _safe_digits_only(s)
    return digits or None
