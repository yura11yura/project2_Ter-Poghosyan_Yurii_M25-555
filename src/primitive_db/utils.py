# src/primitive_db/utils.py

import json

from .constrants import DEFAULT_DATAPATH

def load_metadata(filepath=DEFAULT_DATAPATH):
    """
    Функция для загрузки данных из JSON файла

    Параметры:
        filepath - строка, путь к json файлу
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def save_metadata(data, filepath=DEFAULT_DATAPATH):
    """
    Функция для сохранения данных в JSON файл

    Параметры:
        data - словарь, данные для записи
        filepath - строка, путь к json файлу
    """
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)