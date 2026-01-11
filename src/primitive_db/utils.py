# src/primitive_db/utils.py

import json
import os

from .constrants import DB_INFO_DATAPATH, TABLES_DATAPATH


def load_metadata(filepath=DB_INFO_DATAPATH):
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

def save_metadata(data, filepath=DB_INFO_DATAPATH):
    """
    Функция для сохранения данных в JSON файл

    Параметры:
        data - словарь, данные для записи
        filepath - строка, путь к json файлу
    """
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def load_table_data(table_name):
    """
    Функция для загрузки таблицы из JSON файла

    Параметры:
        table_name - строка, содержит название таблицы
    """
    filepath = os.path.join(TABLES_DATAPATH, f"{table_name}.json")
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return []

def save_table_data(table_name, data):
    """
    Функция для сохранения таблицы в JSON файл

    Параметры:
        table_name - строка, содержит название таблицы
        data - словарь, содержит метадату таблицы
    """
    os.makedirs(TABLES_DATAPATH, exist_ok=True)
    
    filepath = os.path.join(TABLES_DATAPATH, f"{table_name}.json")
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)