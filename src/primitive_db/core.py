# src/primitive_db/core.py

import os

from prettytable import PrettyTable

from .constrants import DATA_TYPES, TABLES_DATAPATH
from .decorators import confirm_action, create_cacher, handle_db_errors, log_time


@handle_db_errors
def create_table(metadata, table_name, columns):
    """
    Функция для создания таблицы

    Параметры:
        metadata - словарь, содержит текущие метаданные
        table_name - стркоа, содержит имя таблицы
        columns - список, содержит список столбцов
    """
    if table_name in metadata:
        raise ValueError(f'Таблица "{table_name}" уже существует.')
    
    parsed_cols = []
    user_defined_id = False
    
    for col in columns:
        if ':' not in col:
            raise ValueError(f"Некорректный формат столбца: {col}. "
                             "Используйте формат 'имя:тип'")
        
        col_name, col_type = col.split(':', 1)
        col_name = col_name.strip()
        col_type = col_type.strip()

        if not col_name or col_name == "":
            raise ValueError("Имя столбца не может быть пустым")
        
        if col_type not in DATA_TYPES:
            raise ValueError(f"Неподдерживаемый тип данных: {col_type}. "
                             f"Допустимые типы: {', '.join(DATA_TYPES)}")
        
        if col_name.upper() == 'ID':
            user_defined_id = True
            if col_type != 'int':
                raise ValueError('Столбец ID должен '
                    f'иметь тип "int", а не "{col_type}"')
        
        parsed_cols.append({'name': col_name, 'type': col_type})
    
    if not user_defined_id:
        parsed_cols.insert(0, {'name': 'ID', 'type': 'int'})
    
    metadata[table_name] = {'columns': parsed_cols}
    
    return metadata

@handle_db_errors
@confirm_action("удаление таблицы")
def drop_table(metadata, table_name):
    """
    Функция для создания таблицы

    Параметры:
        metadata - словарь, содержит текущие метаданные
        table_name - стркоа, содержит имя таблицы
    """
    if table_name not in metadata:
        raise ValueError(f'Таблица "{table_name}" не существует.')

    table_datapath = os.path.join(TABLES_DATAPATH, f"{table_name}.json")
    try:
        if os.path.exists(table_datapath):
            os.remove(table_datapath)
    except OSError as e:
        print(f"Ошибка: не удалось удалить файл для таблицы {table_name}: {e}")
    
    del metadata[table_name]
    return metadata

def list_tables(metadata):
    tables = list(metadata.keys())
    if tables:
        for table in tables:
            print(f"- {table}")
    else:
        print("Нет созданных таблиц.")

@handle_db_errors
def insert(metadata, table_name, values):
    """
    Функция для получения новых записей в таблицу
    
    Параметры:
        metadata - словарь, содержит текущие метаданные
        table_name - стркоа, содержит имя таблицы
        values - список, содержит значения
        
    Возвращает:
        new_record - словарь, содержащий новую запись
    """
    if table_name not in metadata:
        raise ValueError(f'Таблица "{table_name}" не существует.')
    
    table_cols = metadata[table_name]['columns']
    
    if len(values) != len(table_cols) - 1:
        raise ValueError(
            f"Неверное количество значений. Ожидается {len(table_cols)-1}, "
            f"получено {len(values)}")
    
    new_record = {}
    for i in range(1, len(table_cols)):
        col_name = table_cols[i]['name']
        col_type = table_cols[i]['type']
        value = values[i-1]
        
        try:
            if col_type == 'int':
                new_record[col_name] = int(value)

            elif col_type == 'str':
                if isinstance(value, str) and len(value) >= 2 \
                and value[0] == value[-1] and value[0] in ['"', "'"]:
                    new_record[col_name] = value[1:-1]
                else:
                    new_record[col_name] = str(value)

            elif col_type == 'bool':
                if isinstance(value, str):
                    if value.lower() == 'true':
                        new_record[col_name] = True
                    elif value.lower() == 'false':
                        new_record[col_name] = False
                    else:
                        raise ValueError(f'Недопустимое значение для bool: {value}')
                else:
                    new_record[col_name] = bool(value)
        except (ValueError, TypeError):
            raise ValueError(
                f'Некорректное значение для столбца {col_name}: {value}. '
                f'Ожидается тип {col_type}.')
    
    return new_record

@handle_db_errors
@confirm_action("удаление записей")
def delete(table_data, where_clause):
    """
    Функция для удаления данных из таблицы
    
    Параметры:
        table_data - список, содержит словари с данными таблиц
        where_clause - словарь, содержит условием фильтрации
        
    Возвращает:
        new_data - список, содержит данные после удаления
        amount_deleted - целое число, содержит количество удаленных записей
    """
    if not where_clause:
        return [], len(table_data)
    
    new_data = []
    amount_deleted = 0
    
    for record in table_data:
        match = True
        for column, value in where_clause.items():
            if column not in record or record[column] != value:
                match = False
                break
        
        if match:
            amount_deleted += 1
        else:
            new_data.append(record)
    
    return new_data, amount_deleted

@handle_db_errors
def update(table_data, set_clause, where_clause):
    """
    Функция для обновления данных в таблице
    
    Параметры:
        table_data - список, содержит словари с данными таблиц
        set_clause - словарь, содержит новое значение
        where_clause - словарь, содержит условием фильтрации
        
    Возвращает:
        table_data - список, содержит обновленные данные таблиц
        amount_updated - целое число, содержить количество обновленных записей
    """
    amount_updated = 0
    
    for record in table_data:
        match = True
        for column, value in where_clause.items():
            if column not in record or record[column] != value:
                match = False
                break
        if match:
            amount_updated += 1
            for column, new_value in set_clause.items():
                if column in record:
                    record[column] = new_value
    
    return table_data, amount_updated

select_cache, clear_select_cache = create_cacher()

@handle_db_errors
@log_time
def select(table_data, where_clause=None):
    """
    Функция для выборки данных из таблицы
    
    Параметры:
        table_data - список, содержит словари с данными таблиц
        where_clause - словарь, содержит условием фильтрации
        
    Возвращает:
        filtered_data - список, содержит отфильтрованные данные
    """
    if not table_data:
        return []
    
    if where_clause is None:
        cache_key = "all_records"
    else:
        cache_key = str(sorted(where_clause.items()))
    
    def fetch_data():
        if where_clause is None:
            return table_data
        filtered_data = []
        for record in table_data:
            match = True
            for column, value in where_clause.items():
                if column not in record or record[column] != value:
                    match = False
                    break
            
            if match:
                filtered_data.append(record)
        
        return filtered_data
    return select_cache(cache_key, fetch_data)

def print_table_info(metadata, table_name, table_data):
    """
    Функция для вывода информации о таблице
    
    Параметры:
        metadata - словарь, содержит текущие метаданные
        table_name - стркоа, содержит имя таблицы
        table_data - список, содержит словари с данными таблиц
    """
    if table_name not in metadata:
        raise ValueError(f'Таблица "{table_name}" не существует.')
    
    columns_info = metadata[table_name]['columns']
    columns_str = ', '.join(f"{col['name']}:{col['type']}" for col in columns_info)
    count = len(table_data)
    
    print(f'Таблица: {table_name}\nСтолбцы: {columns_str}\n'
        f'Количество записей: {count}')

def display_table(table_data, columns):
    """
    Функция для вывода содержимого таблицы

    Параметры:
        table_data - список, содержит словари с данными таблиц
        columns - список, содержит столбцы таблицы
    """
    if not table_data:
        print("Таблица пустая.")
        return
    
    table = PrettyTable()
    
    table.field_names = [col['name'] for col in columns]
    
    for record in table_data:
        row = []
        for col in columns:
            col_name = col['name']
            value = record.get(col_name, '')
            row.append(value)
        table.add_row(row)
    
    print(table)