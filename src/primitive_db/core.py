# src/primitive_db/core.py

from .constrants import DATA_TYPES


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

    print("HERE --------->", columns)
    
    for col in columns:
        if ':' not in col:
            raise ValueError(f"Некорректный формат столбца: {col}. "
                             "Используйте формат 'имя:тип'")
        
        col_name, col_type = col.split(':', 1)
        col_name = col_name.strip()
        col_type = col_type.strip()
        
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

def drop_table(metadata, table_name):
    """
    Функция для создания таблицы

    Параметры:
        metadata - словарь, содержит текущие метаданные
        table_name - стркоа, содержит имя таблицы
    """
    if table_name not in metadata:
        raise ValueError(f'Таблица "{table_name}" не существует.')
    
    del metadata[table_name]
    return metadata

def list_tables(metadata):
    tables = list(metadata.keys())
    if tables:
        for table in tables:
            print(f"- {table}")
    else:
        print("Нет созданных таблиц.")