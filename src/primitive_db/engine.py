# src/primitive_db/engine.py

import shlex

from .core import (
    clear_select_cache,
    create_table,
    delete,
    display_table,
    drop_table,
    insert,
    list_tables,
    print_table_info,
    select,
    update,
)
from .decorators import handle_db_errors
from .utils import load_metadata, load_table_data, save_metadata, save_table_data


def print_help():
    """
    Функция для вывода вспомогательной информации
    """
    print("\n***База данных***")
    print("Функции:")
    print("<command> create_table <имя_таблицы> <столбец1:тип> "
        "<столбец2:тип> .. - создать таблицу")
    print("<command> list_tables - показать список всех таблиц")
    print("<command> drop_table <имя_таблицы> - удалить таблицу")
    print("<command> info <имя_таблицы> - информация о таблице")
    print("\n***Операции с данными***")
    print("Функции:")
    print("<command> insert into <имя_таблицы> values (<значение1>, "
        "<значение2>, ...) - создать запись.")
    print("<command> select from <имя_таблицы> where <столбец> = <значение> "
        "- прочитать записи по условию.")
    print("<command> select from <имя_таблицы> - прочитать все записи.")
    print("<command> update <имя_таблицы> set <столбец1> = <новое_значение1> "
        "where <столбец_условия> = <значение_условия> - обновить запись.")
    print("<command> delete from <имя_таблицы> where <столбец> = "
        "<значение> - удалить запись.")
    print("<command> info <имя_таблицы> - вывести информацию о таблице.")
    print("\nОбщие команды:")
    print("<command> exit - выход из программы")
    print("<command> help - справочная информация\n")

@handle_db_errors
def run():
    """
    Основной цикл программы
    """
    print_help()
    
    while True:
        try:
            metadata = load_metadata()
            
            user_input = input(">>>Введите команду: ").strip()
            
            if not user_input:
                continue
                
            args = shlex.split(user_input)
            command = args[0].lower()
            
            if command == "exit":
                print("Выход из программы.")
                break
                
            elif command == "help":
                print_help()
                
            elif command == "create_table":
                if len(args) < 3:
                    print("Ошибка: недостаточно аргументов. Формат: create_table "
                        "<имя> <столбец1:тип> ...")
                    continue
                
                table_name = args[1]
                columns = args[2:]
                
                try:
                    metadata = create_table(metadata, table_name, columns)

                    if metadata is not None:
                        save_metadata(metadata)
                        table_info = ', '.join(f"{col['name']}:{col['type']}" \
                            for col in metadata[table_name]['columns'])
                        print(f'Таблица "{table_name}" успешно создана '
                            f'со столбцами: {table_info}')
                except ValueError as e:
                    print(f"Ошибка: {e}")

            elif command == "drop_table":
                if len(args) != 2:
                    print("Ошибка: неверное количество аргументов. "
                        "Формат: drop_table <имя_таблицы>")
                    continue
                
                table_name = args[1]
                
                try:
                    metadata = drop_table(metadata, table_name)

                    if metadata is not None:
                        save_metadata(metadata)
                        print(f'Таблица "{table_name}" успешно удалена.')
                    
                except ValueError as e:
                    print(f"Ошибка: {e}")
                    
            elif command == "list_tables":
                list_tables(metadata)

            elif command == "insert":
                if len(args) < 5 or args[1].lower() != "into" or \
                    args[3].lower() != "values":
                    print("Ошибка: некорректный формат команды. "
                        "Формат: insert into <таблица> values (<значения>)")
                    continue
                
                table_name = args[2]
                
                values_str = ' '.join(args[4:])
                if not (values_str[0] == '(' and values_str[-1] == ')'):
                    print("Ошибка: значения должны быть в скобках")
                    continue
                
                values = [v.strip() for v in values_str[1:-1].split(',')]
                
                try:
                    table_data = load_table_data(table_name)
                    
                    new_record = insert(metadata, table_name, values)

                    if new_record is not None:
                        if table_data:
                            max_id = max(int(record.get('ID', 0)) 
                                for record in table_data)
                            new_id = max_id + 1
                        else:
                            new_id = 1
                        
                        new_record['ID'] = new_id
                        table_data.append(new_record)
                        
                        save_table_data(table_name, table_data)

                        clear_select_cache()
                        
                        print(f'Запись с ID={new_id} успешно добавлена '
                            f'в таблицу "{table_name}".')
                    
                except ValueError as e:
                    print(f"Ошибка: {e}")

            elif command == "delete":
                if len(args) < 5 or args[1].lower() != "from" or \
                    args[3].lower() != "where":
                    print("Ошибка: некорректный формат команды. "
                        "Формат: delete from <таблица> where <условие>")
                    continue
                
                table_name = args[2]
                
                if table_name not in metadata:
                    print(f'Ошибка: таблица "{table_name}" не существует.')
                    continue
                
                where_str = ' '.join(args[4:])
                
                try:
                    where_clause = parse_where(where_str)
                    
                    table_columns = [col['name'] for col in \
                        metadata[table_name]['columns']]
                    
                    if where_clause:
                        for column in where_clause.keys():
                            if column not in table_columns:
                                print(f'Ошибка: столбец "{column}" '
                                    f'не существует в таблице "{table_name}"')
                                continue
                    else:
                        print("Ошибка: некорректный формат команды. "
                        "Формат: delete from <таблица> where <условие>")
                        continue
                    
                    table_data = load_table_data(table_name)
                    new_data, deleted_count = delete(table_data, where_clause)
                    
                    if new_data is not None:
                        if deleted_count > 0:
                            save_table_data(table_name, new_data)
                            clear_select_cache()
                            print(f'Удалено {deleted_count} '
                                f'записей из таблицы "{table_name}".')
                        else:
                            print("Нет записей, соответствующих условию.")
                        
                except ValueError as e:
                    print(f"Ошибка: {e}")

            elif command == "info":
                if len(args) != 2:
                    print("Ошибка: некорректный формат команды. "
                        "Формат: info <таблица>")
                    continue
                
                table_name = args[1]
                
                try:
                    table_data = load_table_data(table_name)
                    print_table_info(metadata, table_name, table_data)
                    
                except ValueError as e:
                    print(f"Ошибка: {e}")

            elif command == "update":
                if len(args) < 8 or args[2].lower() != "set" or \
                    args[-4].lower() != "where":
                    print(args[2].lower())
                    print(args[-4].lower())
                    print("here Ошибка: некорректный формат команды. "
                        "Формат: update <таблица> set <столбец>=<значение> "
                        "where <условие>")
                    continue

                table_name = args[1]
                
                if table_name not in metadata:
                    print(f'Ошибка: таблица "{table_name}" не существует.')
                    continue
                
                set_str = ' '.join(args[3:args.index("where")])
                where_str = ' '.join(args[args.index("where") + 1:])
                
                try:
                    set_clause = parse_set(set_str)
                    where_clause = parse_where(where_str)
                    
                    table_columns = [col['name'] for col in \
                        metadata[table_name]['columns']]
                    
                    for column in set_clause.keys():
                        if column not in table_columns:
                            print(f'Ошибка: столбец "{column}" не '
                                f'существует в таблице "{table_name}"')
                            continue
                    
                    for column in where_clause.keys():
                        if column not in table_columns:
                            print(f'Ошибка: столбец "{column}" не '
                                f'существует в таблице "{table_name}"')
                            continue
                    
                    table_data = load_table_data(table_name)
                    updated_data, amount_updated = \
                        update(table_data, set_clause, where_clause)
                    
                    if updated_data is not None:
                        if amount_updated > 0:
                            save_table_data(table_name, updated_data)
                            clear_select_cache()
                            print(f'Обновлено {amount_updated} '
                                f'записей в таблице "{table_name}".')
                        else:
                            print("Нет записей, соответствующих условию.")
                        
                except ValueError as e:
                    print(f"Ошибка: {e}")
            
            elif command == "select":
                if len(args) < 3 or args[1].lower() != "from":
                    print("Ошибка: некорректный формат команды. "
                        "Формат: select from <таблица> [where <условие>]")
                    continue
                
                table_name = args[2]
                
                if table_name not in metadata:
                    print(f'Ошибка: таблица "{table_name}" не существует.')
                    continue
                
                where_clause = None
                if len(args) > 4 and args[3].lower() == "where":
                    where_str = ' '.join(args[4:])
                    try:
                        where_clause = parse_where(where_str)
                    except ValueError as e:
                        print(f"Ошибка в условии WHERE: {e}")
                        continue
                
                try:
                    table_data = load_table_data(table_name)
                    
                    if where_clause:
                        table_columns = [col['name'] for col in \
                            metadata[table_name]['columns']]
                        for column in where_clause.keys():
                            if column not in table_columns:
                                print(f'Ошибка: столбец "{column}" '
                                    'не существует в таблице "{table_name}"')
                                continue
                    
                    filtered_data = select(table_data, where_clause)
                    
                    if filtered_data is not None:
                        if filtered_data:
                            display_table(filtered_data, \
                                metadata[table_name]['columns'])
                        else:
                            print("Нет данных, соответствующих условию.")
                        
                except ValueError as e:
                    print(f"Ошибка: {e}")

            else:
                print(f"Функции '{command}' нет. Попробуйте снова.")
                
        except KeyboardInterrupt:
            print("\n\nВыход из программы.")
            break
        except Exception as e:
            print(f"Произошла непредвиденная ошибка: {e}")

def parse_where(where_clause):
    """
    Функция для парсинга where условия

    Параметры:
        where_clause - строка, содержит условие where
    """
    if not where_clause:
        return None
    
    where_clause = where_clause.strip()
    
    if '=' not in where_clause:
        raise ValueError("Некорректный формат WHERE. Ожидаемый формат: "
            "where <столбец> = <значение>")
    
    column, value = where_clause.split('=', 1)
    column = column.strip()
    value = value.strip()

    if column == "" or value == "":
        raise ValueError("Некорректный формат WHERE. Ожидаемый формат: "
            "where <столбец> = <значение>")

    return {column: parse_value(value)}

def parse_set(set_clause):
    """
    Функция для парсинга set выражения

    Параметры:
        set_clause - строка, содержит выражение set
    """
    if not set_clause:
        return {}
    
    conditions = set_clause.split(',')
    result = {}
    
    for condition in conditions:
        condition = condition.strip()
        if '=' not in condition:
            raise ValueError(f"Некорректное условие SET: {condition}")
        
        column, value = condition.split('=', 1)
        
        result[column.strip()] = parse_value(value.strip())
    
    return result

def parse_value(value):
    """
    Функция для преобразования значения в соответствующий тип
    """
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ['"', "'"]:
        return value[1:-1]
    
    if value.lower() == 'true':
        return True
    if value.lower() == 'false':
        return False
    
    try:
        return int(value)
    except ValueError:
        return value