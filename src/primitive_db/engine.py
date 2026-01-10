# src/primitive_db/engine.py

import shlex

from .core import create_table, drop_table, list_tables
from .utils import load_metadata, save_metadata


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
    print("\nОбщие команды:")
    print("<command> exit - выход из программы")
    print("<command> help - справочная информация\n")

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
                    save_metadata(metadata)
                    print(f'Таблица "{table_name}" успешно удалена.')
                    
                except ValueError as e:
                    print(f"Ошибка: {e}")
                    
            elif command == "list_tables":
                list_tables(metadata)

            else:
                print(f"Функции '{command}' нет. Попробуйте снова.")
                
        except KeyboardInterrupt:
            print("\n\nВыход из программы.")
            break
        except Exception as e:
            print(f"Произошла непредвиденная ошибка: {e}")