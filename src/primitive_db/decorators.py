# src/primitive_db/decorators.py

import time
from functools import wraps


def handle_db_errors(func):
    """
    Декоратор для обработки ошибок базы данных

    Параметры:
        func - функция, для которой необходимо вернуть wrapper
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except FileNotFoundError:
            print("Ошибка: Файл данных не найден. Возможно, "
                "база данных не инициализирована.")
            return None
        except KeyError as e:
            print(f"Ошибка: Таблица или столбец {e} не найден.")
            return None
        except ValueError as e:
            print(f"Ошибка валидации: {e}")
            return None
        except Exception as e:
            print(f"Произошла непредвиденная ошибка: {e}")
            return None
    return wrapper

def confirm_action(action_name):
    """
    Декоратор для подтверждения опасных операций

    Параметры:
        action_name - строка, содержит название операции
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            response = input('Вы уверены, что хотите '
                f'выполнить "{action_name}"? [y/n]: ').strip().lower()
            if response != 'y':
                print("Операция отменена.")
                return None
            return func(*args, **kwargs)
        return wrapper
    return decorator

def log_time(func):
    """
    Декоратор для замера времени выполнения функции
    
    Параметры:
        func - функция, для которой необходимо вернуть wrapper
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.monotonic()
        result = func(*args, **kwargs)
        end_time = time.monotonic()
        elapsed = end_time - start_time
        print(f"Функция {func.__name__} выполнилась за {elapsed:.3f} секунд.")
        return result
    return wrapper

def create_cacher():
    """
    Функция с замыканием для кэширования

    Возвращает:
        cache_result(key, value_func) - внутренняя функция, зранит кэш
    """
    cache = {}
    
    def cache_result(key, value_func):
        if key in cache:
            return cache[key]
        
        result = value_func()
        cache[key] = result
        return result
    
    return cache_result