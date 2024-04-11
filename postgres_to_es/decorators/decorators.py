import random
import time
from functools import wraps

import psycopg2
from psycopg2.extras import DictCursor
from settings.settings import settings


def backoff(exceptions: tuple, start_sleep_time=0.1, factor=2, border_sleep_time=100):
    """
    Декоратор для повторного выполнения функции через некоторое время, если возникла ошибка.
    Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)

    :param start_sleep_time: начальное время ожидания
    :param factor: во сколько раз нужно увеличивать время ожидания на каждой итерации
    :param border_sleep_time: максимальное время ожидания
    :param exceptions: список исключений
    :return: результат выполнения функции

    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            attempt = 0
            max_attempt = 100
            delay = start_sleep_time * factor
            logger = args[0].logger
            while delay < border_sleep_time and attempt < max_attempt:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    logger.info(e)
                    logger.info(f'Ждем {delay} секунд для повторения запроса')
                    time.sleep(delay)
                    attempt += 1
                    temp = min(border_sleep_time, start_sleep_time * 2 ** attempt)
                    delay = temp / 2 + random.uniform(0, temp / 2)
            return func(*args, **kwargs)
        return inner
    return func_wrapper


def db_reconnect(start_sleep_time=0.1, factor=2, border_sleep_time=100):
    """
    Декоратор для проверки актуальности подключения к БД.
    В случае разрыва соединения, создается новое подключюние к БД.
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            attempt = 0
            max_attempt = 100
            delay = start_sleep_time * factor
            logger = args[0].logger
            pg_conn = args[0].pg_conn
            if pg_conn.closed == 1:
                while delay < border_sleep_time and attempt < max_attempt:
                    try:
                        pg_conn = psycopg2.connect(**settings.postgres, cursor_factory=DictCursor)
                        args[0].pg_conn = pg_conn
                        return func(*args, **kwargs)
                    except psycopg2.Error as e:
                        pg_conn.close()
                        logger.info(e)
                        logger.info(f'Ждем {delay} секунд для повторения запроса')
                        time.sleep(delay)
                        attempt += 1
                        temp = min(border_sleep_time, start_sleep_time * 2 ** attempt)
                        delay = temp / 2 + random.uniform(0, temp / 2)
            return func(*args, **kwargs)
        return inner
    return func_wrapper
