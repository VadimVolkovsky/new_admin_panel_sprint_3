import datetime
import json
import logging
import os
import sys
import time
from contextlib import closing
from dataclasses import dataclass
from functools import wraps
from typing import Any
from uuid import UUID

import psycopg2
import pydantic
import requests
from dotenv import load_dotenv
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from redis import Redis

load_dotenv()

dsl = {
    'dbname': os.environ.get('POSTGRES_DB'),
    'user': os.environ.get('POSTGRES_USER'),
    'password': os.environ.get('POSTGRES_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'port': os.environ.get('DB_PORT'),
}

redis_connection = {
    'host': os.environ.get('REDIS_HOST'),
    'port': os.environ.get('REDIS_PORT'),
}

# update_period = 21600  # каждые 6 часов
update_period = 30


@dataclass
class Film:
    fw_id: UUID
    title: str
    rating: float
    type: str
    created_at: datetime.datetime
    updated_at: datetime.datetime
    writers: list[dict]
    actors: list[dict]
    directors: list[dict]
    genres: list[str]
    writers_names: list[str] = pydantic.Field(default=[])
    directors_names: list[str] = pydantic.Field(default=[])
    actors_names: list[str] = pydantic.Field(default=[])
    description: str | None = pydantic.Field(default='Description in progress...')


# TODO считать среднее кол-во обратываемых запросов в день для мониторинга работы ETL, уведомлять при большой дельте от среднего числа  # noqa E501
# TODO упаковка в докер


# # TODO: добавить jitter в бэкофф
def backoff(exceptions: tuple, start_sleep_time=0.1, factor=2, border_sleep_time=100):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка.
    Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * (factor ^ n), если t < border_sleep_time
        t = border_sleep_time, иначе
    :param start_sleep_time: начальное время ожидания
    :param factor: во сколько раз нужно увеличивать время ожидания на каждой итерации
    :param border_sleep_time: максимальное время ожидания
    :return: результат выполнения функции
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            delay = start_sleep_time * factor
            logger = args[0].logger
            while delay < border_sleep_time:
                try:
                    return func(*args, **kwargs)
                except exceptions:
                    logger.info(f'Ждем {delay} секунд для повторения запроса')
                    time.sleep(delay)
                    delay = delay * factor
            return func(*args, **kwargs)

        return inner
    return func_wrapper


def db_reconnect(start_sleep_time=0.1, factor=2, border_sleep_time=100):
    """
    Функция для проверки актуальности подключения к БД.
    В случае разрыва соединения, создается новое подключюние к БД.
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            delay = start_sleep_time * factor
            logger = args[0].logger
            if args[0].pg_conn.closed != 0:
                while delay < border_sleep_time:
                    try:
                        pg_conn = psycopg2.connect(**dsl, cursor_factory=DictCursor)
                        args[0].pg_conn = pg_conn
                        return func(*args, **kwargs)
                    except psycopg2.Error:
                        logger.info(f'Ждем {delay} секунд для повторения запроса')
                        time.sleep(delay)
                        delay = delay * factor
            return func(*args, **kwargs)
        return inner
    return func_wrapper


class CustomLogger(logging.Logger):
    def __init__(self, name: str):
        super().__init__(name)
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        self.addHandler(console_handler)
        self.setLevel(level='INFO')


class ProtoUpdater:
    """Родительский класс для объединения общих методов и аттрибутов дочерних классов."""
    already_updated_films: list = []
    pg_conn: _connection
    redis: Redis

    def __init__(self, pg_conn: _connection, redis: Redis):
        self.pg_conn = pg_conn
        self.redis = redis
        self.logger = CustomLogger(name=self.__class__.__name__)

    @backoff(exceptions=(psycopg2.InterfaceError, psycopg2.OperationalError))
    @db_reconnect()
    # def extract_selected_films(self, selected_films: tuple[Any, ...]):
    def extract_selected_films(self, selected_films: tuple[Any, ...]):
        """Извлекаем из БД выбранные фильмы со всеми необходимыми данными"""
        cur = self.pg_conn.cursor()
        cur.execute(
            f"SELECT "
            f"fw.id as fw_id, "
            f"fw.title, "
            f"fw.description, "
            f"fw.rating, "
            f"fw.type, "
            f"fw.created_at, "
            f"fw.updated_at, "
            f"array_agg(DISTINCT p.full_name) FILTER(WHERE pfw.role='writer') AS writers_names, "
            f"array_agg(DISTINCT p.full_name) FILTER(WHERE pfw.role='actor') AS actors_names, "
            f"array_agg(DISTINCT p.full_name) FILTER(WHERE pfw.role='director') AS directors_names, "
            f"array_agg(DISTINCT g.name) AS genres, "
            f"json_agg(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) FILTER(WHERE pfw.role='writer') AS writers, "
            f"json_agg(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) FILTER(WHERE pfw.role='actor') AS actors, "
            f"json_agg(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) FILTER(WHERE pfw.role='director') AS directors "
            f"FROM content.film_work fw "
            f"LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id "
            f"LEFT JOIN content.person p ON p.id = pfw.person_id "
            f"LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id "
            f"LEFT JOIN content.genre g ON g.id = gfw.genre_id "
            f"WHERE fw.id IN {selected_films} "
            f"GROUP BY fw_id;"
        )
        data = cur.fetchall()
        result: list[Film] = []
        for obj in data:
            result.append(Film(**obj))
        return result

    @staticmethod
    def prepare_data_to_load(data: list[Film]):
        """Подготовка данных для загрузки в ElasticSearch"""
        pre_data = []
        for film in data:
            pre_data.append({"index": {"_index": "movies", "_id": film.fw_id}})
            pre_data.append(
                {
                    "id": film.fw_id,
                    "imdb_rating": film.rating,
                    "genres": film.genres,
                    "title": film.title,
                    "description": film.description,
                    "directors_names": film.directors_names,
                    "actors_names": film.actors_names,
                    "writers_names": film.writers_names,
                    "directors": film.directors,
                    "actors": film.actors,
                    "writers": film.writers
                }
            )
        payload_data = '\n'.join(json.dumps(data) for data in pre_data) + '\n'
        return payload_data

    @backoff(exceptions=(requests.ConnectionError,))
    def load_data_to_elasticsearch(self, payload_data):
        """Загрузка данных в ElasticSearch"""
        url = 'http://127.0.0.1:9200/_bulk?filter_path=items.*.error'
        headers = {'content-type': 'application/x-ndjson'}
        response = requests.post(url, data=payload_data, headers=headers)
        if not response.status_code == 200:
            self.logger.info(f'Errors found: {response.text}')

    def _check_if_film_already_updated(self, data) -> tuple[Any, ...]:
        """
        Проверяет, если фильм уже был обновлен в текущем цикле обновления, для избежания повторного обновления.
        Возвращает кортеж требующих обновления фильмов
        """
        selected_films = []
        for film in data:
            if film not in self.already_updated_films:
                self.already_updated_films.append(film)
                selected_films.append(film[0])
        return tuple(selected_films)


class UpdateByPerson(ProtoUpdater):
    """Класс для проверки и обновления данных по персоналиям"""
    needs_to_update: bool = True
    limit_rows = 100  # TODO Debug

    @backoff(exceptions=(psycopg2.InterfaceError, psycopg2.OperationalError))
    @db_reconnect()
    def extract_persons_data(self) -> tuple[Any, ...] | None:
        """
        Извлекаем из БД всех персоналий, у которых изменились данные с момента последнего обновления
        """
        cur = self.pg_conn.cursor()
        persons_last_update = self.redis.get('persons_last_update').decode('utf-8')
        cur.execute(
            f"SELECT id, updated_at "
            f"FROM content.person "
            f"WHERE updated_at > '{persons_last_update}' "
            f"ORDER BY updated_at "
            f"LIMIT {self.limit_rows};"
        )
        persons = cur.fetchall()
        if not persons:
            self.needs_to_update = False
            return None
        persons_id = tuple(person[0] for person in persons)

        cur.execute(
            f"SELECT fw.id, fw.updated_at "
            f"FROM content.film_work fw "
            f"LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id "
            f"WHERE pfw.person_id IN {persons_id} "
            f'GROUP BY fw.id '
            f"ORDER BY fw.updated_at "
        )
        data = cur.fetchall()

        self.redis.set('persons_last_update', str(persons[-1]['updated_at']))
        # selected_films = self._check_if_film_already_updated(data)
        # if not selected_films:
        #     self.needs_to_update = False
        #     return None
        selected_films = []
        for film in data:
            selected_films.append(film[0])
        if not selected_films:
            self.needs_to_update = False
            return None
        return tuple(selected_films)


class UpdateByGenre(ProtoUpdater):
    """Класс для проверки и обновления данных по жанрам"""
    needs_to_update: bool = True
    limit_obj_per_fetch = 100

    @backoff(exceptions=(psycopg2.InterfaceError, psycopg2.OperationalError))
    @db_reconnect()
    def extract_and_load_genres_data(self) -> None:
        """
        Извлекаем из БД все жанры, у которых изменились данные с момента последнего обновления.
        Т.к. изменение жанров касается большого количества фильмов, важно извлекать объекты из БД частями,
        и сразу же подгружать обновленные данные в ElasticSearch.
        """
        cur = self.pg_conn.cursor()
        genres_last_update = self.redis.get('genres_last_update').decode('utf-8')
        cur.execute(
            f"SELECT id, updated_at "
            f"FROM content.genre "
            f"WHERE updated_at > '{genres_last_update}' "
            f"ORDER BY updated_at "
        )
        genres = cur.fetchall()
        if not genres:
            self.needs_to_update = False
            return
        genres_id = tuple(genre[0] for genre in genres)

        cur.execute(
            f"SELECT fw.id, fw.updated_at "
            f"FROM content.film_work fw "
            f"LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id "
            f"WHERE gfw.genre_id IN {genres_id} "
            f'GROUP BY fw.id '
            f"ORDER BY fw.updated_at "
        )
        self.redis.set('genres_last_update', str(genres[-1]['updated_at']))
        while data := cur.fetchmany(self.limit_obj_per_fetch):
            # selected_films = self._check_if_film_already_updated(data)
            # if not selected_films:
            #     self.needs_to_update = False
            #     return
            selected_films = []
            for film in data:
                selected_films.append(film[0])
            if not selected_films:
                self.needs_to_update = False
                return None
            selected_films = tuple(selected_films)  # TODO list comprehenshion
            films_data = self.extract_selected_films(selected_films)
            payload_data = self.prepare_data_to_load(films_data)
            self.load_data_to_elasticsearch(payload_data)


class UpdateByFilm(ProtoUpdater):
    """Класс для проверки и обновления данных по фильмам"""
    needs_to_update: bool = True
    limit_rows = 100

    @backoff(exceptions=(psycopg2.InterfaceError, psycopg2.OperationalError))
    @db_reconnect()
    def extract_films_data(self) -> tuple[Any, ...] | None:
        """
        Извлекаем из БД все фильмы, у которых изменились данные с момента последнего обновления
        """
        cur = self.pg_conn.cursor()
        films_last_update = self.redis.get('films_last_update').decode('utf-8')
        cur.execute(
            f"SELECT id, updated_at "
            f"FROM content.film_work "
            f"WHERE updated_at > '{films_last_update}' "
            f"ORDER BY updated_at "
            f"LIMIT {self.limit_rows} "
        )
        films = cur.fetchall()
        # selected_films = self._check_if_film_already_updated(films)
        # if not selected_films:
        #     self.needs_to_update = False
        #     return None
        selected_films = []
        for film in films:
            selected_films.append(film[0])
        if not selected_films:
            self.needs_to_update = False
            return None
        selected_films = tuple(selected_films)
        self.redis.set('films_last_update', str(films[-1]['updated_at']))
        return selected_films


def start_update_by_person(pg_conn: _connection, redis: Redis):
    """Запуск процесса обновления данных по персоналиям"""
    update_by_person = UpdateByPerson(pg_conn, redis)
    update_by_person.logger.info('Запускаем обновление по персоналиям')
    while update_by_person.needs_to_update is True:
        if selected_films := update_by_person.extract_persons_data():
            film_data = update_by_person.extract_selected_films(selected_films)
            payload_data = update_by_person.prepare_data_to_load(film_data)
            update_by_person.load_data_to_elasticsearch(payload_data)
        else:
            update_by_person.logger.info('Завершено обновление по персоналиям')
    return update_by_person.pg_conn


def start_update_by_genre(pg_conn: _connection, redis: Redis):
    """Запуск процесса обновления данных по жанрам"""
    update_by_genre = UpdateByGenre(pg_conn, redis)
    update_by_genre.logger.info('Запускаем обновление по жанрам')
    while update_by_genre.needs_to_update is True:
        update_by_genre.extract_and_load_genres_data()
    update_by_genre.logger.info('Завершено обновление по жанрам')
    return update_by_genre.pg_conn


def start_update_by_film(pg_conn: _connection, redis: Redis):
    """Запуск процесса обновления данных по фильмам"""
    update_by_film = UpdateByFilm(pg_conn, redis)
    update_by_film.logger.info('Запускаем обновление по фильмам')
    while update_by_film.needs_to_update is True:
        if selected_films := update_by_film.extract_films_data():
            film_data = update_by_film.extract_selected_films(selected_films)
            payload_data = update_by_film.prepare_data_to_load(film_data)
            update_by_film.load_data_to_elasticsearch(payload_data)
        else:
            update_by_film.logger.info('Завершено обновление по фильмам')
    update_by_film.logger.info(
        f'Обновление завершено. Следующее обновление начнется через {update_period} сек')
    update_by_film.already_updated_films.clear()


def main():
    redis = Redis(**redis_connection)
    while True:
        with closing(psycopg2.connect(**dsl, cursor_factory=DictCursor)) as pg_conn:
            pg_conn = start_update_by_person(pg_conn, redis)
            pg_conn = start_update_by_genre(pg_conn, redis)
            start_update_by_film(pg_conn, redis)
        time.sleep(update_period)


if __name__ == '__main__':
    main()
