import datetime
import json
import os
import time
from contextlib import closing
from dataclasses import dataclass
from functools import wraps
from pprint import pprint
from typing import Any
from uuid import UUID

import psycopg2
import pydantic
import requests
from dotenv import load_dotenv
from psycopg2 import InterfaceError, OperationalError
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

load_dotenv()

dsl = {
    'dbname': os.environ.get('POSTGRES_DB'),
    'user': os.environ.get('POSTGRES_USER'),
    'password': os.environ.get('POSTGRES_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'port': os.environ.get('DB_PORT'),
}

@dataclass
class Film:
    # class Film(pydantic.BaseModel):
    fw_id: UUID
    title: str
    rating: float
    type: str
    created_at: datetime.datetime
    updated_at: datetime.datetime
    writers_names: list[str]
    directors_names: list[str]
    actors_names: list[str]
    writers: list[dict]
    actors: list[dict]
    directors: list[dict]
    genres: list[str]
    description: str | None = pydantic.Field(default='Description in progress...')


# TODO настроить респонс из эластика с фильтрами по ошибкам - OK
# TODO загрузка по жанрам - ОК
# TODO загрузка по фильмам - ОК
# TODO backoff - эластик ОК, сделать для постгри
# TODO backoff - передавать в декоратор список эксепшенов для каждого метода (че с ним делать дальше?)
# TODO backoff - считать среднее кол-во обратываемых запросов в день для мониторинга работы ETL, уведомлять при большой дельте от среднего числа
# TODO redis (хранить состояние) -
# TODO упаковка в докер


# TODO: добавить jitter в бэкофф
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
            while delay < border_sleep_time:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    print(f'waiting {delay} seconds before retry')
                    time.sleep(delay)
                    delay = delay * factor
            return func(*args, **kwargs)
        return inner
    return func_wrapper


# TODO  протестить реконнекты к базе, определить откуда продолжается выгрузка данных из БД
def db_reconnect(start_sleep_time=0.1, factor=2, border_sleep_time=100):
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            delay = start_sleep_time * factor
            if args[0].pg_conn.closed != 0:
                while delay < border_sleep_time:
                    try:
                        pg_conn = psycopg2.connect(**dsl, cursor_factory=DictCursor)
                        args[0].pg_conn = pg_conn
                        return func(*args, **kwargs)
                    except psycopg2.Error:
                        print(f'waiting {delay} seconds before retry')
                        time.sleep(delay)
                        delay = delay * factor
            return func(*args, **kwargs)
        return inner
    return func_wrapper


class ProtoUpdater:
    default_last_update_time = '2021-06-16 23:00:00.000 +0300'
    persons_last_update: str = default_last_update_time
    genres_last_update: str = default_last_update_time
    films_last_update: str = default_last_update_time
    already_updated_films = []
    pg_conn: _connection

    def __init__(self, pg_conn: _connection):
        self.pg_conn = pg_conn

    @backoff(exceptions=(psycopg2.InterfaceError, psycopg2.OperationalError))
    @db_reconnect()
    def extract_selected_films(self, selected_films: tuple[Any]):
        """Достаем выбранные фильмы со всеми данными"""
        # cur = self.pg_conn.cursor()
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
        pre_data = []
        for film in data:
            pre_data.append({"index": {"_index": "movies", "_id": film.fw_id}})
            pre_data.append(
                {
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

    @staticmethod
    @backoff(exceptions=(ConnectionError,))
    def load_data_to_elasticsearch(payload_data):
        url = 'http://127.0.0.1:9200/_bulk?filter_path=items.*.error'
        headers = {'content-type': 'application/x-ndjson'}
        response = requests.post(url, data=payload_data, headers=headers)
        if not response.status_code == 200:
            pprint(f'Errors found: {response.text}')

    def _check_if_film_already_updated(self, data):
        """
        Проверяет, если фильм уже был обновлен в текущем цикле обновления.
        Возвращает кортеж требующих обновления фильмов
        """
        selected_films = []
        for film in data:
            if film not in self.already_updated_films:
                self.already_updated_films.append(film)
                selected_films.append(film[0])
        return tuple(selected_films)


class UpdateByPerson(ProtoUpdater):
    needs_to_update: bool = True
    limit_rows = 100  # TODO Debug
    # pg_conn: _connection

    @backoff(exceptions=(psycopg2.InterfaceError, psycopg2.OperationalError))
    @db_reconnect()
    def extract_persons_data(self) -> tuple[str] | None:
        """Достаем всех person у которых менялись данные (по дате обновления)"""
        cur = self.pg_conn.cursor()
        cur.execute(
            f"SELECT id, updated_at "
            f"FROM content.person "
            f"WHERE updated_at > '{self.persons_last_update}' "
            f"ORDER BY updated_at "
            f"LIMIT {self.limit_rows};"
        )
        persons = cur.fetchall()
        if not persons:
            self.needs_to_update = False
            return
        persons_id = tuple(person[0] for person in persons)

        # достаем список фильмов в которых они снимались
        cur.execute(
            f"SELECT fw.id, fw.updated_at "
            f"FROM content.film_work fw "
            f"LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id "
            f"WHERE pfw.person_id IN {persons_id} "
            f'GROUP BY fw.id '
            f"ORDER BY fw.updated_at "
        )
        data = cur.fetchall()

        self.persons_last_update = persons[-1]['updated_at']
        selected_films = self._check_if_film_already_updated(data)
        if not selected_films:
            self.needs_to_update = False
            return
        return selected_films


class UpdateByGenre(ProtoUpdater):
    needs_to_update: bool = True
    limit_obj_per_fetch = 100

    @backoff(exceptions=(psycopg2.InterfaceError, psycopg2.OperationalError))
    @db_reconnect()
    def extract_genres_data(self) -> tuple[str] | None:  # TODO rename
        cur = self.pg_conn.cursor()
        cur.execute(
            f"SELECT id, updated_at "
            f"FROM content.genre "
            f"WHERE updated_at > '{self.genres_last_update}' "
            f"ORDER BY updated_at "
        )
        genres = cur.fetchall()
        if not genres:
            self.needs_to_update = False
            return
        genres_id = tuple(genre[0] for genre in genres)

        # достаем фильмы с указанными жанрами
        cur.execute(
            f"SELECT fw.id, fw.updated_at "
            f"FROM content.film_work fw "
            f"LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id "
            f"WHERE gfw.genre_id IN {genres_id} "
            f'GROUP BY fw.id '
            f"ORDER BY fw.updated_at "
        )
        self.genres_last_update = genres[-1]['updated_at']
        while data := cur.fetchmany(self.limit_obj_per_fetch):
            selected_films = self._check_if_film_already_updated(data)
            if not selected_films:
                self.needs_to_update = False
                return
            films_data = self.extract_selected_films(selected_films)
            payload_data = self.prepare_data_to_load(films_data)
            self.load_data_to_elasticsearch(payload_data)


class UpdateByFilm(ProtoUpdater):
    needs_to_update: bool = True
    limit_rows = 100

    @backoff(exceptions=(psycopg2.InterfaceError, psycopg2.OperationalError))
    @db_reconnect()
    def extract_films_data(self) -> tuple[str] | None:  # TODO rename
        cur = self.pg_conn.cursor()
        cur.execute(
            f"SELECT id, updated_at "
            f"FROM content.film_work "
            f"WHERE updated_at > '{self.films_last_update}' "
            f"ORDER BY updated_at "
            f"LIMIT {self.limit_rows} "
        )
        films = cur.fetchall()
        selected_films = self._check_if_film_already_updated(films)
        if not selected_films:
            self.needs_to_update = False
            return
        self.films_last_update = films[-1]['updated_at']
        return selected_films


def main():
    dsl = {
        'dbname': os.environ.get('POSTGRES_DB'),
        'user': os.environ.get('POSTGRES_USER'),
        'password': os.environ.get('POSTGRES_PASSWORD'),
        'host': os.environ.get('DB_HOST'),
        'port': os.environ.get('DB_PORT'),
    }
    with closing(psycopg2.connect(**dsl, cursor_factory=DictCursor)) as pg_conn:
        update_by_person = UpdateByPerson(pg_conn)
        update_by_genre = UpdateByGenre(pg_conn) # TODO что если предыдущий конн уже закрылся, а актуальный коннект лежит в протоклассе. Можно ли хранить актуальный коннект в редис
        # update_by_film = UpdateByFilm(pg_conn)

        print('запускаем апдейт персоналий')
        while update_by_person.needs_to_update is True:
            if selected_films := update_by_person.extract_persons_data():
                film_data = update_by_person.extract_selected_films(selected_films)
                payload_data = update_by_person.prepare_data_to_load(film_data)
                update_by_person.load_data_to_elasticsearch(payload_data)
            else:
                print('log: no persons to update')

        print('запускаем апдейт жанров')
        # check_if_db_was_reconnected()
        while update_by_genre.needs_to_update is True:
            update_by_genre.extract_genres_data()
        print('log: no genres to update')

        # print('запускаем апдейт фильмов')
        # while update_by_film.needs_to_update is True:
        #     if selected_films := update_by_film.extract_films_data():
        #         film_data = update_by_film.extract_selected_films(selected_films)
        #
        #         payload_data = update_by_film.prepare_data_to_load(film_data)
        #         update_by_film.load_data_to_elasticsearch(payload_data)
        #     else:
        #         print('log: no films to update')
        # print('all objects were updated. Next update start in ...')
        #
        # update_by_film.already_updated_films.clear()

# def check_if_db_was_reconnected(prev_updater, next_updater) -> bool:
#     if update_by_genre.pg_conn != update_by_person.pg_conn:  # если разорвался коннект в предыдущем апдейте, и был создан новый.
#         update_by_genre.pg_conn = update_by_person.pg_conn  # то в следующем апдейте используем созданный коннект
#

if __name__ == '__main__':
    main()

# TODO вывести код из мейн в отдельные функкции