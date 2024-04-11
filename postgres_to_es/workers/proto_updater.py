import json
from typing import Any

import psycopg2
import requests
from psycopg2._psycopg import connection as _connection
from redis import Redis

from decorators.decorators import backoff, db_reconnect
from logger.logger import CustomLogger
from models.models import Film
from settings.settings import settings


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
        with self.pg_conn.cursor() as cur:
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
        elastic_host = settings.elasticsearch.host
        elastic_port = settings.elasticsearch.port
        url = f'{elastic_host}:{elastic_port}/_bulk?filter_path=items.*.error'
        headers = {'content-type': 'application/x-ndjson'}
        response = requests.post(url, data=payload_data, headers=headers)
        if not response.ok:
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
