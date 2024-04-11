import time
from contextlib import closing

import psycopg2

from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from redis import Redis

from settings.settings import settings
from workers.update_by_film import UpdateByFilm
from workers.update_by_genre import UpdateByGenre
from workers.update_by_person import UpdateByPerson


def _start_update_by_person(pg_conn: _connection, redis: Redis):
    """Запуск процесса обновления данных по персоналиям"""
    update_by_person = UpdateByPerson(pg_conn, redis)
    update_by_person.logger.info('Запускаем обновление по персоналиям')
    while update_by_person.needs_to_update:
        if selected_films := update_by_person.extract_persons_data():
            film_data = update_by_person.extract_selected_films(selected_films)
            payload_data = update_by_person.prepare_data_to_load(film_data)
            update_by_person.load_data_to_elasticsearch(payload_data)
        else:
            update_by_person.logger.info('Завершено обновление по персоналиям')
    return update_by_person.pg_conn


def _start_update_by_genre(pg_conn: _connection, redis: Redis):
    """Запуск процесса обновления данных по жанрам"""
    update_by_genre = UpdateByGenre(pg_conn, redis)
    update_by_genre.logger.info('Запускаем обновление по жанрам')
    while update_by_genre.needs_to_update:
        update_by_genre.extract_and_load_genres_data()
    update_by_genre.logger.info('Завершено обновление по жанрам')
    return update_by_genre.pg_conn


def _start_update_by_film(pg_conn: _connection, redis: Redis):
    """Запуск процесса обновления данных по фильмам"""
    update_by_film = UpdateByFilm(pg_conn, redis)
    update_by_film.logger.info('Запускаем обновление по фильмам')
    while update_by_film.needs_to_update:
        if selected_films := update_by_film.extract_films_data():
            film_data = update_by_film.extract_selected_films(selected_films)
            payload_data = update_by_film.prepare_data_to_load(film_data)
            update_by_film.load_data_to_elasticsearch(payload_data)
        else:
            update_by_film.logger.info('Завершено обновление по фильмам')
    update_by_film.logger.info(
        f'Обновление завершено. Следующее обновление начнется через {settings.update_period} сек')
    update_by_film.already_updated_films.clear()


def main():
    redis = Redis(**settings.redis.model_dump())
    while True:
        with closing(psycopg2.connect(**settings.postgres.model_dump(), cursor_factory=DictCursor)) as pg_conn:
            pg_conn = _start_update_by_person(pg_conn, redis)
            pg_conn = _start_update_by_genre(pg_conn, redis)
            _start_update_by_film(pg_conn, redis)
        time.sleep(settings.update_period)


if __name__ == '__main__':
    main()
