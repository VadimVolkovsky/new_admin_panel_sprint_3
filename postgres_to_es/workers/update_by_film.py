from typing import Any

import psycopg2

from decorators.decorators import backoff, db_reconnect
from .proto_updater import ProtoUpdater


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
        with self.pg_conn.cursor() as cur:
            films_last_update = self.redis.get('films_last_update').decode('utf-8')
            cur.execute(
                f"SELECT id, updated_at "
                f"FROM content.film_work "
                f"WHERE updated_at > '{films_last_update}' "
                f"ORDER BY updated_at "
                f"LIMIT {self.limit_rows} "
            )
            films = cur.fetchall()
        selected_films = []
        for film in films:
            selected_films.append(film[0])
        if not selected_films:
            self.needs_to_update = False
            return None
        selected_films = tuple(selected_films)
        self.redis.set('films_last_update', str(films[-1]['updated_at']))
        return selected_films
