from typing import Any

import psycopg2

from decorators.decorators import backoff, db_reconnect
from .proto_updater import ProtoUpdater


class UpdateByPerson(ProtoUpdater):
    """Класс для проверки и обновления данных по персоналиям"""
    needs_to_update: bool = True
    limit_rows = 100

    @backoff(exceptions=(psycopg2.InterfaceError, psycopg2.OperationalError))
    @db_reconnect()
    def extract_persons_data(self) -> tuple[Any, ...] | None:
        """
        Извлекаем из БД всех персоналий, у которых изменились данные с момента последнего обновления
        """
        with self.pg_conn.cursor() as cur:
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
        selected_films = []
        for film in data:
            selected_films.append(film[0])
        if not selected_films:
            self.needs_to_update = False
            return None
        return tuple(selected_films)
