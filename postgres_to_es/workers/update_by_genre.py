import psycopg2

from decorators.decorators import backoff, db_reconnect
from .proto_updater import ProtoUpdater


class UpdateByGenre(ProtoUpdater):
    """Класс для проверки и обновления данных по жанрам"""
    needs_to_update: bool = True
    limit_obj_per_fetch = 100

    @backoff(exceptions=(psycopg2.InterfaceError, psycopg2.OperationalError))
    @db_reconnect()
    def extract_and_load_genres_data(self) -> None:
        """
        Метод извлекает из БД все объекты жанров, у которых изменились данные с момента последнего обновления.
        Т.к. изменение жанров всегда касается большого количества фильмов, важно извлекать объекты из БД частями,
        и сразу же подгружать обновленные данные в ElasticSearch.
        """
        with self.pg_conn.cursor() as cur:
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
