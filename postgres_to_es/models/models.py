import datetime
from dataclasses import dataclass
from uuid import UUID

import pydantic
from pydantic import BaseModel


@dataclass
class Film:
    """Модель фильма с необходимыми полями для загрузки в ElasticSearch"""
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


class Redis(BaseModel):
    """Настройки Redis"""
    host: str
    port: int


class Postgres(BaseModel):
    """Настройки PostgreSQL"""
    dbname: str
    user: str
    password: str
    host: str
    port: int


class ElasticSearch(BaseModel):
    """Настройки ElasticSearch"""
    host: str
    port: int
