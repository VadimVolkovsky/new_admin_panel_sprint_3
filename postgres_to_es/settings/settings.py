from pydantic_settings import BaseSettings

from models.models import Redis, Postgres, ElasticSearch


class Settings(BaseSettings):
    """Настройки окружения проекта"""
    redis: Redis
    postgres: Postgres
    elasticsearch: ElasticSearch
    update_period: int = 30

    class Config:
        env_nested_delimiter = '__'


settings = Settings(_env_file='.env', _env_file_encoding='utf-8')
