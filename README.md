# Заключительное задание первого модуля

Ваша задача в этом уроке — загрузить данные в Elasticsearch из PostgreSQL. Подробности задания в папке `etl`.


Инструкция по развертыванию

Выполните в консоли следующие команды:


Создайте файл окружения с тестовыми данными:
```
touch .env & echo "POSTGRES_DB=movies_database
POSTGRES_USER=app
POSTGRES_PASSWORD=123qwe
DB_HOST=sprint_3_postgres
REDIS_HOST=sprint_3_redis
REDIS_PORT=6379
ELASTIC_HOST=http://sprint_3_elastic_search
ELASTIC_PORT=9200" > .env2
```

Запустите развертывание приложения:
```
bash postgres_to_es/deploy.sh
```