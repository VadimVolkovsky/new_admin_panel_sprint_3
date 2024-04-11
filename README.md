# Заключительное задание первого модуля

Ваша задача в этом уроке — загрузить данные в Elasticsearch из PostgreSQL. Подробности задания в папке `etl`.


Инструкция по развертыванию

Выполните в консоли следующие команды:


Создайте файл окружения с тестовыми данными:
```
touch .env & echo "POSTGRES__DBNAME=movies_database
POSTGRES__USER=app
POSTGRES__PASSWORD=123qwe
POSTGRES__HOST=sprint_3_postgres
POSTGRES__PORT=5432
REDIS__HOST=sprint_3_redis
REDIS__PORT=6379
ELASTICSEARCH__HOST=http://sprint_3_elastic_search
ELASTICSEARCH__PORT=9200" > .env2
```

Запустите развертывание приложения:
```
bash postgres_to_es/deploy.sh
```
