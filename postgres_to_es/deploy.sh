docker compose up -d sprint_3_postgres
echo "Waiting for PostgreSQL loading..."
sleep 10
docker cp schemas/database_schema.ddl sprint_3_postgres:/database_schema.ddl
docker exec -it sprint_3_postgres psql -h 127.0.0.1 -U app -d movies_database -f database_schema.ddl
#
docker compose up -d sprint_3_redis
echo "Waiting for Redis loading..."
sleep 10
echo "Setup default 'last_update' values in Redis..."
docker exec -it sprint_3_redis redis-cli set persons_last_update '2021-06-16 23:00:00.000 +0300'
docker exec -it sprint_3_redis redis-cli set genres_last_update '2021-06-16 23:00:00.000 +0300'
docker exec -it sprint_3_redis redis-cli set films_last_update '2021-06-16 23:00:00.000 +0300'

docker compose up -d sprint_3_elastic_search
echo "Waiting for ElasticSearch loading..."
sleep 20
bash schemas/es_schema.txt
echo "Waiting for ElasticSearch schema creation..."
sleep 10
docker compose up -d sprint_3_app