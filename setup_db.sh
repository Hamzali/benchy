docker rm -f interview-db
docker network create interview

docker run -d --name interview-db \
 -p 5432:5432 --network interview \
 -e POSTGRES_PASSWORD=12345 \
 timescale/timescaledb:1.7.4-pg12

cat data/cpu_usage.sql | docker exec -i interview-db \
 psql -U postgres

cat data/cpu_usage.csv | docker exec -i interview-db \
 psql -U postgres -d homework \
 -c "\COPY cpu_usage FROM STDIN WITH DELIMITER AS ',' CSV HEADER"
