Simple CDC leveraging Debezium Engine to CDC.

This App catch in real time changes in a postgres and process it, without any middleware.

This is very useful for low latency data changes like > 1req/s.

## Motivation:
* KafkaConnect and Debezium are very heavy to maintain.
* Low-latency data migration from source to sink.

  ## Pros:
  * Simple JavaApp.
  * Very simple to setup.
    
## Limitation
This is just a POC to make the thing work, Threads management and errors are absent.


## Configure you Postgres
```bash
docker pull postgres:10.6

docker run -d -p 30028:5432 \
  --name postgres-10.6 \
  -e POSTGRES_PASSWORD=postgres \
  postgres:10.6 -c 'shared_preload_libraries=pgoutput'
```

```bash
docker exec -it postgres-10.6 bash

# Edit postgresql.conf
vi /var/lib/postgresql/data/postgresql.conf
# Set:
 wal_level = logical
 max_replication_slots = 20
 max_wal_senders = 20
 wal_sender_timeout = 180s

# Then:
docker restart postgres-10.6

# Verify
docker exec -it postgres-10.6 psql -U postgres -c "SHOW wal_level"
```

Create user and stream:
```bash
docker exec -it postgres-10.6 psql -U postgres

CREATE TABLE public.t_user (
  id BIGINT PRIMARY KEY,
  name VARCHAR(255),
  age SMALLINT
);

CREATE USER test1 WITH PASSWORD 'test123';
ALTER ROLE test1 REPLICATION;
GRANT CONNECT ON DATABASE test_db TO test1;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO test1;

ALTER TABLE t_user REPLICA IDENTITY FULL;
CREATE PUBLICATION dbz_publication FOR ALL TABLES;
```
