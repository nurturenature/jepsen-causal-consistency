#!/bin/bash
set -e

docker compose \
       -f electricsql-compose.yaml \
       -f jepsen-compose.yaml \
       -f jepsen-electricsql-compose.yaml \
       down

docker volume rm jepsen_pg_data || echo "jepsen_pg_data already removed"
docker volume rm jepsen_jepsen-shared || echo "jepsen_jepsen-shared already removed"
