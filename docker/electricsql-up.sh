#!/bin/bash
set -e

export APP_NAME=electric-sqlite3-client
export JEPSEN_REGISTRY="ghcr.io/nurturenature/jepsen-docker/"

docker compose \
       -f electricsql-compose.yaml \
       -f jepsen-compose.yaml \
       -f jepsen-electricsql-compose.yaml \
       up \
       --detach \
       --wait

echo
echo "A full Jepsen + ElectricSQL cluster is up and available"
echo "Create test database tables and electrify them with ./electricsql-run-migrations.sh"
