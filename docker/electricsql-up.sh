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

docker ps --format="table {{.Names}}\t{{.Image}}\t{{.Status}}"

echo
echo "A full Jepsen + ElectricSQL cluster is up and available"
echo "Run a Jepsen test with ./jepsen-docker-cli.sh lein run test"
