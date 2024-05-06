#!/bin/bash
set -e

docker exec -t \
       -w /jepsen/jepsen-causal-consistency/electricsql \
       -e ELECTRIC_SERVICE_HOST=electric \
       -e ELECTRIC_DATABASE_HOST=postgres \
       -e ELECTRIC_DATABASE_NAME=electric-sqlite3-client \
       jepsen-n1 \
       bash -c 'npm run db:migrate'

echo
echo "The database now has electrified tables ready to test."
echo "A test can be run with ./electricsql-intermediate-read.sh"
