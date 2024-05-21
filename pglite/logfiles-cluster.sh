#! /usr/bin/bash
set -e

docker logs electric-pglite-client-electric-1 &> ../store/current/electricsql.log

docker logs electric-pglite-client-postgres-1 &> ../store/current/postgresql.log
