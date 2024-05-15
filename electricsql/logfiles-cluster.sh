#! /usr/bin/bash
set -e

docker logs electric-sqlite3-client-electric-1 &> ./store/current/electricsql.log

docker logs electric-sqlite3-client-postgres-1 &> ./store/current/postgresql.log
