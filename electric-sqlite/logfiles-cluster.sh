#! /usr/bin/bash
set -e

docker logs electric-sqlite-client-electric-1 &> ./store/current/electricsql.log

docker logs electric-sqlite-client-postgres-1 &> ./store/current/postgresql.log
