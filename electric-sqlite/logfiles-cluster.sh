#! /usr/bin/bash
set -e

docker logs electric-electric-1 &> ./store/current/electricsql.log

docker logs electric-postgres-1 &> ./store/current/postgresql.log
