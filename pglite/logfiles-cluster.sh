#! /usr/bin/bash
set -e

docker logs electric &> ../store/current/electricsql.log

docker logs postgres &> ../store/current/postgresql.log
