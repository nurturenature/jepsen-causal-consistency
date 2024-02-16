#! /usr/bin/bash
set -e

lxc file pull postgresql/var/log/postgresql/postgresql-16-main.log ./store/current/postgresql.log

lxc exec electricsql -- journalctl -u electricsql.service > ./store/current/electricsql.log
