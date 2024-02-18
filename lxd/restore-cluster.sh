#! /usr/bin/bash
set -e

lxc delete electricsql --force || true
lxc delete postgresql  --force || true

lxc launch postgresql postgresql
sleep 1
lxc exec postgresql -- pg_isready -t 5

lxc launch electricsql electricsql
sleep 3
# curl --retry 3 http://electricsql:5133/api/status
