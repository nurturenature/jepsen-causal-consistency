#! /usr/bin/bash
set -e

lxc delete electricsql --force || true
lxc delete postgresql  --force || true

lxc launch postgresql postgresql
sleep 3

lxc launch electricsql electricsql
sleep 3
