#! /usr/bin/bash
set -e

lxc delete postgresql --force || true
lxc launch postgresql postgresql

sleep 3

lxc delete electricsql --force || true
lxc launch electricsql electricsql

sleep 3

