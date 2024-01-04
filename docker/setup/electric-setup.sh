#!/bin/bash

# commands may fail if table already exists
psql -d postgresql://postgres:proxy_password@electric:65432 -c 'CREATE TABLE public.lww_registers (k integer PRIMARY KEY, v integer);'
psql -d postgresql://postgres:proxy_password@electric:65432 -c 'ALTER TABLE public.lww_registers ENABLE ELECTRIC;'

# insure table is empty, may haven been pre-existing
psql -d postgresql://postgres:proxy_password@electric:65432 -c 'DELETE FROM public.lww_registers;'
