#! /usr/bin/bash
set -e

npm install

npm run backend:up

npm run db:migrate
