#! /usr/bin/bash
set -e

export ELECTRIC_CONTAINER_NAME="electric"
export ELECTRIC_DATABASE_HOST="localhost"
export ELECTRIC_DATABASE_NAME="electric"
export ELECTRIC_SERVICE="http://localhost:5133"
export ELECTRIC_SERVICE_HOST="localhost"

npm install

npm run backend:up
