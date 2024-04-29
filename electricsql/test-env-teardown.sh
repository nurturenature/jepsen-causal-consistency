#! /usr/bin/bash
set -e

npm run backend:down

# rm containers
docker rm electric-sqlite3-client-electric-1
docker rm electric-sqlite3-client-postgres-1 

# rm data volume
docker volume rm electric-sqlite3-client_pg_data
