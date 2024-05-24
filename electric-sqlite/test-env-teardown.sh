#! /usr/bin/bash
set -e

npm run backend:down

# rm containers
# docker rm electric-sqlite-client-electric-1
# docker rm electric-sqlite-client-postgres-1 

# rm data volume
# docker volume rm electric-sqlite-client_pg_data
