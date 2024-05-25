## Running the Tests

Use electricsql-sql package commands as much as possible.

```bash
# bring up backend
# in project/electricsql

npm install

npm run backend:up
```

```bash
# run tests
# in project

lein run test ...
```

```bash
# copy logs from containers into Jepsen test store
docker logs electric-sqlite3-client-electric-1 > ./store/current/electricsql.log

docker logs electric-sqlite3-client-postgres-1 > ./store/current/postgresql.log
```

```bash
# bring the backend down
# in project/electricsql

npm run backend:down
```