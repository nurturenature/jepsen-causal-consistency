## Running the Tests

Use electricsql-sql package commands as much as possible.

```bash
# bring up backend
# in project/electric-sqlite

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
docker logs electric > ./store/current/electricsql.log

docker logs postgres > ./store/current/postgresql.log
```

```bash
# bring the backend down
# in project/electric-sqlite

npm run backend:down
```