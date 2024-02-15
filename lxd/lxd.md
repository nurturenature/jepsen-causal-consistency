### Make Images

```bash
./make-image.sh postgresql
./make-image.sh electricsql  # takes a long time, erlang, elixir, electricsql from source
```

### Running Tests

```bash
./restore-cluster.sh   # must be run before each test
lein run test ...
```
