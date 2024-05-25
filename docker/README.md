## [Jepsen](https://github.com/jepsen-io/jepsen) Tests for [ElectricSQL](https://github.com/electric-sql/electric)

----

### ElectricSQL Docker Examples

```bash
# add ElectricSQL to published Jepsen images 
./electricsql-build.sh

# bring up by combining compose files
#   -f electricsql-compose.yaml
#   -f jepsen-compose.yaml
#   -f jepsen-electricsql-compose.yaml
./electricsql-up.sh

# run a test
# lots of output ending with the results map
# easier to view results on test webserver
#
# can only run one test per setup
./jepsen-docker-cli.sh lein run test --workload electric-sqlite --min-txn-length 2 --max-txn-length 4 --rate 10 --time-limit 30
./jepsen-docker-cli.sh lein run test --workload electric-sqlite-strong --nemesis offline-online --rate 10 --time-limit 100

# run a webserver for test results on jepsen-control
# available at http://localhost:8080
./electricsql-web.sh

# bring down and cleanup
#   - removes postgresql volume
#   - want to start each test with a known pristine database
./electricsql-down.sh
```

----

### ElectricSQL GitHub Action Examples

See https://github.com/nurturenature/jepsen-causal-consistency/actions
