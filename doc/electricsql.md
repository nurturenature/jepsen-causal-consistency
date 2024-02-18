### Testing ElectricSQL

#### Grow Only Set

```sql
CREATE TABLE gset (id integer PRIMARY KEY, k integer, v integer);
ALTER TABLE gset ENABLE ELECTRIC;
```

Random transactions are generated:

```clj
[[:w 6 1] [:r 9 nil] [:w 7 1]]
```

And executed as SQL transactions on random nodes:

```sql
BEGIN;
  -- [:r k v]
  SELECT k,v FROM gset WHERE k = ?;
  -- [:w k v]
  INSERT INTO gset (id,k,v) VALUES(?, ?, ?);
END;
```

All writes are unique.

----

### Clients

Clients are total sticky available, always interact with:
  - same node
  - same database connection

Heterogeneous:
  - SQLite3
    - ElectricSQL TypeScript 
    - better-sqlite3 TypeScript
    - SQLite3 cli
  - PostgreSQL jdbc driver

Simple/Transparent
  - always rollback on error
  - no retries

### Servers

PostgreSQL
```sql
ALTER SYSTEM SET wal_level = logical;
```

ElectricSQL
```bash
ELECTRIC_WRITE_TO_PG_MODE=direct_writes
```

SQLite3
```ts
conn.pragma('journal_mode = WAL')
```

----

### Strong Convergence

Workload:
- generate a random mixture of reads and writes across all clients
- let database quiesce
- each client does a final read of all keys

Check:
  - all nodes have ok final reads
  - final reads contain all ok writes
  - no unexpected read values

----

### Fault Injection

Jepsen faults are real faults:

  - kill (-9) the ElectricSQL satellite sync service on each node
    - clients continue to read/write to the database
    - sync service restarted

----

### ***Preliminary*** Testing of Normal Operation

ElectricSQL is active/active for PostgreSQL and 1 to many SQLite3s.

#### Limitations Found

ElectricSQL TypeScript API only supports homogeneous transactions, i.e. all reads or all writes.
  - tests using this client only generate homogeneous transactions vs mixed transactions

Direct writes to PostgreSQL can deadlock the replication service's write transactions losing previously ok'd client writes ([issue](https://github.com/electric-sql/electric/issues/919)).
  - tests use a grow only set with unique writes

#### Capacity

Homogeneous transactions: 100+ tps for <= 300 s
Mixed transactions: < ~50 tps
  - valid strong convergence
    ```bash
    # can use ElectricSQL client
    lein run test --workload homogeneous --nodes n1,n2,n3,n4,n5,n6,n7,n8 --postgresql-nodes n1,n2 --electricsql-nodes n3,n4 --better-sqlite3-nodes n5,n6 --sqlite3-cli-nodes n7,n8
    ```

Homogeneous transactions: 100+ tps for > 300 s
Mixed transactions: > ~50 tps
  - invalid strong convergence, each node missing writes
  - errors in ElectricSQL sync server logs:
    ```log
    [error] GenStage consumer #PID<0.3354.0> received $gen_producer message: {:"$gen_producer", {#PID<0.3354.0>, #Reference<0.2284292455.4084727814.83365>},
      {:ask, 500}}
    ```
    ```bash
    # cannot use ElectricSQL client
    lein run test --nodes n1,n2,n3,n4,n5,n6,n7,n8 --postgresql-nodes n1,n2 --better-sqlite3-nodes n3,n4,n5,n6 --sqlite3-cli-nodes n7,n8
    ```
----

### ***Preliminary*** Testing of Fairness

Check the rate at which each node's writes are being read across the cluster.
E.g. are my document edits, puzzle solving moves, inventory control edits, etc. being fairly represented in reads across the cluster.

5 SQLite3 clients do transactions with a random mix of reads/writes against random keys, write values are sequential per key:
```clj
5	:ok	:txn	[[:r 29 8] [:w 7 13]]
2	:ok	:txn	[[:w 3 10] [:r 83 11]]
3	:ok	:txn	[[:w 56 7] [:r 53 10]]
4	:ok	:txn	[[:w 88 13] [:w 10 13]]
...
```

Always check for strong convergence at the end of the test, final reads available and == on each node:
```clj
:strong-convergence {:valid? true,
                     :final-read {0 92,
                                  1 89,
                                  2 99,
                                  ...
                                  97 108,
                                  98 87,
                                  99 90}}
```

Total each node's writes that were read:
```clj
:fairness {:valid? true,
           :reads-of-writes {"n1" 1317,
                             "n2" 1289,
                             "n3" 1289,
                             "n4" 1320,
                             "n5" 1284}}
```

And for every read, plot which node wrote the value:

![Fairness](fairness.png)

It's a gross measurement, and you can see the random ebb and flow, but it shows relative fairness of each node's writes being read.

```bash
lein run test --nodes postgresql,electricsql,n1,n2,n3,n4,n5 --noop-nodes postgresql,electricsql --workload lww-register-strong --time-limit 200 --key-dist uniform --key-count 100 --max-writes-per-key 1000 --min-txn-length 2 --max-txn-length 2 --rate 50
```

----

### ***Preliminary*** Testing of Strong Convergence With Kills

- 10 SQLite3 client nodes
- ~50 tps

```clj
;; ~5s kill the Electric sync service on a random third of the nodes
:nemesis	:info	:kill	["n1" "n4" "n6"]
:nemesis	:info	:kill	{"n1" :killed, "n4" :killed, "n6" :killed}

;; keep doing local transactions even with no sync service
7	:ok	:txn	[[:w 9 12] [:r 8 6] [:r 9 12]]
9	:ok	:txn	[[:w 9 13] [:r 9 13]]
10	:ok	:txn	[[:w 8 15] [:r 9 6] [:r 8 15] [:w 4 1]]
11	:ok	:txn	[[:r 9 7] [:w 8 16]]
3	:ok	:txn	[[:r 9 10] [:w 9 17] [:r 6 nil]]
4	:ok	:txn	[[:w 8 19] [:w 9 18] [:w 6 2] [:w 6 3]]
6	:ok	:txn	[[:r 4 nil] [:r 9 nil]]

;; ~5s restart sync service on nodes forcing it to catch-up with local and remote writes,
;; and hopefully forcing it to deal with timing/recovery of sync in progress kills 😈 
:nemesis	:info	:start	:all
:nemesis	:info	:start	{"n1" :started, "n2" :already-running, "n3" :already-running, "n4" :started, ...}
```

![Strong Convergence with Kills](strong-convergence-kill-latency.png)

```clj
;; strong convergence
{:valid? true,
 :final-read {0 6,
              1 12,
              2 23,
              3 56,
              4 91,
              5 171,
              6 254,
              7 247,
              8 251,
              9 256,
              ...}}
```
