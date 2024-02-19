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

Less of a fault, and more indicative of normal behavior.

In a local first environment, clients will be coming and going in all manner at all times.

----

### ***Preliminary*** Testing of Normal Operation

ElectricSQL is active/active for PostgreSQL and 1 to many heterogenous SQLite3 clients.

#### Limitations Found

##### ElectricSQL TypeScript API
  - only supports homogeneous transactions, i.e. all reads or all writes vs mixed read/write transactions
  - only supports multiple record create or update, not upsert

so tests default to using homogeneous transactions that either read or create many.

##### Direct writes to PostgreSQL can deadlock the replication service's write transactions losing previously ok'd client writes ([issue](https://github.com/electric-sql/electric/issues/919))
  - cannot active/active PostgreSQL/SQLite3 with transactions that update/upsert

so tests use a grow only set with unique writes.

##### At a rate of > ~50tps for 500s
Invalid strong convergence, all writes were not replicated to every node
  - errors in ElectricSQL sync server logs w/higher tps:
    ```log
    [error] GenStage consumer #PID<0.3354.0> received $gen_producer message: {:"$gen_producer", ...,
      {:ask, 500}}
    ...
    [error] GenServer {:n, :l, {Electric.Postgres.CachedWal.Producer, "18b6fff8-16b2-404a-82ec-933ec8190c00"}} terminating
    ```
  - errors in ElectricSQL SQLite3 satellite service logs w/higher tps:
    ```log
    SatelliteError: sending a transaction while outbound replication has not started 
    ...
    [proto] recv: #SatErrorResp{type: INTERNAL} an error occurred in satellite: server error 
    Connectivity state changed: disconnected
    ```

so tests are run at ~ 25tps.

##### Default test invocation:
```bash
lein run test --workload homogeneous --nodes n1,n2,n3,n4,n5,n6,n7,n8 --postgresql-nodes n1,n2 --electricsql-nodes n3,n4 --better-sqlite3-nodes n5,n6 --sqlite3-cli-nodes n7,n8 --rate 25 --time-limit 500
```

----

### ***Preliminary*** Testing of Client Kills

Workload
  - ~25 tps
  - homogeneous transactions
  - 5 client nodes, all ElectricSQL TypeScript clients

```clj
;; ~5s kill client sync service on 2 random nodes
:nemesis	:info	:kill	["n2" "n5"]
:nemesis	:info	:kill	{"n2" :killed, "n5" :killed}

;; ~5s restart client sync service
;; client resyncs into an active cluster, resumes local transactions
;; exposes sync service to timing/recovery of sync in progress interruptions 😈 
:nemesis	:info	:start	:all
:nemesis	:info	:start	{"n1" :already-running, "n2" :started, "n3" :already-running, "n4" :already-running, "n5" :started}
```

#### Does not strongly converge.

```clj
;; node n5 never reads 2 writes from node n1
:strong-convergence {:valid? false,
                     :expected-read-count 1624,
                     :incomplete-final-reads {"n5" {:count 2,
                                                    :missing {43 {1 "n1"},
                                                              44 {2 "n1"}}}}}
```

The logs show that n1 wrote the values while n5 was offline and n5 did not resync correctly:
```clj
:nemesis :info :kill  {"n2" :killed, "n5" :killed}
...
"n1"     :ok   :w-txn [[:w 44 2] [:w 43 1]]
...
:nemesis :info :start {"n1" :already-running, "n2" :started, "n3" :already-running, "n4" :already-running, "n5" :started}
```

Test command:
```bash
lein run test --workload homogeneous --nodes n1,n2,n3,n4,n5 --electricsql-nodes n1,n2,n3,n4,n5 --rate 25 --nemesis kill
```
