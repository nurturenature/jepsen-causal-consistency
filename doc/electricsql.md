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

### Limitations In Testing Normal Operation

ElectricSQL is active/active for PostgreSQL and 1 to many heterogenous SQLite3 clients.

We would like to test heterogenous clients doing transactions with a mixture of reads and writes.

#### ElectricSQL TypeScript API
  - only supports homogeneous transactions, i.e. all reads or all writes vs mixed read/write transactions
  - only supports multiple record create or update, not upsert

So tests that use an ElectricSQL TypeScript API client
  - must use homogeneous transactions
  - can only have a single write in the transaction when upsert'ing

----

### Testing Active/Active (PostgreSQL/SQLite3) Sync with No Introduced Faults

#### Workload:
  - LWW Register
  - 2 PostgreSQL jdbc clients
  - 2 better-sqlite3 TypeScript clients
  - transactions a random mix of reads/writes

#### Deadlock in replication service writes leads to ok'd SQLite3 client writes not being stored in PostgreSQL or replicated to other SQLite3 clients

On node n3, the SQLite3 client does an ok write of [5 121]:
```clj
{:index 3637, :type :ok, :f :txn, :value [[:r 6 224] [:r 20 3] [:w 5 121] [:w 18 171]], :node "n3"}
```

The ElectricSQL replication satellite on node n3 replicates [5 121]:
```log
[proto] send: #SatOpLog{ops: ... new: ["5", "121"], old: data: ["5", "111"] ...}
...
```

ElectricSQL replication PL/pgSQL function for the electrified PostgreSQL table fails with a deadlock:
```log
ERROR:  deadlock detected
DETAIL:  Process 182 waits for ShareLock on transaction 2174; blocked by process 225.
	Process 225 waits for ShareLock on transaction 2172; blocked by process 182.
	Process 182: INSERT INTO "public"."lww_register"("k","v") VALUES (5,121)
	Process 225: INSERT INTO lww_register (k,v) VALUES (20,256) ON CONFLICT(k) DO UPDATE SET v = 256
...
	PL/pgSQL function electric.reorder_main_op___public__lww_register() line 12 at SQL statement
STATEMENT:  INSERT INTO "public"."lww_register"("k","v") VALUES (5,121)
```

PostgreSQL deadlock causes error in ElectricSQL sync service:
```log
[error] GenServer #PID<0.2820.0> terminating
** (RuntimeError) Postgres.Writer failed to execute statement INSERT INTO "public"."lww_register"("k","v") VALUES (5,121) with error {:error, {:error, :error, "40P01", :deadlock_detected, "deadlock detected"...}}
  (electric 0.9.0) lib/electric/replication/postgres/writer.ex:93: anonymous fn/2 in Electric.Replication.Postgres.Writer.send_transaction/3
...
```
Write appears to not be retried and are not observed by any PostgreSQL clients or the other SQLite3 client. 

This is straightforward to reproduce.

Conclusion, cannot active/active PostgreSQL/SQLite3 with transactions that update/upsert 
([issue](https://github.com/electric-sql/electric/issues/919)).

Test command:
```bash
lein run test --workload lww-register --nodes n1,n2,n3,n4 --postgresql-nodes n1,n2 --better-sqlite3-nodes n3,n4 --min-txn-length 4 --max-txn-length 4
```

----

### ***Preliminary*** Testing of Normal Operation

#### Workload:
  - 10 tps
  - each transaction is a single unique write operation
  - 10 ElectricSQL TypeScript client nodes
  - for 600s

#### Invalid Strong Convergence

n1:
  - reads everyone else's writes
  - its writes only partially read by everyone else
```clj
:strong-convergence
{:valid? false,
 :expected-read-count 5948,
 :incomplete-final-reads {"n2" {:missing-count 50,
                                :missing {1 {64 "n1"},
                                          2 {57 "n1"},
                                          3 {65 "n1"},
                                          ...}},
                          "n3" {:missing-count 50,
                                :missing {1 {64 "n1"},
                                          2 {57 "n1"},
                                          3 {65 "n1"},
                                          ...}},
                           "n10" {:missing-count 50,
                                 :missing {1 {64 "n1"},
                                           2 {57 "n1"},
                                           3 {65 "n1"},
                                           ...}}}}
```

Even though n1 dutifully sent out replication messages for its writes:
```log
[proto] send: #SatOpLog{ops: [#Begin{lsn: AAAD8A==, ts: 1708385205563, isMigration: false}, #Insert{for: 0, tags: [], new: ["10064", "1", "64"]}, #Commit{lsn: }]}
notify changes
actually changed notifier
```

ElectricSQL sync service logs:
```log
[error] GenStage consumer #PID<0.3290.0> received $gen_producer message: {:"$gen_producer", {#PID<0.3290.0>, #Reference<0.2906289151.3705143304.108196>},
  {:ask, 500}}
...
# 10+ occurrences
```

Test command:
```bash
lein run test --workload gset-single-writes --nodes n1,n2,n3,n4,n5,n6,n7,n8,n9,n10 --electricsql-nodes n1,n2,n3,n4,n5,n6,n7,n8,n9,n10 --rate 10 --time-limit 600
```

#### Capacity issues at rates of > ~10 writes/s and/or multi-minute test times
Invalid strong convergence, all writes were not replicated to every node
  - errors in ElectricSQL sync server logs:
    ```log
    [error] GenServer {:n, :l, {Electric.Postgres.CachedWal.Producer, "18b6fff8-16b2-404a-82ec-933ec8190c00"}} terminating
    
    # w/higher tps
    [error] GenStage consumer #PID<0.3354.0> received $gen_producer message: {:"$gen_producer", ...,
      {:ask, 500}}
    ...
    ```
  - errors in ElectricSQL SQLite3 satellite service logs:
    ```log
    # w/higher tps
    SatelliteError: sending a transaction while outbound replication has not started 
    ...
    [proto] recv: #SatErrorResp{type: INTERNAL} an error occurred in satellite: server error 
    Connectivity state changed: disconnected
    ```

so tests are run at ~ 25tps (mix of read or write txns) for 1-2 minutes.

##### Default test invocation:
```bash
lein run test --workload gset-homogeneous --nodes n1,n2,n3,n4,n5,n6,n7,n8,n9,n10 --postgresql-nodes n1 --electricsql-nodes n2,n3,n4 --better-sqlite3-nodes n5,n6,n7 --sqlite3-cli-nodes n8,n9,n10 --rate 25 --time-limit 100
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
lein run test --workload gset-homogeneous --nodes n1,n2,n3,n4,n5 --electricsql-nodes n1,n2,n3,n4,n5 --rate 25 --nemesis kill
```
