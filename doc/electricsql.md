### Testing ElectricSQL

#### LWW Register

```sql
CREATE TABLE lww_registers (k integer PRIMARY KEY, v integer);
ALTER TABLE lww_registers ENABLE ELECTRIC;
```

Random transactions are generated:

```clj
[[:w 6 1] [:r 9 nil] [:w 7 1]]
```

And executed as SQL transactions on random nodes:

```sql

BEGIN;
  -- [:r k v]
  SELECT k,v FROM lww_registers WHERE k = ?;
  -- [:w k v]
  INSERT INTO lww_registers(k,v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v = ?;
END;
```

----

#### Clients

Clients are sticky, always:
  - talks to same node
  - uses same connection

Heterogeneous:
  - SQLite3 CLI
  - PostgreSQL jdbc driver

----

### Strong Convergence

- generate a random mixture of reads and writes across all clients
- let database briefly quiesce
- each client does a final read of all keys in a single transaction from each node

Check:

  - all nodes have an ok read
    - total sticky availability
  - all nodes read the same value for all keys
    - strong convergence

----

### Fault Injection

Jepsen faults are real faults:

  - kill (-9) the ElectricSQL satellite sync service on each node
    - clients continue to read/write to the database
    - sync service restarted