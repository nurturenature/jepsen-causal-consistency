# Max Write Wins Data Model

Database is a Max Write Wins key/value register of integer/integer:

```SQL
CREATE TABLE IF NOT EXISTS public.mww (
    k INTEGER NOT NULL,
    v INTEGER NOT NULL,
    CONSTRAINT mww_pkey PRIMARY KEY (k)
);
```

On writes the maximum, transaction or row, value wins.
Example PostgreSQL update:

```SQL
-- txn-k and txn-v are key and value from txn
UPDATE mww SET v = GREATEST(txn-v, mww.v) WHERE k = txn-k RETURNING *;
```

----

Values written in a transaction are monotonic per key.

Values read in a transaction are monotonic per key.

Transactions are atomic.

Each client will read values in Causally Consistent way.
