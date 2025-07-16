### Max Write Wins Data Model

Database is a Max Write Wins key/value register of integer/integer:
```SQL
CREATE TABLE IF NOT EXISTS public.mww (
    id TEXT NOT NULL,
    k INTEGER NOT NULL UNIQUE,
    v INTEGER NOT NULL,
    CONSTRAINT mww_pkey PRIMARY KEY (id)
);
```

On writes the maximum, transaction or row, value wins.
Example PostgreSQL update:
```dart
// max write wins, so GREATEST() value of v
final v = crudEntry.opData!['v'] as int;
patch = await tx.execute(
  'UPDATE ${table.name} SET v = GREATEST($v, ${table.name}.v) WHERE id = \'${crudEntry.id}\' RETURNING *',
);
```

Values written in a transaction are monotonic per key.

Values read in a transaction are monotonic per key.

Transactions are atomic.

Each client will read values in Causally Consistent way.
