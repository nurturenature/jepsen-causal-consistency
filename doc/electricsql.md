## Testing ElectricSQL, Preliminary Results

### Different APIs, Different Experiences?

Interacting with electrified tables using SQL statements:

- `better-sqlite3.transaction` or `PGlite.exec`
  - loss of isolation
  - non-atomic transactions with intermediate reads
  - failure to read your writes
  - non-monotonic reads, version cycling
  - appear to be lost writes
  - loses convergence at lower rates and durations

Interacting with electrified tables using the generated API:

- `electric.client.update,updateMany,findMany`
  - non-monotonic reads, version cycling
  - maybe a hint of lost writes
  - keeps convergence at higher rates and durations

#### Or are the Generated API Constraints

- single value update vs concat
- single upsert in a transaction
- can't mix generic writes and reads in a transaction

reducing the test's ability to create and analyze a transaction history that exposes deeper anomalies?

----

### Active / Active

PostgreSQL transaction isolation:

- need repeatable read to get causal+ experience
- a bit more than needed
- but default read committed is not enough

Concurrent transactions:
- PostgreSQL: transactions with repeatable read isolation
- SQLite3: generated ElectricSQL client API

behave the same as doing SQLite3 transactions only.

----

### Testing the Tests

The same tests are run with a non-electrified SQLite3 and a non-electrified PostgreSQL:

- run at rates and durations an order of magnitude greater than the ElectricSQL tests
- no anomalies
