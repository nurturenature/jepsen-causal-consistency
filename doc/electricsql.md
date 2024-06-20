## This project is on hold.  It is not up to date with the newer versions of ElectricSQL.  Work will resume in the Fall of 2024.

## Testing ElectricSQL, Preliminary Results

### Using Different APIs

#### Interacting with electrified tables using SQL statements:

- `better-sqlite3.transaction` or `PGlite.exec`
  - loss of isolation
  - non-atomic transactions with intermediate reads
  - failure to read your writes
  - non-monotonic reads, version cycling
  - appear to be lost writes
  - diverges from convergence at lower rates and durations

#### Interacting with electrified tables using the generated API:

- `electric.client.update,updateMany,findMany`
  - non-monotonic reads, version cycling
  - maybe a hint of lost writes
  - keeps convergence at higher rates and durations

#### Less Anomalies or are the Generated API Constraints:

- single value update vs concat
- single upsert in a transaction
- can't mix generic writes and reads in a transaction

reducing the test's ability to create and analyze a transaction history that exposes deeper anomalies?

##### Issues opened:

- [Non-atomic transactions with intermediate reads are common](https://github.com/electric-sql/electric/issues/1245)
- [Delayed/Failure to read your writes, lost writes are common](https://github.com/electric-sql/electric/issues/1254)

----

### Active / Active

PostgreSQL transaction isolation:

- need repeatable read to get causal+ experience
- a bit more than needed
- but default read committed is not enough

Concurrent transactions:
- PostgreSQL: SQL transactions with repeatable read isolation
- SQLite3: generated ElectricSQL client API

behaves the same as using the generated API with SQLite3.

##### Issues opened:
- [Concurrent PostgreSQL and SQLite3 clients can trigger a deadlock in electric.reorder_main_op pgSQL, Replication.Postgres.Writer restarts, writes being replicated appear to be lost](https://github.com/electric-sql/electric/issues/919)

----

### Last Write Wins

Last write does not always win using generated API under load:

<table>
  <thead>
      <tr>
          <th colspan="5">#{[84 206] [84 207]}</th>
      </tr>
      <tr>
          <th>Index</th>
          <th>Process</th>
          <th>Type</th>
          <th>Fn</th>
          <th>Mops</th>
      </tr>
  </thead>
  <tbody>
      <tr>
          <td>1657</td>
          <td>2</td>
          <td>ok</td>
          <td>updateMany</td>
          <td>[:append 84 206]</td>
      </tr>
      <tr>
          <td>1661</td>
          <td>3</td>
          <td>ok</td>
          <td>updateMany</td>
          <td>[:append 84 207]</td>
      </tr>
      <tr>
          <td>1675</td>
          <td>3</td>
          <td>ok</td>
          <td>update</td>
          <td>[:r 84 [207]]</td>
      </tr>
      <tr>
          <td>1685</td>
          <td>3</td>
          <td>ok</td>
          <td>update</td>
          <td>[:r 84 [206]]</td>
      </tr>
      <tr>
          <td>1698</td>
          <td>3</td>
          <td>ok</td>
          <td>update</td>
          <td>[:r 84 [206]]</td>
      </tr>
  </tbody>
  <tfoot>
      <tr>
          <th colspan="5">Sources: #{:G0-realtime}</th>
      </tr>
  </tfoot>
</table>

----

### Testing the Tests

The same tests are run with a non-electrified SQLite3 and a non-electrified PostgreSQL:

- run at rates and durations an order of magnitude greater than the ElectricSQL tests
- no anomalies
