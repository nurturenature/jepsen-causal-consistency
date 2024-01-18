(ns causal.db.promises
  "A namespace to hold orchestration `setup!`/`teardown!` promises.
   Avoids cyclic load dependencies between the database namespaces.")

(def postgresql-available?
  "A promise that's true when PostgreSQL is available."
  (promise))

(def electricsql-available?
  "A promise that's true when ElectricSQL is available."
  (promise))

(def electricsql-teardown?
  "A promise that's true when ElectricSQL teardown is complete."
  (promise))

(def sqlite3-teardown?s
  "A map of SQLite3 node names to a promise that is delivered true when node's teardown is complete."
  (atom {}))
