(ns causal.cluster
  "A `db` that represents the full cluster:
     - postgresql
     - electricsql
     - sqlite3 client nodes
   
   `setup!`/`teardown!` use `promise`s to orchestrate order.
   "
  (:require
   [causal.db
    [electricsql :as electricsql]
    [postgresql :as postgresql]
    [sqlite3 :as sqlite3]]
   [jepsen
    [db :as db]]))

(def dbs
  "A map of node names to `db` protocols."
  {"postgresql"  (postgresql/db)
   "electricsql" (electricsql/db)
   :default      (sqlite3/db)})

(defn node->db
  "Maps a node name to its `db` protocol, e.g.:
     - 'postgresql'  -> `db/postgresql`
     - 'electricsql` -> `db/electricsql`
     - client nodes  -> `db/sqlite3`"
  [node]
  (get dbs node (:default dbs)))

(defn db
  "Cluster representation of all databases in the test."
  []
  (reify db/DB
    (setup!
      [_db test node]
      (db/setup! (node->db node) test node))

    (teardown!
      [_db test node]
      (db/teardown! (node->db node) test node))

    ; ElectricSQL doesn't have `primaries`.
    ; db/Primary

    db/LogFiles
    (log-files
      [_db test node]
      (db/log-files (node->db node) test node))

    db/Kill
    (start!
      [_db test node]
      (db/start! (node->db node) test node))

    (kill!
      [_db test node]
      (db/kill! (node->db node) test node))

    db/Pause
    (pause!
      [_db test node]
      (db/pause! (node->db node) test node))

    (resume!
      [_db test node]
      (db/resume! (node->db node) test node))))
