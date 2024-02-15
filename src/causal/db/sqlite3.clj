(ns causal.db.sqlite3
  (:require [clojure.string :refer [split-lines]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen
             [db :as db]
             [control :as c]
             [util :as u]]
            [jepsen.control
             [util :as cu]]
            [jepsen.os.debian :as deb]
            [slingshot.slingshot :refer [try+]]))

(def install-dir
  "Directory to install into."
  "/root/jepsen-causal-consistency")

(def app-dir
  "Application directory."
  (str install-dir "/electricsql"))

(def database-file
  "SQLite3 database file."
  (str app-dir "/electric.db"))

(def database-files
  "A collection of all SQLite3 database files."
  [(str app-dir "/electric.db")
   (str app-dir "/electric.db-shm")
   (str app-dir "/electric.db-wal")])

(def pid-file (str app-dir "/client.pid"))

(def log-file-short "client.log")
(def log-file       (str app-dir "/" log-file-short))

(def app-ps-name "node")

(def electricsql-setup?
  "Is ElectricSQL setup?"
  (atom false))

(defn db
  "ElectricSQL SQLite database."
  []
  (reify db/DB
    (setup!
      [this test node]
      (info "Setting up SQLite3 client")

      ; `client` may use `psql` CLI
      (when-not (cu/file? "/etc/apt/sources.list.d/pgdg.list")
        ; add key, repository, update, and add package
        (c/su
         (deb/install [:wget :gpg])
         (c/exec :wget :--quiet :-O :- "https://www.postgresql.org/media/keys/ACCC4CF8.asc" :| :sudo :apt-key :add :-)
         (deb/add-repo! :pgdg "deb https://apt.postgresql.org/pub/repos/apt bookworm-pgdg main")
         (deb/update!)
         (deb/install [:postgresql-client])))

      ; one client sets up ElectricSQL
      (locking electricsql-setup?
        (when-not @electricsql-setup?
          (info "Setting up ElectricSQL gset table:")
          (c/exec :psql :-d "postgresql://postgres:postgres@electricsql:65432"
                  :-c "CREATE TABLE public.gset (id integer PRIMARY KEY, k integer, v integer);")
          (c/exec :psql :-d "postgresql://postgres:postgres@electricsql:65432"
                  :-c "ALTER TABLE public.gset ENABLE ELECTRIC;")
          (info
           (c/exec :psql :-d "postgresql://postgres:postgres@electricsql:65432"
                   :-c "TABLE public.gset;"))
          (swap! electricsql-setup? (fn [_] true))))

      ; `client` may use `sqlite3` CLI
      (c/su
       (deb/install [:sqlite3]))

      ; NodeJS
      (when-not (cu/file? "/etc/apt/sources.list.d/nodejs.list")
        (c/su
         (deb/install [:wget :gpg])
         ; add key, repository, and update
         (u/meh  ; key may already exist, if real failure, will fail in apt update
          (c/exec :wget :--quiet :-O :- "https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key"
                  :| :gpg :--dearmor
                  :| :apt-key :add :-))
         (deb/add-repo! :nodejs "deb https://deb.nodesource.com/node_20.x nodistro main")
         (deb/update!)))
      (c/su
       (deb/install [:nodejs]))

      ; jepsen-causal-consistency
      (c/su
       (deb/install [:git]))
      (if (cu/exists? (str install-dir "/.git"))
        (c/su
         (c/cd install-dir
               (c/exec :git :pull)))
        (c/su
         (c/exec :rm :-rf install-dir)
         (c/exec :mkdir :-p install-dir)
         (c/exec :git :clone "https://github.com/nurturenature/jepsen-causal-consistency.git" install-dir)))

      ; building client migrations via ElectricSQL can be fussy, retry
      (assert (u/timeout
               60000 false
               (u/retry
                2 (do (c/su
                       (c/cd app-dir
                             (info "Building SQLite3 client")
                             (c/exec :npm :install)
                             (c/exec :npm :run :build)))
                      true)))
              (str "Unable to build SQLite3 client on " node))

      (db/start! this test node))

    (teardown!
      [this test node]
      (info "Tearing down SQLite3")
      (db/kill! this test node)
      (c/su
       (c/exec :rm :-rf database-files)
       (c/exec :rm :-rf log-file)))

    ; ElectricSQL doesn't have `primaries`.
    ; db/Primary

    db/LogFiles
    (log-files
      [_db _test _node]
      {log-file log-file-short})

    db/Kill
    (start!
      [_this _test _node]
      (if (cu/daemon-running? pid-file)
        :already-running
        (do
          (c/su
           (c/exec :rm :-f pid-file log-file)
           (cu/start-daemon!
            {:chdir   app-dir
             :logfile log-file
             :pidfile pid-file}
            "/usr/bin/npm" :run :start))
          :started)))

    (kill!
      [_this _test _node]
      (c/su
       (cu/stop-daemon! pid-file)
       (cu/grepkill! app-ps-name))
      :killed)

    db/Pause
    (pause!
      [_this _test _node]
      (c/su
       (cu/grepkill! :stop app-ps-name))
      :paused)

    (resume!
      [_this _test _node]
      (c/su
       (cu/grepkill! :cont app-ps-name))
      :resumed)))
