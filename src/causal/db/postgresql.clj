(ns causal.db.postgresql
  "Install and configure PostgreSQL for ElectricSQL on node `postgresql`."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen
             [control :as c]
             [db :as db]
             [util :as u]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as deb]
            [slingshot.slingshot :refer [try+]]))

(def package
  "Package name in repository."
  :postgresql)

(def service
  "systemd service name."
  "postgresql")

(def ps-name
  "Process name."
  "postgres")

(def host
  "Name of host machine for PostgreSQL."
  "postgresql")

(def connection-url
  "PostgreSQL connection URI."
  (str "postgresql://postgres:postgres@" host))

(defn insure-repo
  "Insures PostgreSQL repository is installed.
   Assumes `c/on c/su`."
  []
  (when-not (cu/file? "/etc/apt/sources.list.d/pgdg.list")
    ; add key, repository, and update
    (u/meh  ; key may already exist, if real failure, will fail in apt update
     (c/exec :wget :--quiet :-O :- "https://www.postgresql.org/media/keys/ACCC4CF8.asc" :| :sudo :apt-key :add :-))
    (deb/add-repo! :pgdg "deb https://apt.postgresql.org/pub/repos/apt bookworm-pgdg main")
    (deb/update!)))

(def available?
  "A promise that's true when PostgreSQL is available."
  (promise))

(defn db
  "PostgreSQL database."
  []
  (reify db/DB
    (setup!
      [_db _test node]
      (info "Setting up PostgreSQL")
      (c/su
       (when (not (deb/installed? package))
         ; dependencies
         (deb/install [:lsb-release :wget :gpg])

         ; add key, repository, and package
         (insure-repo)
         (deb/install [package])

         ; start service
         (c/exec :systemctl :start  service)
         (c/exec :pg_isready :--quiet)

         ; enable logical replication
         (c/exec :su :- :postgres :-c
                 "psql -U postgres -c 'ALTER SYSTEM SET wal_level = logical;'")
         (c/exec :systemctl :restart service)
         (info
          (c/exec :su :- :postgres :-c
                  "psql -U postgres -c 'show wal_level;'"))

         ; configure access
         (c/exec :echo "listen_addresses = '*'"
                 :>> "/etc/postgresql/16/main/postgresql.conf")
         (c/exec :echo "host all postgres all scram-sha-256"
                 :>> "/etc/postgresql/16/main/pg_hba.conf")
         (c/exec :systemctl :restart service))

       (c/exec :systemctl :restart service)
       (c/exec :pg_isready :--quiet)

       ; set password for use by electricsql
       (c/exec :su :- :postgres :-c
               "psql -U postgres -c \"ALTER USER postgres WITH PASSWORD 'postgres';\"")
       (info "PostgreSQL databases: "
             (c/exec :su :- :postgres :-c
                     "psql -U postgres -c '\\dd';")))

      (c/exec :pg_isready :--quiet)
      (deliver available? true))

    (teardown!
      [this test node]
      (info "Tearing down PostgreSQL")
      (u/meh  ; tests may have already stopped/killed database
       (c/su
        (c/exec :systemctl :stop service)))
      (db/kill! this test node))

    ;; ElectricSQL doesn't have `primaries`.
    ;; db/Primary

    ;; TODO: rotate and capture log files
    db/LogFiles
    (log-files
      [_db _test _node]
      {})

    db/Kill
    (start!
      [_this _test _node]
      (try+
       (c/su
        (c/exec :systemctl :is-active :--quiet service))
       :already-running
       (catch [] _
         (c/su
          (c/exec :systemctl :start service))
         :started)))

    (kill!
      [_this _test _node]
      (c/su
       (u/meh
        (c/exec :systemctl :stop service))
       (cu/grepkill! ps-name))
      :killed)

    db/Pause
    (pause!
      [_this _test _node]
      (c/su
       (cu/grepkill! :stop ps-name))
      :paused)

    (resume!
      [_this _test _node]
      (c/su
       (cu/grepkill! :cont ps-name))
      :resumed)))
