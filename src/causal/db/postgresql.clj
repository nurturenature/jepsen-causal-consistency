(ns causal.db.postgresql
  "Install and configure PostgreSQL for ElectricSQL on node `postgresql`."
  (:require [causal.db.promises :as promises]
            [clojure.tools.logging :refer [info]]
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

(def host     "postgresql")
(def user     "postgres")
(def password "postgres")

(def connection-url
  "PostgreSQL connection URI."
  (str "postgresql://" user ":" password "@" host))

(def log-file "/var/log/postgresql/postgresql-16-main.log")

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

;; TODO: doesn't work
;; (defn drop-table-db
;;   "Explicitly removes ElectricSQL changes, table, and database." []
;;   (u/meh
;;    (c/su
;;     ;; (c/exec :su :- :postgres :-c
;;     ;;         "psql -U postgres -c 'ALTER SUBSCRIPTION postgres_1 DISABLE;'")
;;     ;; (c/exec :su :- :postgres :-c
;;     ;;         "psql -U postgres -c 'ALTER SUBSCRIPTION postgres_1 SET (slot_name = NONE);'")
;;     ;; (c/exec :su :- :postgres :-c
;;     ;;         "psql -U postgres -c 'DROP SUBSCRIPTION postgres_1;'")
;;     ;; (c/exec :su :- :postgres :-c
;;     ;;         "psql -U postgres -c 'ALTER SUBSCRIPTION postgres_1 DISABLE;'")
;;     (c/exec :su :- :postgres :-c
;;             "psql -U postgres -c 'DROP PUBLICATION electric_publication;'")
;;     (c/exec :su :- :postgres :-c
;;             "psql -U postgres -c 'DROP SCHEMA electric CASCADE;'")
;;     ;; (c/exec :su :- :postgres :-c
;;     ;;         "psql -U postgres -c 'SELECT pg_drop_replication_slot ('electric_replication_out_test');'")
;;     (c/exec :su :- :postgres :-c
;;             "psql -U postgres -c 'DROP DATABASE electric;'"))))

(defn db
  "PostgreSQL database."
  []
  (reify db/DB
    (setup!
      [_db _test _node]
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

       (info "PostgreSQL tables:")
       (info (c/exec :su :- :postgres :-c
                     "psql -U postgres -c '\\dt';")))

      (deliver promises/postgresql-available? true))

    (teardown!
      [this test node]
      (assert (deref promises/electricsql-teardown? 60000 false)
              "ElectricSQL not teardown? true")

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
      {log-file "postgresql.log"})

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
