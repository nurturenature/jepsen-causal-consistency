(ns causal.postgresql
  "Install and configure PostgreSQL for ElectricSQL on node `postgresql`."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen
             [control :as c]
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

(def host
  "Name of host machine for PostgreSQL."
  "postgresql")

(def electric-user
  "`electric` user."
  "electric")

(def electric-password
  "`electric` role password."
  "electric")

(def connection-url
  "PostgreSQL connection URI."
  (str "postgresql://" electric-user ":" electric-password "@" host "/electric"))

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

(defn teardown
  "Teardown PostgreSQL."
  [_opts]
  (info "Tearing down PostgreSQL")
  (c/on host
        (c/su  ; tests may have stopped/killed database
         (when (try+
                (c/exec :systemctl :restart service)
                true
                (catch [] _
                  false))
           (c/exec :su :- :postgres :-c
                   "dropdb --force --if-exists electric")
           (c/exec :su :- :postgres :-c
                   "dropuser --if-exists electric")
           (info "PostgreSQL databases: "
                 (c/exec :su :- :postgres :-c
                         "psql -U postgres -c '\\dd';"))
           (info "PostgreSQL roles: "
                 (c/exec :su :- :postgres :-c
                         "psql -U postgres -c '\\du';"))

           (c/exec :systemctl :stop service)))))

(defn delete
  "Delete PostgreSQL."
  [_opts]
  (info "Deleting PostgreSQL")
  (c/on host
        (c/su
         (u/meh
          (c/exec :systemctl :stop service))

         (u/meh
          (deb/uninstall! [package]))

         (c/exec :rm :-rf
                 "/var/lib/postgresql*"
                 "/var/log/postgresql*"
                 "/etc/postgresql*"))))

(defn setup
  "Sets up, installing if necessary, and starts PostgreSQL."
  [_opts]
  (info "Setting up PostgreSQL")
  (c/on host
        (c/su
         (when (not (deb/installed? package))
           ; dependencies
           (deb/install [:lsb-release :wget :gpg])

           ; add key, repository, and package
           (insure-repo)
           (deb/install [package])

           ; start service
           (c/exec :systemctl :start  service)

           ; enable logical replication
           (c/exec :su :- :postgres :-c
                   "psql -U postgres -c 'ALTER SYSTEM SET wal_level = logical';")
           (c/exec :systemctl :restart service)
           (info
            (c/exec :su :- :postgres :-c
                    "psql -U postgres -c 'show wal_level';"))

           ; configure access
           (c/exec :echo "listen_addresses = '*'"
                   :>> "/etc/postgresql/16/main/postgresql.conf")
           (c/exec :echo "host all electric all scram-sha-256"
                   :>> "/etc/postgresql/16/main/pg_hba.conf")
           (c/exec :systemctl :restart service))

         (c/exec :systemctl :restart service)

         ; create electric role, database owned by electric
         (c/exec :su :- :postgres :-c
                 "psql -U postgres -c \"CREATE ROLE electric WITH LOGIN PASSWORD 'electric' SUPERUSER;\"")
         (c/exec :su :- :postgres :-c
                 "createdb -O electric electric")
         (info "PostgreSQL databases: "
               (c/exec :su :- :postgres :-c
                       "psql -U postgres -c '\\dd';"))
         (info "PostgreSQL roles: "
               (c/exec :su :- :postgres :-c
                       "psql -U postgres -c '\\du';")))))

(def opt-spec
  "Specifies CLI options."
  [[nil "--nodes NODE_LIST" (str "Must be " host)
    :default [host]]
   [nil "--teardown" "If set, tears down PostgreSQL."]
   [nil "--delete"   "If set, deletes PostgreSQL."]
   [nil "--setup"    "If set, sets up, installing if necessary, and starts PostgreSQL."]])

(defn opt-fn
  "Transforms CLI options before execution."
  [parsed]
  (let [nodes (u/coll (get-in parsed [:options :nodes]))]
    (assert (= nodes [host]) (str ":nodes must be [" host "], not " nodes)))
  parsed)

(defn run-fn
  "Body of command."
  [{:keys [options]}]
  (when (:teardown options)
    (teardown options))

  (when (:delete options)
    (delete options))

  (when (:setup options)
    (setup options)))

(def command
  "Jepsen CLI command spec."
  {"postgresql"
   {:opt-spec opt-spec
    :opt-fn opt-fn
    :usage (str "Setup, teardown, or delete PostgreSQL on " host ".")
    :run run-fn}})