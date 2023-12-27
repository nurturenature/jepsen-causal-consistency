(ns causal.postgresql
  "Install and configure PostgreSQL for ElectricSQL on node `postgresql`."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen
             [control :as c]
             [util :as u]]
            [jepsen.os.debian :as deb]))

(def package
  "Package name in repository."
  :postgresql)

(def service
  "systemd service name."
  "postgresql@16-main")

(def host
  "Name of host machine for PostgreSQL."
  "postgresql")

(def electric-user
  "`electric` user."
  "electric")

(def electric-password
  "`electric` role password."
  "electric")

(def database-url
  "PostgreSQL connection URI."
  (str "postgresql://" electric-user ":" electric-password "@" host "/electric"))

(def opt-spec
  "Specifies CLI options."
  [[nil "--nodes NODE_LIST" (str "Must be " host)
    :default [host]]])

(defn opt-fn
  "Transforms CLI options before execution."
  [parsed]
  (let [nodes (u/coll (get-in parsed [:options :nodes]))]
    (assert (= nodes [host]) (str ":nodes must be [" host "], not " nodes)))
  parsed)

(defn run-fn
  "Body of command."
  [_opts]
  (info "Installing PostgreSQL on " host)
  (c/on host
        (c/su
         ; dependencies
         (deb/install [:lsb-release :wget :gpg])

         ; stop and cleanup any existing
         (u/meh
          (c/exec :systemctl :stop service)
          (deb/uninstall! [package])
          (c/exec :rm :-rf
                  "/var/lib/postgresql*"
                  "/var/log/postgresql*"
                  "/etc/postgresql*"))

         ; add key, repository, and package
         (u/meh  ; key may already exist, if real failure, will fail in apt update
          (c/exec :wget :--quiet :-O :- "https://www.postgresql.org/media/keys/ACCC4CF8.asc" :| :sudo :apt-key :add :-))
         (deb/add-repo! :pgdg "deb https://apt.postgresql.org/pub/repos/apt bookworm-pgdg main")
         (deb/update!)
         (deb/install [package])

         ; start
         (c/exec :systemctl :restart service)

         ; enable logical replication
         (c/exec :su :- :postgres :-c
                 "psql -U postgres -c 'ALTER SYSTEM SET wal_level = logical'")
         (c/exec :systemctl :restart service)
         (info
          (c/exec :su :- :postgres :-c
                  "psql -U postgres -c 'show wal_level'"))

         ; create electric role
         (c/exec :su :- :postgres :-c
                 "psql -U postgres -c \"CREATE ROLE electric WITH LOGIN PASSWORD 'electric' SUPERUSER;\""))
        (info "Installed")))

(def command
  {"postgresql"
   {:opt-spec opt-spec
    :opt-fn opt-fn
    :usage (str "Installs PostgreSQL on " host ".")
    :run run-fn}})