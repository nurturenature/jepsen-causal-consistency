(ns causal.postgresql
  "Install and configure PostgreSQL for ElectricSQL on node `postgresql`."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen
             [control :as c]
             [util :as u]]))

(def version
  "Version of PostgreSQL to install."
  "16")

(def package
  "Package name in repository."
  (str "postgresql-" version))

(def service
  "systemd service name."
  (str "postgresql@" version "-main"))

(def host
  "Name of host machine for PostgreSQL."
  "postgresql")

(def electric-password
  "`electric` role password"
  "electric")

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
  [opts]
  (info (pr-str opts))
  (info "Installing PostgreSQL on " host)
  (c/on host
        (c/su
         ; dependencies
         (c/exec "DEBIAN_FRONTEND='noninteractive'"
                 :apt :-qy :install :lsb-release :curl :gpg)

         ; stop and cleanup any existing
         (u/meh (c/exec :systemctl :stop service)
                (c/exec "DEBIAN_FRONTEND='noninteractive'"
                        :apt :remove :-qy :--purge package)
                (c/exec :rm :-rf
                        "/etc/apt/sources.list.d/pgdg.list"
                        "/etc/apt/trusted.gpg.d/postgresql.gpg"
                        "/var/lib/postgresql"
                        "/var/log/postgresql"
                        "/etc/postgresql"))

         ; add repository, key, and package
         (c/exec :echo "deb http://apt.postgresql.org/pub/repos/apt bookworm-pgdg main"
                 :> "/etc/apt/sources.list.d/pgdg.list")
         (c/exec :curl :-fsSL "https://www.postgresql.org/media/keys/ACCC4CF8.asc"
                 :| :gpg :--batch :--dearmor :-o "/etc/apt/trusted.gpg.d/postgresql.gpg")
         (c/exec "DEBIAN_FRONTEND='noninteractive'"
                 :apt :update)
         (c/exec "DEBIAN_FRONTEND='noninteractive'"
                 :apt :-qy :install package)

         ; start
         (c/exec :systemctl :restart service)

         ; enable logical replication
         (c/exec :su :- :postgres :-c
                 "psql -U postgres -c 'ALTER SYSTEM SET wal_level = logical'")
         (c/exec :systemctl :restart service)
         (c/exec :su :- :postgres :-c
                 "psql -U postgres -c 'show wal_level'")

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