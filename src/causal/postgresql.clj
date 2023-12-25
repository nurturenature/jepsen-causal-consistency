(ns causal.postgresql
  "Install and configure PostgreSQL for ElectricSQL on node `postgress`."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen
             [control :as c]
             [util :as u]]
            [jepsen.util :as u]))

(def host
  "Name of host machine for PostgreSQL."
  "postgresql")

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
         ; dependancies
         (c/exec "DEBIAN_FRONTEND='noninteractive'"
                 :apt :-qy :install :lsb-release :curl :gpg)

         ; stop and cleanup any existing
         (u/meh (c/exec :systemctl :stop "postgresql@15-main")
                (c/exec :rm "/etc/apt/sources.list.d/pgdg.list"
                        "/etc/apt/trusted.gpg.d/postgresql.gpg"))

         ; add repository, key, and package
         (c/exec :echo "deb http://apt.postgresql.org/pub/repos/apt bookworm-pgdg main"
                 :> "/etc/apt/sources.list.d/pgdg.list")
         (c/exec :curl :-fsSL "https://www.postgresql.org/media/keys/ACCC4CF8.asc"
                 :| :gpg :--batch :--dearmor :-o "/etc/apt/trusted.gpg.d/postgresql.gpg")
         (c/exec "DEBIAN_FRONTEND='noninteractive'"
                 :apt :update)
         (c/exec "DEBIAN_FRONTEND='noninteractive'"
                 :apt :-qy :install :postgresql-15)

         ; start
         (c/exec :systemctl :restart "postgresql@15-main"))
        (info "Installed")))

(def command
  {"postgresql"
   {:opt-spec opt-spec
    :opt-fn opt-fn
    :usage (str "Installs PostgreSQL on " host ".")
    :run run-fn}})