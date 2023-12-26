(ns causal.electricsql
  "Install and configure ElectricSQL sync service on node `electricsql`."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen
             [control :as c]
             [util :as u]]))

(def host
  "Name of host machine for ElectricSQL."
  "electricsql")

(def install-dir
  "Directory to install ElectricSQL to."
  "/root/electricsql")

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
  (info "Installing ElectricSQL on " host)
  (c/on host
        (c/su
         ; stop and cleanup any existing
         (u/meh
          (c/exec (str install-dir "/_build/prod/rel/electric/bin/electric stop"))
          (c/exec :rm :-rf install-dir))

         ; dependencies



         ; start

         ; enable logical replication
         )
        (info "Installed")))

(def command
  {"electricsql"
   {:opt-spec opt-spec
    :opt-fn opt-fn
    :usage (str "Installs ElectricSQL on " host ".")
    :run run-fn}})