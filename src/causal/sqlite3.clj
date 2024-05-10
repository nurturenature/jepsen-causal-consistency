(ns causal.sqlite3
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen
             [db :as db]
             [control :as c]
             [util :as u]]
            [jepsen.control
             [util :as cu]]
            [jepsen.os.debian :as deb]))

(def install-dir
  "Directory to install into."
  "/jepsen/jepsen-causal-consistency")

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

(defn app-env-map
  [{:keys [electric-host] :as _opts}]
  (let [electric-host (or electric-host "electric")]
    {:ELECTRIC_SERVICE (str "http://" electric-host ":5133")}))

(defn app-env
  [opts]
  (->> (app-env-map opts)
       (c/env)))

(def electricsql-setup?
  "Is ElectricSQL setup?"
  (atom false))

(defn wipe
  "Wipes local SQLite3 db files.
   Assumes on node and privs for file deletion."
  []
  (c/exec :rm :-rf database-files))

(defn db
  "ElectricSQL SQLite database."
  [opts]
  (reify db/DB
    (setup!
      [this test node]
      (info "Setting up SQLite3 client")

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

      ; install deps
      (c/cd app-dir
            (c/exec :npm :install))

      ; one client sets up ElectricSQL
      (locking electricsql-setup?
        (when-not @electricsql-setup?
          (warn "Assuming that db migrations have already been run, db is available with tables created")
          ;; (c/cd app-dir
          ;;       c/exec (app-env opts) :npm :run "db:migrate")

          (swap! electricsql-setup? (fn [_] true))))

      ; build client
      (c/cd app-dir
            (c/exec (app-env opts) :npm :run "client:generate")
            (c/exec (app-env opts) :npm :run "client:build"))

      (db/start! this test node))

    (teardown!
      [this test node]
      (info "Tearing down SQLite3")
      (db/kill! this test node)
      (c/su
       (wipe)
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
           (cu/start-daemon!
            {:env     (app-env-map opts)
             :chdir   app-dir
             :logfile log-file
             :pidfile pid-file}
            "/usr/bin/npm" :run "app:start"))
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
