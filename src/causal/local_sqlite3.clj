(ns causal.local-sqlite3
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
  (str install-dir "/local-sqlite3"))

(def database-dir
  "Local SQLite3 database dir."
  "/var/jepsen/shared/db")

(def database-file
  "Local SQLite3 database file."
  (str database-dir "/local.db"))

(def database-files
  "A collection of all local SQLite3 database files."
  [database-file
   (str database-file "-shm")
   (str database-file "-wal")])

(def pid-file (str app-dir "/client.pid"))

(def log-file-short "client.log")
(def log-file       (str app-dir "/" log-file-short))

(def app-ps-name "node")

(def local-sqlite3-setup?
  "Is local SQLite3 setup?"
  (atom false))

(defn wipe
  "Wipes local SQLite3 db files.
   Assumes on node and privs for file deletion."
  []
  (c/exec :rm :-rf database-files))

(defn db
  "Local SQLite3 database."
  []
  (reify db/DB
    (setup!
      [this test node]
      (info "Setting up local SQLite3 client")

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

      ; one client sets up local SQLite3
      (locking local-sqlite3-setup?
        (when-not @local-sqlite3-setup?
          (warn "Creating local SQLite3 db and applying migrations")
          (c/cd app-dir
                (c/exec :mkdir :-p database-dir)
                (c/exec :sqlite3 database-file :< "./db/migrations/02-create_lww_list_append_table.sql"))

          (swap! local-sqlite3-setup? (fn [_] true))))

      ; build client
      (c/cd app-dir
            (c/exec :npm :run "client:build"))

      (db/start! this test node))

    (teardown!
      [this test node]
      (info "Tearing down local SQLite3")
      (db/kill! this test node)
      (c/su
       (wipe)
       (c/exec :rm :-rf log-file)))

    ; local SQLite3 doesn't have `primaries`.
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
            {:chdir   app-dir
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
