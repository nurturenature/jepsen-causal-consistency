(ns causal.pglite
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
  (str install-dir "/pglite"))

(def pid-file (str app-dir "/client.pid"))

(def log-file-short "client.log")
(def log-file       (str app-dir "/" log-file-short))

(def app-ps-name "node")

(defn app-env-map
  [{:keys [electric-host] :as _opts}]
  {:ELECTRIC_SERVICE (str "http://" electric-host ":5133")})

(defn app-env
  [opts]
  (->> (app-env-map opts)
       (c/env)))

(defn db
  "ElectricSQL PGlite database."
  [opts]
  (reify db/DB
    (setup!
      [this test node]
      (info "Setting up PGlite client")

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
            (c/exec :rm :-rf "node_modules/" "dist/" "src/generated/")
            (c/exec :npm :install)
            (c/exec :sed :-i "1s/^/import { WebSocket } from 'ws';\\n/" "./node_modules/electric-sql/dist/sockets/web.js"))

      ; build client
      (c/cd app-dir
            (c/exec (app-env opts) :npm :run "client:generate")
            (c/exec (app-env opts) :npm :run "client:build"))

      (db/start! this test node))

    (teardown!
      [this test node]
      (info "Tearing down PGlite")
      (db/kill! this test node)
      (c/exec :rm :-rf log-file))

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