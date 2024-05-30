(ns causal.non-electric-postgres
  (:require [causal.util :as util]
            [clojure.tools.logging :refer [info]]
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
  (str install-dir "/electric-sqlite"))

(defn app-env-map
  [{:keys [electric-host postgres-host] :as _opts}]
  {:ELECTRIC_DATABASE_HOST postgres-host
   :ELECTRIC_DATABASE_NAME "electric"
   :ELECTRIC_SERVICE       (str "http://" electric-host ":5133")
   :ELECTRIC_SERVICE_HOST  electric-host})

(defn app-env
  [opts]
  (->> (app-env-map opts)
       (c/env)))

(def electricsql-setup?
  "Is ElectricSQL setup?"
  (atom false))

(defn db
  "non-electric-postgres database."
  [opts]
  (reify db/DB
    (setup!
      [_this _test _node]
      (info "Setting up non-electric-postgres client")

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
               (util/git-clean-pull)))
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
          (info "Running db:migrate")
          (c/cd app-dir
                (c/exec (app-env opts) :npm :run "db:migrate"))

          (swap! electricsql-setup? (fn [_] true)))))

    (teardown!
      [_this _test _node])

    ; ElectricSQL doesn't have `primaries`.
    ; db/Primary

    ; logs are on postgres host
    ; db/LogFiles
    ; (log-files
    ;   [_db _test _node])

    db/Kill
    (start!
      [_this _test _node])

    (kill!
      [_this _test _node])

    db/Pause
    (pause!
      [_this _test _node])

    (resume!
      [_this _test _node])))
