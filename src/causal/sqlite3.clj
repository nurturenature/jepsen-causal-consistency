(ns causal.sqlite3
  (:require [clojure.string :refer [split-lines]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen
             [db :as db]
             [control :as c]
             [util :as u]]
            [jepsen.control
             [util :as cu]]
            [jepsen.os.debian :as deb]
            [slingshot.slingshot :refer [try+]]))

(defn db
  "ElectricSQL SQLite database."
  []
  (reify db/DB
    (setup! [this test node])

    (teardown! [this test node])

    ; ElectricSQL doesn't have `primaries`.
    ; db/Primary

    db/LogFiles
    (log-files [_db _test _node])

    db/Kill
    (start! [_this test node])

    (kill! [_this _test _node])

    db/Pause
    (pause! [_this _test _node])

    (resume! [_this _test _node])))
