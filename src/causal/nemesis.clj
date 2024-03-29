(ns causal.nemesis
  (:require [causal.sqlite3 :as sqlite3]
            [clj-http.client :as http]
            [jepsen
             [control :as c]
             [db :as db]
             [generator :as gen]
             [nemesis :as nemesis]
             [util :as u]]
            [jepsen.nemesis.combined :as nc]))

(defn start!
  "Generic start!, justs calls db/start! for this `db`."
  [test node]
  (db/start! (sqlite3/db) test node))

(defn stop!
  "A more polite stop! that sends a stop request to the client."
  [test node]
  (try
    (http/post (str "http://" node ":8089/control/stop"))
    (catch Exception _e)
    (finally
      (db/kill! (sqlite3/db) test node)))
  :stopped)

(defn stop-start-nemesis
  "A nemesis to politely stop and start the db.
   This nemesis responds to:
  ```
  {:f :stop-node  :value :node-spec}   ; target nodes as interpreted by `db-nodes`
  {:f :start-node :value nil}
   ```"
  [db]
  (reify
    nemesis/Reflection
    (fs [_this]
      [:stop-node :start-node])

    nemesis/Nemesis
    (setup! [this _test]
      this)

    (invoke! [_this test {:keys [f value] :as op}]
      (let [result (case f
                     :stop-node  (let [targets (nc/db-nodes test db value)]
                                   (c/on-nodes test targets stop!))
                     :start-node (c/on-nodes test start!))]
        (assoc op :value result)))

    (teardown! [_this _test]
      nil)))

(defn stop-start-package
  "A nemesis and generator package that politely stops and starts the db.
   
   Opts:
   ```clj
   {:stop-start {:targets [...]}}  ; A collection of node specs, e.g. [:one, :all]
  ```"
  [{:keys [db faults interval stop-start] :as _opts}]
  (let [needed?    (contains? faults :stop-start)
        targets    (:targets stop-start (nc/node-specs db))
        stop-node  (fn stop-node [_ _]
                     {:type  :info
                      :f     :stop-node
                      :value (rand-nth targets)})
        start-node {:type  :info
                    :f     :start-node
                    :value nil}
        gen       (->> (gen/flip-flop stop-node (repeat start-node))
                       (gen/stagger (or interval nc/default-interval)))]
    {:generator       (when needed? gen)
     :final-generator (when needed? start-node)
     :nemesis         (stop-start-nemesis db)
     :perf            #{{:name  "stop-start"
                         :start #{:stop-node}
                         :stop  #{:start-node}
                         :color "#D1E8A0"}}}))

(defn reset-db!
  "Resets a client db:
     - stop client
     - rm local SQLite3 db
     - start client
     - which resyncs db and resumes processing transactions"
  [test node]
  (stop! test node)
  (sqlite3/wipe)
  (start! test node)
  (u/sleep 3000)  ; time to sync
  :reset-db)

(defn reset-db-nemesis
  "A nemesis to reset the client db.
   This nemesis responds to:
  ```
  {:f :reset-db  :value :node-spec}   ; target nodes as interpreted by `db-nodes`
   ```"
  [db]
  (reify
    nemesis/Reflection
    (fs [_this]
      [:reset-db])

    nemesis/Nemesis
    (setup! [this _test]
      this)

    (invoke! [_this test {:keys [f value] :as op}]
      (let [result (case f
                     :reset-db (let [targets (nc/db-nodes test db value)]
                                 (c/on-nodes test targets reset-db!)))]
        (assoc op :value result)))

    (teardown! [_this _test]
      nil)))

(defn reset-db-package
  "A nemesis and generator package that resets the client db.
   
   Opts:
   ```clj
   {:reset-db {:targets [...]}}  ; A collection of node specs, e.g. [:one, :all]
  ```"
  [{:keys [db faults interval reset-db] :as _opts}]
  (let [needed?   (contains? faults :reset-db)
        targets   (:targets reset-db (nc/node-specs db))
        reset-db  (fn reset-db [_ _]
                    {:type  :info
                     :f     :reset-db
                     :value (rand-nth targets)})
        gen       (->> reset-db
                       (gen/stagger (or interval nc/default-interval)))
        final-gen {:type  :info
                   :f     :reset-db
                   :value :all}]
    {:generator       (when needed? gen)
     :final-generator (when needed? final-gen)
     :nemesis         (reset-db-nemesis db)
     :perf            #{{:name  "reset-db"
                         :fs    #{:reset-db}
                         :start #{}
                         :stop  #{}
                         :color "#ADE8A0"}}}))

(defn nemesis-package
  "Constructs combined nemeses and generators into a nemesis package."
  [opts]
  (let [opts (update opts :faults set)]
    (->> [(stop-start-package opts)
          (reset-db-package   opts)]
         (concat (nc/nemesis-packages opts))
         (remove nil?)
         nc/compose-packages)))