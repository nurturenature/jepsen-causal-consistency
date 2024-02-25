(ns causal.nemesis
  (:require [causal.sqlite3 :as sqlite3]
            [jepsen
             [control :as c]
             [generator :as gen]
             [nemesis :as nemesis]]
            [jepsen.nemesis.combined :as nc]))

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
                                   (c/on-nodes test targets sqlite3/stop!))
                     :start-node (c/on-nodes test sqlite3/start!))]
        (assoc op :value result)))

    (teardown! [_this _test]
      nil)))

(defn stop-start-package
  "A nemesis and generator package that politely stops and starts the db.
   
   Opts:
   ```clj
   {:stop-start {:targets [...]}}  ; A collection of node specs, e.g. [:one, :all]
  ```."
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

(defn nemesis-package
  "Constructs combined nemeses and generators into a nemesis package."
  [opts]
  (let [opts (update opts :faults set)]
    (->> [(stop-start-package opts)]
         (concat (nc/nemesis-packages opts))
         (remove nil?)
         nc/compose-packages)))