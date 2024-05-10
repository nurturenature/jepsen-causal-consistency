(ns causal.nemesis
  (:require [clj-http.client :as http]
            [jepsen
             [control :as c]
             [generator :as gen]
             [nemesis :as nemesis]]
            [jepsen.nemesis.combined :as nc]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn online!
  "Bring sync service online."
  [_test node]
  (try+
   (http/get (str "http://" node ":8089/control/connect"))
   :online

   (catch (and (instance? java.net.ConnectException %)
               (re-find #"Connection refused" (.getMessage %)))
          {}
     :connection-refused)))

(defn offline!
  "Take sync service offline."
  [_test node]
  (try+
   (http/get (str "http://" node ":8089/control/disconnect"))
   :offline

   (catch (and (instance? java.net.ConnectException %)
               (re-find #"Connection refused" (.getMessage %)))
          {}
     :connection-refused)))

(defn offline-online-nemesis
  "A nemesis to take the sync service offline and online.
   This nemesis responds to:
  ```
  {:f :offline :value :node-spec}   ; target nodes as interpreted by `db-nodes`
  {:f :online  :value nil}
   ```"
  [db]
  (reify
    nemesis/Reflection
    (fs [_this]
      [:offline :online])

    nemesis/Nemesis
    (setup! [this _test]
      this)

    (invoke! [_this test {:keys [f value] :as op}]
      (let [result (case f
                     :offline (let [targets (nc/db-nodes test db value)]
                                (c/on-nodes test targets offline!))
                     :online  (c/on-nodes test online!))]
        (assoc op :value result)))

    (teardown! [_this _test]
      nil)))

(defn offline-online-package
  "A nemesis and generator package that takes the sync service offline and online.
   
   Opts:
   ```clj
   {:offline-online {:targets [...]}}  ; A collection of node specs, e.g. [:one, :all]
  ```"
  [{:keys [db faults interval offline-online] :as _opts}]
  (let [needed?    (contains? faults :offline-online)
        targets    (:targets offline-online (nc/node-specs db))
        offline    (fn offline [_ _]
                     {:type  :info
                      :f     :offline
                      :value (rand-nth targets)})
        online     {:type  :info
                    :f     :online
                    :value nil}
        gen       (->> (gen/flip-flop offline (repeat online))
                       (gen/stagger (or interval nc/default-interval)))]
    {:generator       (when needed? gen)
     :final-generator (when needed? online)
     :nemesis         (offline-online-nemesis db)
     :perf            #{{:name  "offline-online"
                         :start #{:offline}
                         :stop  #{:online}
                         :color "#D1E8A0"}}}))

(defn nemesis-package
  "Constructs combined nemeses and generators into a nemesis package."
  [opts]
  (let [opts (update opts :faults set)]
    (->> [(offline-online-package opts)]
         (concat (nc/nemesis-packages opts))
         (remove nil?)
         nc/compose-packages)))