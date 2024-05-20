(ns causal.lww-list-append.checker.lww
  (:require [causal.util :as u]
            [causal.lww-list-append.checker
             [adya :as adya]
             [cyclic-versions :as cyclic-versions]
             [graph :as adya-g]]
            [elle
             [core :as elle]
             [txn :as txn]]
            [jepsen
             [checker :as checker]
             [history :as h]
             [store :as store]]))

(defn graph
  "Given indexes and a history of client transactions, computes a
   ```
   {:graph      transaction-graph
    :explainer  explainer-for-graph
    :anomalies  seq-of-anomalies}
   ```
   by combining a w->w graph derived from observed version ordering and a realtime ordered graph."
  [{:keys [observed-vg write-index] :as _indexes} history-clients]
  (let [; TODO: note use of observed-vg as causal-vg to include monotonic reads
        ;       can this be done in the adya checker too?
        ww-tg     (adya-g/ww-tg {:causal-vg   observed-vg
                                 :write-index write-index} nil)
        analyzers [elle/realtime-graph
                   (fn [_history] ww-tg)]

        analyzer  (apply elle/combine analyzers)]

    (analyzer history-clients)))

(defn check
  "Does a w->w ordering derived from observed version order combined with a realtime graph cycle?
   
   Returns nil or an anomalies map.
   
   To align with Elle, we test for
   ```
   :strong-PL-1 [:strong-PL-1-cycle-exists
                 :G0-realtime]
   ```"
  [opts history-complete]
  (let [history-clients (->> history-complete
                             h/client-ops)
        history-oks     (->> history-clients
                             ; TODO: shouldn't be any :info in total sticky availability, handle explicitly
                             h/oks)

        opts            (-> opts
                            ; lww anomalies
                            (update :anomalies into [:G0-realtime :strong-PL-1-cycle-exists])
                            (update :directory str "/lww"))

        {:keys [observed-cyclic-versions]
         :as indexes}   (adya/indexes history-oks)

        cycles          (->> history-clients
                             (txn/cycles! opts (partial graph indexes))
                             :anomalies)
        anomalies       (cond-> cycles
                          observed-cyclic-versions
                          (assoc :cyclic-versions observed-cyclic-versions))]

    (txn/result-map opts anomalies)))

(defn checker
  "For Jepsen test map."
  [defaults]
  (reify checker/Checker
    (check [_this test history opts]
      (let [opts (merge defaults opts)
            opts (update opts :directory (fn [old]
                                           (if (nil? old)
                                             nil
                                             (store/path test [old]))))
            results (check opts history)]

        ; chart cyclic-versions
        (let [cyclic-versions (get-in results [:anomalies :cyclic-versions])
              output-dir      (:directory opts)]
          (when (and (seq cyclic-versions)
                     output-dir)
            (cyclic-versions/viz cyclic-versions (str output-dir "/cyclic-versions") history)))

        ; chart G0-realtime versions
        (let [G0-realtime (->> results
                               :anomalies
                               :G0-realtime
                               (mapcat :steps)
                               (filter (fn [{:keys [type]}] (= type :ww)))
                               (map (fn [{:keys [kv kv']}]
                                      {:sources #{:G0-realtime} :sccs (list #{kv kv'})})))
              output-dir  (:directory opts)]
          (when (and (seq G0-realtime)
                     output-dir)
            (cyclic-versions/viz G0-realtime (str output-dir "/G0-realtime-versions") history)))

        results))))
