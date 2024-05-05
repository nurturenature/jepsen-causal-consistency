(ns causal.lww-list-append.workload
  (:require [causal.lww-list-append.client :as client]
            [causal.lww-list-append.checker
             [adya :as adya]
             [strong-convergence :as sc]]
            [elle.list-append :as l-a]
            [causal.util :as util]
            [jepsen.checker :as checker]))

(defn causal
  "Basic LWW list-append workload *only* a causal consistency checker."
  [opts]
  {:client          (client/->LWWListAppendClient nil)
   :generator       (l-a/gen opts)
   :final-generator (util/final-generator opts)
   :checker         (checker/compose
                     {:causal-consistency (adya/checker (merge util/causal-opts opts))})})

(defn strong
  "Basic LWW list-append workload with *only* a strong convergence checker."
  [opts]
  (merge (causal opts)
         {:checker (checker/compose
                    {:strong-convergence (sc/final-reads)})}))

(defn causal+strong
  "Basic LWW list-append workload with *only* a strong convergence checker."
  [opts]
  (merge (causal opts)
         {:checker (checker/compose
                    {:causal-consistency (adya/checker (merge util/causal-opts opts))
                     :strong-convergence (sc/final-reads)})}))

(defn intermediate-read
  "Custom workload to demonstrate intermediate-read anomalies."
  [opts]
  (let [opts (merge opts
                    {:consistency-models []
                     :anomalies          [:G-single-item :G1b]
                     :anomalies-ignored  nil}
                    {:min-txn-length     4
                     :max-writes-per-key 128})]
    (causal+strong opts)))

(comment
  ;; (set/difference (elle.consistency-model/anomalies-prohibited-by [:strong-session-consistent-view])
  ;;                 (elle.consistency-model/anomalies-prohibited-by [:strong-session-PL-2]))
  ;; #{:G-cursor :G-monotonic :G-single :G-single-item :G-single-item-process :G-single-process :G1-process :lost-update}
  )