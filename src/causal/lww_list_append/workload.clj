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
