(ns causal.lww-list-append.workload
  (:require [causal.lww-list-append.client :as client]
            [causal.lww-list-append.checker
             [adya :as adya]]
            [elle.list-append :as l-a]
            [causal.util :as util]
            [jepsen.checker :as checker]))

(defn workload
  "Basic LWW list-append workload."
  [opts]
  {:client          (client/->LWWListAppendClient nil)
   :generator       (l-a/gen opts)
   :final-generator (util/final-generator opts)
   :checker         (checker/compose
                     {:causal-consistency (adya/checker (merge util/causal-opts opts))})})
