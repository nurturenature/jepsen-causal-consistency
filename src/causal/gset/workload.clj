(ns causal.gset.workload
  (:require [causal.gset
             [client :as client]
             [causal-consistency :as cc]
             [strong-convergence :as sc]]
            [causal.util :as util]
            [jepsen.checker :as checker]))

(def causal-opts
  "Opts to configure Elle for causal consistency."
  {:consistency-models [:strong-session-consistent-view] ; Elle's strong-session with Adya's Consistent View(PL-2+)
   :anomalies          [:internal]                       ; basic hygiene to read your writes in a transaction
   :anomalies-ignored  [:lost-update]                    ; `lost-update`s are causally Ok, but they are PL-2+, Adya 4.1.3 ?!?

   ; where to store anomaly explanations, graphs
   :directory "causal"

   ; causal graph analysis and plotting can be resource intensive
   :cycle-search-timeout 10000
   :max-plot-bytes       1000000
   :plot-timeout         10000})

(defn workload
  "Gset workload."
  [opts]
  {:client          (client/->GSetClient nil)
   :generator       (util/generator opts)
   :final-generator (util/final-generator opts)
   :checker         (checker/compose
                     {:causal-consistency (cc/checker (merge causal-opts opts))
                      :strong-convergence (sc/final-reads)})})

(defn workload-homogeneous-txns
  "A workload with a generator that emits transactions that are all read or write ops,
   E.g. for the ElectricSQL TypeScript client.
   Generator must only generate txns consisting exclusively of reads or writes
   to accommodate the API."
  [{:keys [min-txn-length max-txn-length] :as opts}]
  (let [min-txn-length (* 2 (or min-txn-length 1))
        max-txn-length (* 2 (or max-txn-length 4))
        opts           (assoc opts
                              :min-txn-length     min-txn-length
                              :max-txn-length     max-txn-length
                              :key-dist           :uniform
                              :key-count          100
                              :max-writes-per-key 1000)
        workload (workload opts)
        generator (->> (:generator workload)
                       (mapcat (fn [{:keys [value] :as op}]
                                 (let [[rs ws] (->> value
                                                    (reduce (fn [[rs ws] [f _k _v :as mop]]
                                                              (case f
                                                                :r (if (some #(= % mop) rs)
                                                                     [rs ws]
                                                                     [(conj rs mop) ws])
                                                                :w [rs (conj ws mop)]))
                                                            [[] []]))]
                                   (cond (and (seq rs)
                                              (seq ws))
                                         (let [r-op (assoc op :value rs :f :r-txn)
                                               w-op (assoc op :value ws :f :w-txn)]
                                           (->> [r-op w-op] shuffle vec))

                                         (seq rs)
                                         [(assoc op :value rs :f :r-txn)]

                                         (seq ws)
                                         [(assoc op :value ws :f :w-txn)])))))]
    (assoc workload
           :generator generator)))

(defn workload-single-writes
  "The default workload with a generator that emits transactions consisting of a single write."
  [opts]
  (let [opts      (merge opts
                         {:min-txn-length     1
                          :max-txn-length     1
                          :key-dist           :uniform
                          :key-count          100
                          :max-writes-per-key 1000})
        workload  (workload opts)
        generator (->> (:generator workload)
                       (filter #(->> %
                                     :value
                                     first
                                     first
                                     (= :w)))
                       (map #(assoc % :f :w-txn)))]
    (assoc workload
           :generator generator)))
