(ns causal.lww-list-append.workload
  (:require [causal.lww-list-append.client :as client]
            [causal.lww-list-append.checker
             [adya :as adya]
             [lww :as lww]
             [strong-convergence :as sc]]
            [causal.util :as util]
            [elle
             [list-append :as list-append]
             [txn :as txn]]
            [jepsen.checker :as checker]))

(defn causal
  "Basic LWW list-append workload *only* a causal consistency checker."
  [opts]
  {:client          (client/->LWWListAppendClient nil)
   :generator       (list-append/gen opts)
   :final-generator (util/final-generator opts)
   :checker         (checker/compose
                     {:causal-consistency (adya/checker (merge util/causal-opts opts))})})

(defn strong
  "Basic LWW list-append workload with *only* a strong convergence checker."
  [opts]
  (merge (causal opts)
         {:checker (checker/compose
                    {:strong-convergence (sc/final-reads)})}))

(defn lww
  "Basic LWW list-append workload with *only* a lww checker."
  [opts]
  (merge (causal opts)
         {:checker (checker/compose
                    {:lww (lww/checker (merge util/causal-opts opts))})}))

(defn causal+strong
  "Basic LWW list-append workload with both a causal and strong convergence checker."
  [opts]
  (merge (causal opts)
         {:checker (checker/compose
                    {:causal-consistency (adya/checker (merge util/causal-opts opts))
                     :strong-convergence (sc/final-reads)})}))

(defn causal+strong+lww
  "Basic LWW list-append workload with a causal, a strong convergence, and a lww checker."
  [opts]
  (merge (causal opts)
         {:checker (checker/compose
                    {:causal-consistency (adya/checker (merge util/causal-opts opts))
                     :strong-convergence (sc/final-reads)
                     :lww                (lww/checker (merge util/causal-opts opts))})}))

(defn strong+lww
  "Basic LWW list-append workload with a strong convergence and lww checker."
  [opts]
  (merge (causal opts)
         {:checker (checker/compose
                    {:strong-convergence (sc/final-reads)
                     :lww                (lww/checker (merge util/causal-opts opts))})}))

(defn intermediate-read
  "Custom workload to demonstrate intermediate-read anomalies."
  [opts]
  (let [opts (merge opts
                    {:consistency-models []
                     :anomalies          [:G-single-item :G1b]
                     :anomalies-ignored  nil}
                    {:min-txn-length     4
                     :max-writes-per-key 128})]
    (causal+strong+lww opts)))

(defn read-your-writes
  "Custom workload to demonstrate read-your-writes anomalies."
  [opts]
  (let [opts (merge opts
                    {:consistency-models []
                     :anomalies          [:G-single-item-process :cyclic-versions]
                     :anomalies-ignored  nil}
                    {:min-txn-length     4
                     :max-writes-per-key 128})]
    (causal+strong+lww opts)))

(def homogeneous-generator
  "A generator that produces homogeneous transactions to work with the ElectricSQL TypeScript API:
   - single mop :appends
   - single or multi mop reads with distinct keys"
  (->>
   ; seq of mops 
   {:min-txn-length     1
    :max-txn-length     1
    :max-writes-per-key 256}
   list-append/append-txns
   (map first)

   ; process in chunks
   (partition 1000)

   ; combine mops into homogeneous txns 
   (mapcat (fn [mops]
             (->> mops
                  (reduce (fn [[txns txn keys] [f k _v :as mop]]
                            (cond
                              ; append is always a single mop
                              (= :append f)
                              (let [new-txns  (if (seq txn)
                                                [txn [mop]]
                                                [[mop]])]
                                [(into txns new-txns) [] #{}])

                              ; read of new key
                              (and (= :r f)
                                   (not (contains? keys k)))
                              [txns (conj txn mop) (conj keys k)]

                              ; read of key already in txn
                              :else
                              [(conj txns txn) [mop] #{k}]))
                          [nil [] #{}])
                  first       ; destructure accumulator, note we drop any mops in current txn
                  reverse)))  ; keep original generator order
   (txn/gen)))

(defn homogeneous
  "Custom workload exclusively for ElectricSQL TypeScript Clients."
  [opts]
  (merge (causal+strong+lww opts)
         {:generator homogeneous-generator}))

(comment
  ;; (set/difference (elle.consistency-model/anomalies-prohibited-by [:strong-session-consistent-view])
  ;;                 (elle.consistency-model/anomalies-prohibited-by [:strong-session-PL-2]))
  ;; #{:G-cursor :G-monotonic :G-single :G-single-item :G-single-item-process :G-single-process :G1-process :lost-update}
  )