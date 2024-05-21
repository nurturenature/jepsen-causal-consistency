(ns causal.lww-list-append.workload
  (:require [causal
             [local-sqlite3 :as local-sqlite3]
             [pglite :as pglite]
             [sqlite3 :as sqlite3]]
            [causal.lww-list-append
             [client :as client]]
            [causal.lww-list-append.checker
             [adya :as adya]
             [lww :as lww]
             [strong-convergence :as sc]]
            [causal.util :as util]
            [elle
             [list-append :as list-append]
             [txn :as txn]]
            [jepsen.checker :as checker]))

(defn electric-sqlite
  "A workload for:
   - SQLite3 db
   - ElectricSQL generated client API
   - single mop txn generator
   - causal + strong + lww checkers"
  [{:keys [min-txn-length max-txn-length] :as opts}]
  (let [opts (assoc opts
                    :min-txn-length (or min-txn-length 1)
                    :max-txn-length (or max-txn-length 1))]

    {:db              (sqlite3/db opts)
     :client          (client/->ElectricSQLClient nil)
     :generator       (list-append/gen opts)
     :final-generator (util/final-generator opts)
     :checker         (checker/compose
                       {:causal-consistency (adya/checker (merge util/causal-opts opts))
                        :strong-convergence (sc/final-reads)
                        :lww                (lww/checker (merge util/causal-opts opts))})}))

(defn electric-sqlite-strong
  "An electric-sqlite workload with only a strong convergence checker."
  [opts]
  (update (electric-sqlite opts)
          :checker
          dissoc :causal-consistency :lww))

(defn better-sqlite
  "The electric-sqlite workload with:
   - better-sqlite3 client API"
  [{:keys [min-txn-length max-txn-length] :as opts}]
  (let [opts (assoc opts
                    :min-txn-length (or min-txn-length 2)
                    :max-txn-length (or max-txn-length 4))]

    (merge (electric-sqlite opts)
           {:client (client/->BetterSQLite3Client nil)})))

(defn electric-pglite
  "The electric-sqlite workload with:
   - PGlite db
   - ElectricSQL generated client API"
  [opts]
  (merge (electric-sqlite opts)
         {:db     (pglite/db opts)
          :client (client/->PGliteClient nil)}))

(defn electric-pglite-strong
  "An electric-pglite workload with only a strong convergence checker."
  [opts]
  (update (electric-pglite opts)
          :checker
          dissoc :causal-consistency :lww))

(defn pgexec-pglite
  "The electric-pglite workload with:
   - PGlite.exec client API"
  [{:keys [min-txn-length max-txn-length] :as opts}]
  (let [opts (assoc opts
                    :min-txn-length (or min-txn-length 2)
                    :max-txn-length (or max-txn-length 4))]

    (merge (electric-pglite opts)
           {:client (client/->PGExecClient nil)})))

(defn local-sqlite
  "A workload for:
   - single shared SQLite3 db
   - better-sqlite3 client API
   - multiple mop txn generator
   - causal + strong + lww checkers"
  [{:keys [min-txn-length max-txn-length] :as opts}]
  (let [opts (assoc opts
                    :min-txn-length (or min-txn-length 2)
                    :max-txn-length (or max-txn-length 4))]

    {:db              (local-sqlite3/db)
     :client          (client/->BetterSQLite3Client nil)
     :generator       (list-append/gen opts)
     :final-generator (util/final-generator opts)
     :checker         (checker/compose
                       {:causal-consistency (adya/checker (merge util/causal-opts opts))
                        :strong-convergence (sc/final-reads)
                        :lww                (lww/checker (merge util/causal-opts opts))})}))

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

(comment
  ;; (set/difference (elle.consistency-model/anomalies-prohibited-by [:strong-session-consistent-view])
  ;;                 (elle.consistency-model/anomalies-prohibited-by [:strong-session-PL-2]))
  ;; #{:G-cursor :G-monotonic :G-single :G-single-item :G-single-item-process :G-single-process :G1-process :lost-update}
  )