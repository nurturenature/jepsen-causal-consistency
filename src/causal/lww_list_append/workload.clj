(ns causal.lww-list-append.workload
  (:require [causal
             [electric-sqlite :as electric-sqlite]
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
            [jepsen
             [checker :as checker]
             [generator :as gen]]
            [jepsen.generator :as gen]))

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
  (merge (electric-sqlite opts)
         {:checker (checker/compose
                    {:strong-convergence (sc/final-reads)})}))

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
  (merge (electric-pglite opts)
         {:checker (checker/compose
                    {:strong-convergence (sc/final-reads)})}))

(defn better-sqlite
  "The electric-sqlite workload with:
   - better-sqlite3 client API"
  [{:keys [min-txn-length max-txn-length] :as opts}]
  (let [opts (assoc opts
                    :min-txn-length (or min-txn-length 2)
                    :max-txn-length (or max-txn-length 4))]

    (merge (electric-sqlite opts)
           {:client (client/->BetterSQLite3Client nil)})))

(defn better-sqlite-strong
  "An better-sqlite workload with only a strong convergence checker."
  [opts]
  (merge (better-sqlite opts)
         {:checker (checker/compose
                    {:strong-convergence (sc/final-reads)})}))

(defn pgexec-pglite
  "The electric-pglite workload with:
   - PGlite.exec client API"
  [{:keys [min-txn-length max-txn-length] :as opts}]
  (let [opts (assoc opts
                    :min-txn-length (or min-txn-length 2)
                    :max-txn-length (or max-txn-length 4))]

    (merge (electric-pglite opts)
           {:client (client/->PGExecClient nil)})))

(defn pgexec-pglite-strong
  "An pgexec-pglite workload with only a strong convergence checker."
  [opts]
  (merge (pgexec-pglite opts)
         {:checker (checker/compose
                    {:strong-convergence (sc/final-reads)})}))

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

(defn active-active
  "A workload for:
   - node n1, process 0, uses multi-op generator
     - PostgreSQL db
     - jdbc driver
   - remaining nodes use single-op generator
     - SQLite3 db
     - ElectricSQL generated client API
   - causal + strong + lww checkers"
  [{:keys [min-txn-length max-txn-length] :as opts}]
  (let [all-keys                 (->> 10
                                      range
                                      (into (sorted-set)))
        read-all                 (->> all-keys
                                      (mapv (fn [k]
                                              [:r k nil])))
        opts                     (assoc opts
                                        :key-dist           :uniform
                                        :key-count          (count all-keys)
                                        :max-writes-per-key 10000)
        {electric-gen :generator
         :as electric-workload}  (electric-sqlite opts)
        electric-gen             (->> electric-gen
                                      (gen/map (fn [{:keys [value] :as op}]
                                                 (if (= :r (->> value first first))
                                                   (assoc op :value read-all)
                                                   op))))
        postgres-gen             (->> (assoc opts
                                             :min-txn-length (or min-txn-length 4)
                                             :max-txn-length (or max-txn-length 4))
                                      list-append/gen
                                      (gen/map (fn [{:keys [value] :as op}]
                                                 (let [value (->> value
                                                                  (mapv (fn [[f k v :as mop]]
                                                                          (case f
                                                                            :r      mop
                                                                            :append [:append k (+ 10000 v)]))))]
                                                   (assoc op :value value)))))]

    (merge electric-workload
           {:generator (gen/mix
                        [; jdbc PostgreSQL
                         (gen/on-threads #{0}
                                         postgres-gen)
                       ; ElectricSQL SQLite3
                         (gen/on-threads #{1 2 3 4}
                                         electric-gen)])})))
(def total-key-count
  "The total number of keys."
  100)

(def default-key-count
  "The default number of keys to act on in a transactions."
  10)

(def all-keys
  "A sorted set of all keys."
  (->> (range total-key-count)
       (into (sorted-set))))

(defn op+
  "Given a :f and :value, creates an :invoke op."
  [f value]
  (merge {:type :invoke :f f :value value}))

(defn updateMany
  "Given a value, returns a updateMany transaction
   to update a random set of keys with the value.
   Optional opts can specify a :key-count for the number of keys to update."
  ([v] (updateMany nil v))
  ([{:keys [key-count] :as _opts} v]
   (let [key-count (or key-count default-key-count)
         in-keys   (->> all-keys
                        shuffle
                        (take key-count)
                        (into []))
         v         (str v)]

     {:data  {:v v}
      :where {:k {:in in-keys}}})))

(defn updateMany-gen
  "Given optional opts, return a lazy sequence of updateMany transactions."
  ([] (updateMany-gen nil))
  ([opts]
   (->> (range)
        (map (fn [v]
               (let [value (updateMany opts v)]
                 (op+ :updateMany value)))))))

(defn findMany
  "Returns a findMany transaction
   to find a random set of keys.
   Optional opts can specify a :key-count for the number of keys to find."
  ([] (findMany {}))
  ([{:keys [key-count] :as _opts}]
   (let [key-count (or key-count default-key-count)
         in-keys   (->> all-keys
                        shuffle
                        (take key-count)
                        (into []))]

     {:where {:k {:in in-keys}}})))

(defn findMany-gen
  "Given optional opts, return a lazy sequence of findMany transactions."
  ([] (findMany-gen nil))
  ([opts]
   (repeatedly (fn []
                 (let [value (findMany opts)]
                   (op+ :findMany value))))))

(defn electric-generator
  "Given option opts, return a generator of ops for the ElectricSQL TypeScript API."
  ([] (electric-generator nil))
  ([opts]
   (gen/mix [(updateMany-gen opts)
             (findMany-gen opts)])))

(defn typescript
  "A workload for:
   - SQLite3 db
   - ElectricSQL generated client API
   - causal + strong + lww checkers"
  [opts]
  {:db              (electric-sqlite/db opts)
   :client          (client/->TypeScriptClient nil)
   :generator       (electric-generator opts)
   :final-generator (util/final-generator opts)
   :checker         (checker/compose
                     {:causal-consistency (adya/checker (merge util/causal-opts opts))
                      :strong-convergence (sc/final-reads)
                      :lww                (lww/checker (merge util/causal-opts opts))})})

(comment
  ;; (set/difference (elle.consistency-model/anomalies-prohibited-by [:strong-session-consistent-view])
  ;;                 (elle.consistency-model/anomalies-prohibited-by [:strong-session-PL-2]))
  ;; #{:G-cursor :G-monotonic :G-single :G-single-item :G-single-item-process :G-single-process :G1-process :lost-update}
  )