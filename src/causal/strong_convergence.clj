(ns causal.strong-convergence
  (:require [clojure.set :as set]
            [elle.txn :as txn]
            [jepsen
             [checker :as checker]
             [history :as h]]))

(defn final-reads
  "Do `:final-read? true` reads strongly converge?"
  []
  (reify checker/Checker
    (check [_this {:keys [nodes] :as _test} history _opts]
      (let [nodes        (set nodes)
            history      (->> history
                              h/client-ops
                              h/oks)
            all-keys     (->> history
                              (h/remove :final-read?)
                              txn/all-keys
                              (into #{}))
            history'     (->> history
                              (h/filter :final-read?))
            all-keys'    (->> history'
                              txn/all-keys
                              (into #{}))
            node-finals  (->> history'
                              (reduce (fn [acc {:keys [node value] :as _op}]
                                        (assoc acc node value))
                                      {}))
            value-finals  (->> node-finals
                               (group-by val)
                               (map (fn [[k v]]
                                      [k (->> v keys sort)])))
            value-finals' (->> value-finals
                               (map (fn [[v nodes]]
                                      [(->> v
                                            (filter (fn [[_f _k v]] v))
                                            (map (fn [[_f k v]] [k v]))
                                            (into (sorted-map)))
                                       nodes])))]
        (merge
         {:valid? true}
         ; final read from all nodes?
         (when (seq (set/difference nodes (set (keys node-finals))))
           {:valid? false
            :missing-node-reads (set/difference nodes (set (keys node-finals)))})

         ; all reads are the same?
         (when (= 1 (count value-finals))
           {:final-read value-finals'})
         (when (< 1 (count value-finals))
           {:valid? false
            :divergent-final-reads value-finals'})

         ; reads contained all-keys?
         (when (seq (set/difference all-keys all-keys'))
           {:valid? false
            :missing-keys-reads (set/difference all-keys all-keys')}))))))