(ns causal.strong-convergence
  (:require [clojure.set :as set]
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
            history'     (->> history
                              (h/filter :final-read?))
            node-finals  (->> history'
                              (reduce (fn [acc {:keys [node value] :as _op}]
                                        (assoc acc node value))
                                      {}))
            value-finals (->> node-finals
                              (group-by val)
                              (map (fn [[read read-by]]
                                     [read (keys read-by)])))]
        (merge
         {:valid? true}
         ; final read from all nodes?
         (when (seq (set/difference nodes (set (keys node-finals))))
           {:valid? false
            :missing-node-reads (set/difference nodes (set (keys node-finals)))})

         ; all reads are the same?
         (when (= 1 (count value-finals))
           {:final-read (->> value-finals
                             first
                             first
                             (map (fn [[_f k v]]
                                    (if (nil? v)
                                      nil
                                      [k v])))
                             (remove nil?)
                             sort)})
         (when (< 1 (count value-finals))
           {:valid? false
            :divergent-final-reads value-finals}))))))