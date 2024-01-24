(ns causal.strong-convergence
  (:require [clojure.set :as set]
            [jepsen
             [checker :as checker]
             [history :as h]]))

(defn mops->map
  "Takes a sequence of read mops and returns a k/v map with nils removed."
  [mops]
  (->> mops
       (reduce (fn [mops' [f k v]]
                 (assert (= :r f))
                 (if (nil? v)
                   mops'
                   (assoc mops' k v)))
               (sorted-map))))

(defn final-reads
  "Do `:final-read? true` reads strongly converge?"
  []
  (reify checker/Checker
    (check [_this {:keys [nodes noop-nodes] :as _test} history _opts]
      (let [noop-nodes   (into #{} noop-nodes)
            nodes        (into #{} nodes)
            nodes        (set/difference nodes noop-nodes)
            history      (->> history
                              h/client-ops
                              h/oks
                              (h/remove :noop?))
            history'     (->> history
                              (h/filter :final-read?))
            node-finals  (->> history'
                              (reduce (fn [acc {:keys [node value] :as _op}]
                                        (assoc acc node (mops->map value)))
                                      {}))
            summary      (->> node-finals
                              (reduce (fn [acc [node reads]]
                                        (->> reads
                                             (reduce (fn [acc [k v]]
                                                       (-> acc
                                                           (update k (fn [old]
                                                                       (if (nil? old)
                                                                         (sorted-map)
                                                                         old)))
                                                           (update-in [k v] (fn [old]
                                                                              (if (nil? old)
                                                                                (sorted-set node)
                                                                                (conj old node))))))
                                                     acc)))
                                      (sorted-map))
                              (remove (fn [[_k vs]]
                                        (if (->> vs keys count (= 1))
                                          true
                                          false))))
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
                             first)})
         (when (< 1 (count value-finals))
           {:valid? false
            :divergent-final-reads summary}))))))
