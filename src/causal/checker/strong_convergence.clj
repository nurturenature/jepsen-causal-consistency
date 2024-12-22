(ns causal.checker.strong-convergence
  (:require [causal.checker.graph :as cc-g]
            [clojure.set :as set]
            [jepsen
             [checker :as checker]
             [history :as h]]))

(defn final-reads
  "Do `:final-read? true` reads strongly converge?
   Check:
     - final read from all nodes
     - final read values the same for all nodes
     - all read values were actually written, :ok or :info"
  []
  (reify checker/Checker
    (check [_this {:keys [nodes] :as _test} history _opts]
      (let [nodes           (->> nodes (into (sorted-set)))
            history         (->> history
                                 h/client-ops)
            history-possible (->> history
                                  h/possible)
            history-oks     (->> history
                                 h/oks)
            history-reads   (->> history-oks
                                 (h/filter :final-read?))
            write-index     (cc-g/write-index history-possible)

            ; {k {v #{node}}}
            summary         (->> history-reads
                                 (reduce (fn [summary {:keys [node value] :as _op}]
                                           (->> value
                                                (reduce (fn [summary [_f k v]]
                                                          (update-in summary [k v] set/union #{node}))
                                                        summary)))
                                         nil))

            ; {k #{node}}
            missing-nodes   (->> summary
                                 (reduce (fn [missing-nodes [k v->nodes]]
                                           (let [read-nodes (->> v->nodes
                                                                 vals
                                                                 (apply set/union))
                                                 missing    (set/difference nodes read-nodes)]
                                             (if (seq missing)
                                               (update missing-nodes k set/union missing)
                                               missing-nodes)))
                                         (sorted-map)))

            ; {k {[node] v}} sorted by k, sorted by node->v, with nodes sorted
            divergent-reads (->> summary ; {k {v #{node}}}
                                 ; filter to k's with multiple {v #{node}}
                                 (filter (fn [[_k v->nodes]]
                                           (not= 1 (count v->nodes))))
                                 ; reverse map of v->nodes to nodes->v
                                 (map (fn [[k v->nodes]]
                                        [k (->> (zipmap (map vec (map sort (vals v->nodes))) (keys v->nodes))
                                                (into (sorted-map-by (fn [x y] (compare (first x) (first y))))))]))
                                 (into (sorted-map)))

            invalid-reads   (->> summary
                                 (reduce (fn [invalid-reads [k v->nodes]]
                                           (->> v->nodes
                                                (reduce (fn [invalid-reads [v nodes]]
                                                          (let [v (last v)]
                                                            (cond
                                                              ; nil read
                                                              (nil? v)
                                                              invalid-reads

                                                              ; read value not in write index
                                                              (nil? (get write-index [k v]))
                                                              (update-in invalid-reads [k v] set/union nodes)

                                                              ; valid read
                                                              :else
                                                              invalid-reads)))
                                                        invalid-reads)))
                                         (sorted-map)))]
        (cond-> {:valid? true}
          (seq missing-nodes)
          (assoc :valid? false
                 :missing-nodes missing-nodes)

          (seq divergent-reads)
          (assoc :valid? false
                 :divergent-reads divergent-reads)

          (seq invalid-reads)
          (assoc :valid? false
                 :invalid-reads invalid-reads))))))
