(ns causal.checker.fairness
  (:require [elle.rw-register :as rw]
            [jepsen
             [checker :as checker]
             [history :as h]
             [store :as store]
             [txn :as txn]
             [util :as u]]
            [jepsen.checker.perf :as perf]
            [jepsen.history.fold :as f]
            [slingshot.slingshot :refer [try+]]
            [tesser.core :as t])
  (:import (jepsen.history Op)))

(defn node->color
  "Given a sequence of nodes, yields a map of node->color using gnuplot colors."
  [nodes]
  (->> nodes
       (map (fn [color type]
              [type ['rgb color]])
            (cycle ["red" "violet" "green" "brown"
                    "orange" "blue" "pink"
                    "gold" "grey"
                    "#F0DC82" ; buff
                    ]))
       (into (sorted-map))))

(defn nodes->points
  "Given a sequence of nodes, yields a map of node -> gnuplot-point-type, so we can
  render each function in a different style."
  [nodes]
  (->> nodes
       (map-indexed (fn [i f] [f (* 2 (+ 2 i))]))
       (into {})))

(defn rate-preamble
  "Gnuplot commands for setting up a rate plot."
  [test output-path]
  (concat (perf/preamble output-path)
          [[:set :title (str (:name test) " fairness")]]
          '[[set ylabel "Observed Writes (Hz)"]]))

(defn writes-read-by-node-rate-graph!
  "Writes a plot of operation rate by their completion times."
  [{:keys [nodes] :as test} history {:keys [subdirectory nemeses]}]
  (let [history'    (->> history
                         h/client-ops
                         h/oks
                         (h/remove :final-read?))
        nodes       (into (sorted-set) nodes)
        nemeses     (or nemeses (:nemeses (:plot test)))
        dt          1
        td          (double (/ dt))
        ; Times might technically be out-of-order (and our tests do this
        ; intentionally, just for convenience)
        t-max       (h/task history :max-time []
                            (let [t (->> (t/map :time)
                                         (t/max)
                                         (h/tesser history))]
                              (u/nanos->secs (or t 0))))
        ext-index    (h/task history' :ext-index []
                             (rw/ext-index txn/ext-writes history'))
        ; Compute rates: a map of f -> type -> time-bucket -> rate
        datasets
        (h/fold
         history'
         (f/loopf {:name :fairness-graph}
                   ; We work with a flat map for speed, and nest it at
                   ; the end
                  ([m (transient {})]
                   [^Op op]
                   (recur (let [bucket (perf/bucket-time dt (u/nanos->secs
                                                             (.time op)))]
                            (reduce (fn [acc [f k v]]
                                      (if (and (= :r f)
                                               v)
                                        (let [node (->> (get-in @ext-index [k v])
                                                        first
                                                        :node)
                                              k [(.f op) node bucket]]
                                          (assoc! acc k (+ (get acc k 0) td)))
                                        acc))
                                    m
                                    (:value op))))
                   (persistent! m))
                   ; Combiner: merge, then furl
                  ([m {}]
                   [m2]
                   (recur (merge-with + m m2))
                   (reduce (fn unfurl [nested [ks rate]]
                             (assoc-in nested ks rate))
                           {}
                           m))))
        fs             (u/polysort (keys datasets))
        nodes->points- (nodes->points nodes)
        output-path (.getCanonicalPath
                     (store/path! test subdirectory "fairness.png"))
        preamble (rate-preamble test output-path)
        series   (for [f fs, node nodes]
                   {:title     (str "write from " (name node))
                    :with      'linespoints
                    :linetype  ((node->color nodes) node)
                    :pointtype (nodes->points- node)
                    :data      (let [m (get-in datasets [f node])]
                                 (map (juxt identity #(get m % 0))
                                      (perf/buckets dt @t-max)))})]
    (-> {:preamble  preamble
         :series    series}
        (perf/with-range)
        (perf/with-nemeses history nemeses)
        perf/plot!
        (try+ (catch [:type ::no-points] _ :no-points)))))

(defn fairness
  "Plot the rate of reads of each node's writes,
   e.g. are each nodes writes fairly represented, being read by itself and other nodes?
   Count all reads of writes."
  []
  (reify checker/Checker
    (check [_this {:keys [nodes] :as test} history opts]
      (let [history'  (->> history
                           h/client-ops
                           h/oks)
            ext-write-index (rw/ext-index txn/ext-writes history')
            reads-of-writes (->> history'
                                 (reduce (fn [acc op]
                                           (reduce (fn [acc [f k v]]
                                                     (if (and (= :r f)
                                                              v)
                                                       (let [node (->> (get-in ext-write-index [k v])
                                                                       first
                                                                       :node)
                                                             node (or node (:node op))] ; may be an txn internal w/r
                                                         (assoc acc node (+ (get acc node 0) 1)))
                                                       acc))
                                                   acc
                                                   (:value op)))
                                         (sorted-map)))]
        (writes-read-by-node-rate-graph! test history opts)
        (merge
         {:valid? true}
         {:reads-of-writes reads-of-writes})))))
