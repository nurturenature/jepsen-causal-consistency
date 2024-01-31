(ns causal.checker.fairness
  (:require [clojure.set :as set]
            [jepsen
             [checker :as checker]
             [history :as h]
             [store :as store]
             [util :as u]]
            [jepsen.checker.perf :as perf]
            [jepsen.history.fold :as f]
            [slingshot.slingshot :refer [try+ throw+]]
            [tesser.core :as t]))

(defn fs->points
  "Given a sequence of :f's, yields a map of f -> gnuplot-point-type, so we can
  render each function in a different style."
  [fs]
  (->> fs
       (map-indexed (fn [i f] [f (* 2 (+ 2 i))]))
       (into {})))

(def types
  "What types are we rendering?"
  [:ok :info :fail])

(def type->color
  "Takes a type of operation (e.g. :ok) and returns a gnuplot color."
  {:ok   ['rgb "#81BFFC"]
   :info ['rgb "#FFA400"]
   :fail ['rgb "#FF1E90"]})

(defn rate-preamble
  "Gnuplot commands for setting up a rate plot."
  [test output-path]
  (concat (perf/preamble output-path)
          [[:set :title (str (:name test) " rate")]]
          '[[set ylabel "Throughput (hz)"]]))

(defn rate-graph!
  "Writes a plot of operation rate by their completion times."
  [test history {:keys [subdirectory nemeses]}]
  (let [nemeses     (or nemeses (:nemeses (:plot test)))
        dt          10
        td          (double (/ dt))
        ; Times might technically be out-of-order (and our tests do this
        ; intentionally, just for convenience)
        t-max       (h/task history :max-time []
                            (let [t (->> (t/map :time)
                                         (t/max)
                                         (h/tesser history))]
                              (u/nanos->secs (or t 0))))
        ; Compute rates: a map of f -> type -> time-bucket -> rate
        datasets
        (h/fold
         (->> history
              (h/remove h/invoke?)
              h/client-ops)
         (f/loopf {:name :rate-graph}
                   ; We work with a flat map for speed, and nest it at
                   ; the end
                  ([m (transient {})]
                   [^Op op]
                   (recur (let [bucket (perf/bucket-time dt (u/nanos->secs
                                                             (.time op)))
                                k [(.f op) (.type op) bucket]]
                            (assoc! m k (+ (get m k 0) td))))
                   (persistent! m))
                   ; Combiner: merge, then furl
                  ([m {}]
                   [m2]
                   (recur (merge-with + m m2))
                   (reduce (fn unfurl [nested [ks rate]]
                             (assoc-in nested ks rate))
                           {}
                           m))))
        fs          (u/polysort (keys datasets))
        fs->points- (fs->points fs)
        output-path (.getCanonicalPath
                     (store/path! test subdirectory "rate.png"))
        preamble (rate-preamble test output-path)
        series   (for [f fs, t types]
                   {:title     (str (u/name+ f) " " (name t))
                    :with      'linespoints
                    :linetype  (type->color t)
                    :pointtype (fs->points- f)
                    :data      (let [m (get-in datasets [f t])]
                                 (map (juxt identity #(get m % 0))
                                      (perf/buckets dt @t-max)))})]
    (-> {:preamble  preamble
         :series    series}
        (perf/with-range)
        (perf/with-nemeses history nemeses)
        perf/plot!
        (try+ (catch [:type ::no-points] _ :no-points)))))

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

(defn fairness
  "Plot the rate of reads of each node's writes,
   e.g. are each nodes writes fairly represented, being read by other nodes?"
  []
  (reify checker/Checker
    (check [_this {:keys [nodes] :as _test} history _opts]
      (let [nodes        (->> nodes (into (sorted-set)))
            history      (->> history
                              h/client-ops
                              h/oks)
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
