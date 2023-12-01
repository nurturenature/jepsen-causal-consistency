(ns crdt.gset
  (:require [clojure.set :as set]
            [clojure.tools.logging :refer [info]]
            [elle
             [core :as ec]
             [graph :as g]]
            [jepsen.history :as h]
            [jepsen.history.fold :as f]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn gset-history
  "Generate a sample history of gset ops.
   Optional opts:
   ```
   {:num-processes integer
    :num-elements  integer}
   ```"
  ([] (gset-history {}))
  ([opts]
   (let [num-processes (:num-processes opts 5)
         num-elements  (:num-elements  opts 1000)]
     (->> num-elements
          (range)
          (mapcat (fn [v]
                    [{:process (rand-int num-processes)
                      :type :ok
                      :f :a
                      :value v}
                     {:process (rand-int num-processes)
                      :type :ok
                      :f :r
                      :value (set (range (+ 1 v)))}]))
          (h/history)))))

(def gset-ryw-history
  "Generate a sample history of gset ops with read-your-writes."
  (->> [{:process 0 :type :ok :f :a :value 0}
        {:process 0 :type :ok :f :r :value #{0}}]
       (h/history)))

(def gset-ryw-anomaly-history
  "Generate a sample history of gset ops with a read-your-writes anomaly."
  (->> [{:process 0 :type :ok :f :a :value 0}
        {:process 0 :type :ok :f :r :value #{}}]
       (h/history)))

(def gset-mono-w-history
  "Generate a sample history of gset ops with monotonic-writes."
  (->> [{:process 0 :type :ok :f :a :value 0}
        {:process 0 :type :ok :f :a :value 1}
        {:process 1 :type :ok :f :r :value #{0}}
        {:process 1 :type :ok :f :r :value #{0 1}}]
       (h/history)))

(def gset-mono-w-anomaly-history
  "Generate a sample history of gset ops with a monotonic-writes anomaly."
  (->> [{:process 0 :type :ok :f :a :value 0}
        {:process 0 :type :ok :f :a :value 1}
        {:process 1 :type :ok :f :r :value #{1}}
        {:process 1 :type :ok :f :r :value #{0 1}}]
       (h/history)))

(def gset-mono-r-history
  "Generate a sample history of gset ops with monotonic-reads."
  (->> [{:process 0 :type :ok :f :a :value 0}
        {:process 1 :type :ok :f :a :value 1}
        {:process 2 :type :ok :f :a :value 2}
        {:process 3 :type :ok :f :r :value #{1}}
        {:process 3 :type :ok :f :r :value #{0 1}}
        {:process 3 :type :ok :f :r :value #{0 1 2}}]
       (h/history)))

(def gset-mono-r-anomaly-history
  "Generate a sample history of gset ops with a monotonic-reads anomaly."
  (->> [{:process 0 :type :ok :f :a :value 0}
        {:process 2 :type :ok :f :r :value #{0}}
        {:process 2 :type :ok :f :r :value #{}}]
       (h/history)))

(def gset-wfr-history
  "Generate a sample history of gset ops with writes-follow-reads."
  (->> [{:process 0 :type :ok :f :a :value 0}
        {:process 1 :type :ok :f :r :value #{0}}
        {:process 1 :type :ok :f :a :value 1}
        {:process 2 :type :ok :f :r :value #{0 1}}]
       (h/history)))

(def gset-wfr-anomaly-history
  "Generate a sample history of gset ops with a writes-follow-reads anomaly."
  (->> [{:process 0 :type :ok :f :a :value 0}
        {:process 1 :type :ok :f :r :value #{0}}
        {:process 1 :type :ok :f :a :value 1}
        {:process 2 :type :ok :f :r :value #{1}}]
       (h/history)))

(defn processes
  "Given a history, returns a set of all processes in the history."
  [history]
  (let [fold (f/make-fold {:name :processes
                           :reducer-identity (constantly #{})
                           :reducer (fn [acc op]
                                      (conj acc (.process op)))
                           :combiner-identity (constantly #{})
                           :combiner set/union})]
    (-> history
        (h/fold fold))))

(defn analyze-adds
  "Given a history, filters for adds and returns 
   ```
   [{:process :set-of-values-added}
    {:value :add-op}]
   ```"
  [history]
  (let [adds (->> history
                  (h/filter-f :a))
        fused_folds (->> [(f/make-fold {:name :adds-by-process
                                        :reducer-identity (constantly {})
                                        :reducer (fn [acc op]
                                                   (let [process (.process op)
                                                         value (.value op)]
                                                     (update acc process
                                                             (fn [old]
                                                               (if (nil? old)
                                                                 #{value}
                                                                 (conj old value))))))
                                        :combiner-identity (constantly {})
                                        :combiner (fn [acc chunk]
                                                    (merge-with set/union
                                                                acc chunk))})
                          (f/make-fold {:name :add-by-value
                                        :reducer-identity (constantly {})
                                        :reducer (fn [acc op]
                                                   (let [value (.value op)]
                                                     (assoc acc value op)))
                                        :combiner-identity (constantly {})
                                        :combiner merge})]
                         (apply f/fuse)
                         :fused)
        [adds-by-process
         add-by-value]   (-> adds
                             (h/fold fused_folds))]

    [adds-by-process
     add-by-value]))

(defn first-reads
  "Given a history, filters for reads and returns 
   ```
   {:value {:process :first-read}}
   ```"
  [history]
  (let [[_ first-reads]
        (->> history
             (h/filter-f :r)
             (reduce (fn [[seen first-reads] op]
                       (let [process (.process op)
                             value   (.value op)
                             news    (set/difference value (get seen process))
                             first-reads (->> news
                                              (reduce (fn [acc new]
                                                        (assoc-in acc [new process] op))
                                                      first-reads))
                             seen    (update seen process set/union news)]
                         [seen first-reads]))
                     [{} {}]))]
    first-reads))

(defn rw-anti-graph
  "For every read, link to the first add in each process that wasn't read, if any."
  [history [adds-by-process add-by-value]]
  (let [reads (->> history
                   (h/filter-f :r))]
    (-> reads
        (h/fold (f/make-fold {:name :rw-anti
                              :reducer-identity (fn []
                                                  (g/linear (g/named-graph :rw-anti (g/digraph)))) ; TODO: op-digraph
                              :reducer (fn [g op]
                                         ; link to the first add in each process that wasn't read by op
                                         (let [value (.value op)]
                                           (->> adds-by-process
                                                vals
                                                (reduce (fn [g adds]
                                                          (let [unread (set/difference adds value)]
                                                            (if (seq unread)
                                                              (let [first-unread (->> unread
                                                                                      (apply min) ; add values are monotonic per process
                                                                                      (get add-by-value))]
                                                                (g/link g op first-unread))
                                                              g)))
                                                        g))))
                              :post-reducer g/forked
                              :combiner-identity (fn []
                                                   (g/linear (g/named-graph :rw-anti (g/digraph)))) ; TODO: op-digraph
                              :combiner (fn [acc chunk-result]
                                          (g/named-graph-union acc chunk-result))
                              :post-combiner g/forked})))))

(defrecord RW-Anti-Explainer []
  ec/DataExplainer
  (explain-pair-data [_ a b]
    (when (and ((h/has-f? :r) a)
               ((h/has-f? :a) b)
               (not (contains? (.value a) (.value b))))
      {:type   :rw-anti
       :read-p (.process a)
       :add-p  (.process b)
       :read   (.value a)
       :add    (.value b)}))

  (render-explanation [_ {:keys [read-p add-p read add]} a-name b-name]
    (if (= read-p add-p)
      (str "In the same process " read-p " " a-name "'s read of " (pr-str read)
           " did not observe " b-name "'s add of " (pr-str add))
      (str a-name "'s read of " (pr-str read)
           " did not observe " b-name "'s add of " (pr-str add)))))

(defn rw-anti-analysis
  "Given a history, builds a graph of rw-anti(dependency) relationships."
  [history]
  (let [analyze-adds (h/task history analyze-adds [] (analyze-adds history))
        graph        (h/task history rw-anti-graph [analyzed-adds analyze-adds] (rw-anti-graph history analyzed-adds))]

    {:graph     @graph
     :explainer (RW-Anti-Explainer.)}))

(defn wr-causal-graph
  "For every add, link to the first read in each process that contains that add, if any."
  [history first-reads]
  (let [adds (->> history
                  (h/filter-f :a))]
    (-> adds
        (h/fold (f/make-fold {:name :wr-causal
                              :reducer-identity (fn []
                                                  (g/linear (g/named-graph :wr-causal (g/digraph)))) ; TODO: op-digraph
                              :reducer (fn [g op]
                                         ; link to the first read in each process that contains op's add
                                         (let [value (.value op)
                                               first-reads (->> value
                                                                (get first-reads)
                                                                vals)]
                                           (if (seq first-reads)
                                             (g/link-to-all g op first-reads)
                                             g)))
                              :post-reducer g/forked
                              :combiner-identity (fn []
                                                   (g/linear (g/named-graph :wr-causal (g/digraph)))) ; TODO: op-digraph
                              :combiner (fn [acc chunk-result]
                                          (g/named-graph-union acc chunk-result))
                              :post-combiner g/forked})))))

(defrecord WR-Causal-Explainer []
  ec/DataExplainer
  (explain-pair-data [_ a b]
    (when (and ((h/has-f? :a) a)
               ((h/has-f? :r) b)
               (contains? (.value b) (.value a)))
      {:type   :wr-causal
       :add    (.value a)
       :read   (.value b)}))

  (render-explanation [_ {:keys [add read]} a-name b-name]
    (str a-name "'s add of " (pr-str add)
         " was observed by " b-name "'s read of " (pr-str read))))

(defn wr-causal-analysis
  "Given a history, builds a graph of wr-causal relationships."
  [history]
  (let [first-reads (h/task history first-reads [] (first-reads history))
        graph       (h/task history wr-causal-graph [analyzed-reads first-reads] (wr-causal-graph history analyzed-reads))]

    {:graph     @graph
     :explainer (WR-Causal-Explainer.)}))

(defn rr-mono-graph
  "Within a process, order reads by monotonic values, if any."
  [history processes]
  (let [reads (->> history
                   (h/filter-f :r))]
    (->> processes
         (reduce (fn [g process]
                   (->> reads
                        (h/filter (comp #{process} :process))
                        (group-by :value)
                        (sort-by (fn [[v _ops]]
                                   (count v)))  ; TODO richer semantics, e.g. superset
                        (partition 2 1)
                        (reduce (fn [g [[_v1 ops1] [_v2 ops2]]]
                                  (g/link-all-to-all g ops1 ops2))
                                g)))
                 (g/linear (g/named-graph :rr-mono (g/digraph)))) ; TODO: op-digraph
         g/forked)))

(defrecord RR-Mono-Explainer []
  ec/DataExplainer
  (explain-pair-data [_ a b]
    (when (and (= (.process a) (.process b))
               ((h/has-f? :r) a)
               ((h/has-f? :r) b)
               (< (count (.value a)) (count (.value b))))
      {:type   :rr-mono
       :read    (.value a)
       :read'   (.value b)}))

  (render-explanation [_ {:keys [read read']} a-name b-name]
    (str a-name "'s read of " (pr-str read)
         " has less elements than " b-name "'s read of " (pr-str read'))))

(defn rr-mono-analysis
  "Given a history, builds a graph of rr-mono relationships."
  [history]
  (let [processes (h/task history processes [] (processes history))
        graph     (h/task history rr-mono-graph [ps processes] (rr-mono-graph history ps))]

    {:graph     @graph
     :explainer (RR-Mono-Explainer.)}))

(defn ww-wfr-order
  "For a process, return a writes-follow-reads graph."
  [history process processes [adds-by-process add-by-value]]
  (let [history (->> history
                     (h/filter (comp #{process} :process)))
        [g _] (->> history
                   (reduce (fn [[g seen] op]
                             (let [f     (.f op)
                                   value (.value op)]
                               (case f
                                 :r [g
                                     (set/union seen value)]
                                 :a [(->> processes
                                          (reduce (fn [g p]
                                                    (let [seen-by-p (set/intersection seen (get adds-by-process p))]
                                                      (if (seq seen-by-p)
                                                        (let [last-seen-add (->> seen-by-p
                                                                                 (apply max)
                                                                                 (get add-by-value))]
                                                          (g/link g last-seen-add op))
                                                        g)))
                                                  g))
                                     seen])))
                           [(g/linear (g/named-graph :ww-wfr (g/digraph))) ; TODO: op-digraph
                            #{}]))]
    (g/forked g)))

(defn ww-wfr-graph
  "Combined graph of each process's writes-follow-reads graph."
  [history processes [adds-by-process add-by-value]]
  (->> processes
       (map #(ww-wfr-order history % processes [adds-by-process add-by-value]))
       (reduce g/named-graph-union (g/linear (g/named-graph :ww-wfr (g/digraph))))
       g/forked))

(defrecord WW-WFR-Explainer []
  ec/DataExplainer
  (explain-pair-data [_ a b]
    (when (and ((h/has-f? :a) a)
               ((h/has-f? :a) b)) ; TODO: encode wfr
      {:type :ww-wfr
       :w    (.value a)
       :w'   (.value b)}))

  (render-explanation [_ {:keys [w w']} a-name b-name]
    (str a-name "'s add of " (pr-str w)
         " was observed by " b-name " before its add of " (pr-str w'))))

(defn ww-wfr-analysis
  "Given a history, builds a graph of ww-wfr relationships."
  [history]
  (let [processes    (h/task history processes [] (processes history))
        analyze-adds (h/task history analyze-adds [] (analyze-adds history))
        graph        (h/task history ww-wfr-graph [ps processes analyzed-adds analyze-adds] (ww-wfr-graph history ps analyzed-adds))]

    {:graph     @graph
     :explainer (WW-WFR-Explainer.)}))

(defn causal-check
  "Checks a history for causal consistency:
     - read your writes    anomalies cycle rw-anti and process orders
     - monotonic writes    anomalies cycle wr-causal, rw-anti, and process orders
     - monotonic reads     anomalies cycle rr-mono, or wr-causal, rw-anti, and process orders
     - writes follow reads anomalies cycle ww-wfr, or wr-causal, rw-anti, and process orders"
  [history]
  (->> history
       (h/client-ops)
       (h/oks)
       (ec/check {:analyzer (ec/combine rw-anti-analysis wr-causal-analysis rr-mono-analysis ww-wfr-analysis ec/process-graph)
                  :directory "./target/out"})))
