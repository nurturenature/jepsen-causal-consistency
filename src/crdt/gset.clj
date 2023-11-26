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

(defn gset-ryw-anomaly-history
  "Generate a sample history of gset ops with a read-your-writes anomaly."
  []
  (->> [{:process 0
         :type :ok
         :f :a
         :value 0}
        {:process 0
         :type :ok
         :f :r
         :value #{}}]
       (h/history)))

(defrecord RYWExplainer []
  ec/DataExplainer
  (explain-pair-data [_ a b]
    (when (and (= (.process a) (.process b))
               ((h/has-f? :r) a)
               ((h/has-f? :a) b)
               (not (contains? (.value a) (.value b))))
      {:type    :read-your-writes
       :process (.process a)
       :read    (.value a)
       :add     (.value b)}))

  (render-explanation [_ {:keys [process read add]} a-name b-name]
    (str "In process " process " " a-name " read of " (pr-str read)
         " did not observe " b-name " add of " (pr-str add))))

(defn ryw-graph
  "Given a history, builds a graph of rw-antidependency relationships within each process.
   A ryw anomaly will cycle rw-antidependency and process order graphs."
  [history]
  (let [history (->> history
                     (h/client-ops)
                     (h/oks))
        adds  (->> history (h/filter-f :a))
        reads (->> history (h/filter-f :r))
        fused_folds (->> [(f/make-fold {:name :adds-by-process
                                        :reducer-identity (constantly {})
                                        :reducer (fn [acc op]
                                                   (let [process (.process op)
                                                         value (.value op)]
                                                     (update acc process
                                                             (fn [old value]
                                                               (if (nil? old)
                                                                 #{value}
                                                                 (conj old value)))
                                                             value)))
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
                             (h/fold fused_folds))
        graph (-> reads
                  (h/fold (f/make-fold {:name :read-your-writes
                                        :reducer-identity (fn []
                                                            (g/linear (g/named-graph :read-your-writes (g/digraph)))) ; TODO: op-digraph
                                        :reducer (fn [g op]
                                                   (let [process (.process op)
                                                         value   (.value op)
                                                         unread  (set/difference (get adds-by-process process) value)
                                                         first-unread (->> unread
                                                                           (apply min) ; add values are monotonic per process
                                                                           (get add-by-value))]
                                                     (g/link g op first-unread)))
                                        :post-reducer g/forked
                                        :combiner-identity (fn []
                                                             (g/linear (g/named-graph :read-your-writes (g/digraph)))) ; TODO: op-digraph
                                        :combiner (fn [acc chunk-result]
                                                    (g/named-graph-union acc chunk-result))
                                        :post-combiner g/forked})))]

    {:graph     graph
     :explainer (RYWExplainer.)}))

(defn ryw-check
  "Checks a history for read your writes anomalies."
  [history]
  (->> history
       (ec/check {:analyzer (ec/combine ryw-graph ec/process-graph)
                  :directory "./target/out"})))
