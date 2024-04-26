(ns causal.lww-list-append.checker.graph
  "Graph functions to support checking LWW list-append for causal consistency."
  (:require [bifurcan-clj
             [core :as b]
             [graph :as bg]]
            [clojure.set :as set]
            [elle
             [core :as elle]
             [txn :as ct]
             [graph :as g]
             [rels :refer [ww wr rw]]
             [util :as util :refer [index-of]]]
            [jepsen.history :as h]
            [slingshot.slingshot :refer [throw+]])
  (:import (clojure.lang MapEntry)))

(defn read-mop-kvs
  "Given a read mop, returns a #{[k v]} for all read values including nil and prefixes."
  [[f k v :as mop]]
  (let [_ (assert (= f :r) (str "not a read mop: " mop))
        v (or v [nil])] ; account for [:r k nil]
    (->> v
         (reduce (fn [r-kvs v']
                   (set/union r-kvs #{[k v']}))
                 nil))))

(defn read-kvs-txn
  "Given a transaction, returns a #{[k v]} for all read values including prefixes and intermediate reads."
  [txn]
  (->> txn
       (reduce (fn [r-kvs [f _k _v :as mop]]
                 (case f
                   :append r-kvs
                   :r      (set/union r-kvs (read-mop-kvs mop))))
               nil)))

(defn kv-read-mops-txn
  "Given a transaction, returns a seq of the mops that read [k v]."
  [txn [k _v :as kv]]
  (->> txn
       (filter (fn [[m-f m-k _m-v :as mop]]
                 (case m-f
                   :append false
                   :r (let [mop-kvs (read-mop-kvs mop)]
                        (and (= m-k k)
                             (contains? mop-kvs kv))))))))

(defn read-index
  "Given a history, returns a
   ```
   {[k v] #{ops}}
   ```
   for all reads of [k v], including
     - full read prefix v's
     - intermediate reads in transaction
   Including read prefixes and intermediate reads can be used for causal ordering."
  [history-oks]
  (->> history-oks
       ct/op-mops
       (reduce (fn [index [op [f _k _v :as mop]]]
                 (case f
                   :append index
                   :r (let [mop-kvs (read-mop-kvs mop)]
                        (->> mop-kvs
                             (reduce (fn [index kv]
                                       (update index kv set/union #{op}))
                                     index)))))
               nil)))

(defn write-kvs-txn
  "Given a transaction, returns a #{[k v]} for all write values including intermediate writes."
  [txn]
  (->> txn
       (reduce (fn [w-kv [f k v]]
                 (case f
                   :r w-kv
                   :append (set/union w-kv #{[k v]})))
               nil)))

(defn write-index
  "Given a history, returns a
   ```
   {[k v] op}  ; writes are asserted to be unique
   ```
   for all writes, including intermediate writes.
   Intermediate writes will be observed in the prefix of reads
   so can be used for causal ordering."
  [history-oks]
  (->> history-oks
       ct/op-mops
       (reduce (fn [index [op [f k v :as mop]]]
                 (case f
                   :r index
                   :append (if-let [existing (get index [k v])]
                             (throw+ {:type     :duplicate-write
                                      :op       op
                                      :mop      mop
                                      :existing existing})
                             (assoc index [k v] op))))
               nil)))

(defn ext-writes
  "Given a transaction, returns a
   ```
   {k [v]}
   ```
   for all external writes of [k v]
     - does include intermediate writes, i.e. what txn is writing to any existing prefix"
  [txn]
  (->> txn
       (reduce (fn [ext-writes [f k v :as _mop]]
                 (case f
                   :r ext-writes
                   :append (update ext-writes k (fn [cur-v]
                                                  (if (nil? cur-v)
                                                    [v]
                                                    (conj cur-v v))))))
               nil)))

(defn ext-reads
  "Given a transaction, returns a
   ```
   {k [v]}
   ```
   for all final external reads of [k v]
     - reads preceding a write are blinded by the write
       - don't know version write interacted with
       - process pov may have been replicated into between mops
     - reads following a write read internal state and are ignored
     - includes nil reads
     - does not include intermediate reads"
  [txn]
  (let [[ext-reads
         _int-writes]  (->> txn
                            (reduce (fn [[ext-reads int-writes] [f k v :as _mop]]
                                      (case f
                                        :append
                                        [(dissoc ext-reads k) ; blinded by the write
                                         (assoc int-writes k true)]

                                        :r
                                        [; ignore reads of internal writes in a txn
                                         (if (get int-writes k)
                                           ext-reads
                                           (assoc ext-reads k v))
                                         int-writes]))
                                    [nil nil]))]
    ext-reads))

(defn ext-read-index
  "Given a history, returns a
   ```
   {[k v] #{ops}}
   ```
   for all final external reads of [k v]
     - includes read prefix v's
     - does not include intermediate reads/writes"
  [history]
  (->> history
       (reduce (fn [ext-read-index {:keys [value] :as op}]
                 (let [ext-reads (ext-reads value)]
                   (->> ext-reads
                        (reduce (fn [ext-read-index [k vs]]
                                  (->> vs ; all vs including prefix
                                       (reduce (fn [ext-read-index v]
                                                 (update ext-read-index [k v] set/union #{op}))
                                               ext-read-index)))
                                ext-read-index))))
               nil)))

(defn init-nil-vg
  "Given a history, returns a version graph that captures [k nil] <hb [k v]
   for the first observation of k in each process."
  [history]
  (let [[g _observed]
        (->> history
             (reduce (fn [[g observed] {:keys [process value] :as _op}]
                       (->> value
                            ext-reads
                            (reduce (fn [[g observed] [k vs]]
                                      (let [observed-v (get-in observed [process k])
                                            this-v     (first vs)]
                                        (if (or observed-v
                                                (nil? this-v))
                                          [g observed]
                                          [(g/link g [k nil] [k this-v])
                                           (assoc-in observed [process k] this-v)])))
                                    [g observed])))
                     [(b/linear (g/digraph)) nil]))]
    (b/forked g)))

(defn read-prefix-vg
  "Given a history, returns a version graph that captures read prefix ordering, [k v] <hb [k v.next].
   All read mops in a transaction are considered."
  [history]
  (let [g (->> history
               ct/op-mops
               (reduce (fn [g [_op [f k vs :as _mop]]]
                         (case f
                           :append
                           g

                           :r
                           (->> vs
                                (partition 2 1)
                                (reduce (fn [g [from-v to-v]]
                                          (g/link g [k from-v] [k to-v]))
                                        g))))
                       (b/linear (g/digraph))))]
    (b/forked g)))

(defn monotonic-writes-vg
  "Given a history, returns a version graph with monotonic writes ordering.
   Monotonic writes are per process.
   We go through the history mop by mop to get the most explicit graph we can,
   i.e. this specific [k v] <hb [k' v'] vs this set #{[k v]} <hb #{[k' v']}. 
   
   As causal is multi-object, i.e. multiple keys, so is our graph:
   ```
   [:append :x 1]
   [:append :y 2]

   [:x 1] <hb [:y 2]
   ```"
  [history]
  (let [[g _observed] ; {process [k v]}
        (->> history
             ct/op-mops
             (reduce (fn [[g observed] [{:keys [process] :as _op} [f k v :as _mop]]]
                       (case f
                         :r
                         [g observed]

                         :append
                         (let [this-kv     [k v]
                               observed-kv (get observed process [k nil])  ; no prev is writing after nil
                               observed'   (assoc observed process this-kv)]
                           [(g/link g observed-kv this-kv)
                            observed'])))
                     [(b/linear (g/digraph)) nil]))]
    (b/forked g)))

(defn writes-follow-reads-vg
  "Given a history, returns a version graph with writes follows reads ordering.
   Writes follows reads are per process.
   We go through the history mop by mop to get the most explicit graph we can,
   i.e. intra-transaction, transaction edges, more specific [k v] <hb [k' v'] vs this set #{[k v]} <hb #{[k' v']}. 

   As causal is multi-object, i.e. multiple keys, so is our graph:
   ```
   [:r :x 1]
   [:append :y 2]

   [:x 1] <hb [:y 2]
   ```"
  [history]
  (let [[g _observed]  ; {process #{[k v]}}
        (->> history
             ct/op-mops
             (reduce (fn [[g observed] [{:keys [process] :as _op} [f k v :as mop]]]
                       (case f
                         :r
                         (let [read-kvs (read-mop-kvs mop)]
                           [g (update observed process set/union read-kvs)])

                         :append
                         (let [read-kvs (get observed process)
                               write-kv [k v]]
                           [(g/link-all-to g read-kvs write-kv)
                            (dissoc observed process)])))
                     [(b/linear (g/digraph)) nil]))]
    (b/forked g)))

;; TODO: could be smarter, e.g. use common prefix to determine new kvs,
;;       last/first v as already have prefix order
(defn monotonic-reads-vg
  "Given a history, returns a version graph with monotonic reads ordering.
   Monotonic reads are per process, per key.
   We go through the history mop by mop to get the most explicit graph we can,
   e.g. will help create cycles for intra-transaction non-monotonic reads, read your writes, etc. 

   ```
   [:r :x 1]
   [:r :y 1]
   [:r :x 2]

   [:x 1] <hb [:x 2]
   [:x 1] ?hb [:y 1] and [:y 1] ?hb [:x 2]
   ```"
  [history]
  (let [[g _observed]  ; {process {k [k v]}}
        (->> history
             ct/op-mops
             (reduce (fn [[g observed] [{:keys [process] :as _op} [f k v :as _mop]]]
                       (case f
                         ; observe our writes for versioning
                         :append
                         [g (assoc-in observed [process k] [k v])]

                         :r
                         (let [this-kv     [k (last v)]  ; no prefix
                               observed-kv (get-in observed [process k] [k nil])]
                           [(if (not= this-kv observed-kv)
                              (g/link g observed-kv this-kv)
                              g)
                            (assoc-in observed [process k] this-kv)])))
                     [(b/linear (g/digraph)) nil]))]
    (b/forked g)))

(def observed-vg-sources
  "A map of sources to build an observed version graph."
  {:init-nil            init-nil-vg
   :read-prefix         read-prefix-vg
   :monotonic-writes    monotonic-writes-vg
   :writes-follow-reads writes-follow-reads-vg
   :monotonic-reads     monotonic-reads-vg})

(def causal-vg-sources
  "A set of sources to build a causal version graph.
   See README.md for discussion of why monotonic reads are not included."
  (dissoc observed-vg-sources :monotonic-reads))

(defn causal-kvg
  "Given a multi-object causal version graph, returns a
   `{k vg}` containing each k in the version graph."
  [vg]
  (let [all-keys (->> vg
                      bg/vertices
                      (map first)
                      (into #{}))]
    (->> all-keys
         (map (fn [k]
                (let [kvg (g/collapse-graph (fn [[vert-k _vert-v]] (= k vert-k)) vg)]
                  (MapEntry. k kvg))))
         (into {}))))

(defn causal-vg
  "Given a map of sources, {name fn}, and a history, returns
   ```
   {:causal-vg       multi-object-version-graph
    :causal-kvg      single-object-by-k-version-graph-map 
    :sources         #{version-sources-used-to-build-graph}
    :cyclic-versions sequence-of-versions-with-cycles}
   ```
   Version graph is multi-object, e.g. across keys 
   If any version source graph has cycles, or would create cycles if combined with other sources,
   it is not combined into the final graph and its cycles are reported."
  [vg-sources history-oks]
  (let [[vg sources cyclic-versions]
        (->> vg-sources
             (reduce (fn [[vg sources cyclic-versions] [next-source next-grapher]]
                       ; check this version graph individually before combining
                       (let [next-vg   (next-grapher history-oks)
                             next-sccs (g/strongly-connected-components next-vg)]
                         (if (seq next-sccs)
                           [vg sources (conj cyclic-versions {:sources #{next-source}
                                                              :sccs    next-sccs})]
                           ; now try combining
                           (let [combined-sources (conj sources next-source)
                                 combined-vg      (g/digraph-union vg next-vg)
                                 combined-sccs    (g/strongly-connected-components combined-vg)]
                             (if (seq combined-sccs)
                               [vg sources (conj cyclic-versions {:sources combined-sources
                                                                  :sccs    combined-sccs})]
                               [combined-vg combined-sources cyclic-versions])))))
                     [(b/linear (g/digraph)) #{} nil]))

        vg  (b/forked vg)
        kvg (causal-kvg vg)]

    (cond-> {:causal-vg  vg
             :causal-kvg kvg}
      (seq sources)
      (assoc :sources sources)

      (seq cyclic-versions)
      (assoc :cyclic-versions cyclic-versions))))

(defrecord WWExplainer [causal-vg]
  elle/DataExplainer
  (explain-pair-data [_ a b]
    (let [writes   (write-kvs-txn (:value a))
          writes'  (write-kvs-txn  (:value b))
          edges    (->> causal-vg
                        bg/edges
                        (filter (fn [edge]
                                  (and (contains? writes  (.from edge))
                                       (contains? writes' (.to   edge))))))]
      (when (seq edges)
        (let [edge    (first edges)
              [k  v]  (.from edge)
              [k' v'] (.to   edge)]
          {:type  :ww
           :kv    [k v]
           :kv'   [k' v']
           :a-mop-index (index-of (:value a) [:append k v])
           :b-mop-index (index-of (:value b) [:append k' v'])}))))

  (render-explanation [_ {:keys [kv kv']} a-name b-name]
    (str a-name " wrote " (pr-str kv) " which <hb in causal version order " (pr-str kv')
         ", which was written by " b-name)))

(defn ww-tg
  "Given a causal version graph, a write index, and an unused history to match calling API, returns
   ```
   {:graph     ww-transaction-graph
    :explainer ww-explainer
    :anomalies seq-of-anomalies}
   ```
   with write of [k v] <hb write of [k v]' based on version ordering."
  [{:keys [causal-vg write-index] :as _indexes} _history]
  (let [; version graph will have [k nil] versions, e.g. top vertices, but nobody ever writes nil
        causal-vg      (->> causal-vg
                            (g/collapse-graph (fn [[_k v :as _vertex]] v)))

        ; [:append k v] <hb [:append k' v'] in version order
        [tg anomalies] (->> causal-vg
                            bg/edges
                            (reduce (fn [[tg anomalies] edge]
                                      (let [[from-k from-v
                                             :as from-kv] (bg/edge-from edge)
                                            from-op       (get write-index from-kv)
                                            [to-k to-v
                                             :as to-kv]   (bg/edge-to edge)
                                            to-op         (get write-index to-kv)]
                                        (cond
                                          ; garbage version(s), garbage read, corrupted write, etc. in vg
                                          (or (nil? from-op)
                                              (nil? to-op))
                                          [tg (conj anomalies (MapEntry. :garbage-versions
                                                                         {:type :ww
                                                                          :op   from-op
                                                                          :kv   from-kv
                                                                          :op'  to-op
                                                                          :kv'  to-kv}))]

                                          ; don't self link
                                          (= (:index from-op)
                                             (:index to-op))
                                          ; confirm write order
                                          (if (< (index-of (:value from-op) [:append from-k from-v])
                                                 (index-of (:value to-op)   [:append to-k   to-v]))
                                            [tg anomalies]
                                            [tg (conj anomalies (MapEntry. :cyclic-versions
                                                                           {:type :ww
                                                                            :op   from-op
                                                                            :kv   from-kv
                                                                            :kv'  to-kv}))])

                                          :else
                                          [(g/link tg from-op to-op ww) anomalies])))
                                    [(b/linear (g/op-digraph)) nil]))]
    (merge {:graph     (b/forked tg)
            :explainer (WWExplainer. causal-vg)}
           (when (seq anomalies)
             {:anomalies anomalies}))))

(defrecord WRExplainer []
  elle/DataExplainer
  (explain-pair-data [_ a b]
    (let [writes          (write-kvs-txn (:value a))
          reads           (read-kvs-txn  (:value b))
          [k v
           :as shared-kv] (first (set/intersection writes reads))
          r-mop           (first (kv-read-mops-txn (:value b) shared-kv))]
      (when r-mop
        {:type  :wr
         :key   k
         :value v
         :a-mop-index (index-of (:value a) [:append k v])
         :b-mop-index (index-of (:value b) r-mop)})))

  (render-explanation [_ {:keys [key value]} a-name b-name]
    (str a-name " wrote " (pr-str key) " = " (pr-str value)
         ", which was read by " b-name)))

(defn wr-tg
  "Given write and read indexes, and an unused history to match the calling APIs, returns
   ```
   {:graph     wr-transaction-graph
    :explainer wr-explainer
    :anomalies seq-of-anomalies}
   ```
   with write of [k v] <hb reads of [k v] ordering.
   A wr edge is only created for the first read in each process that observes the write
   as succeeding reads are transitively included by process order."
  [{:keys [write-index read-index] :as _indexes} _history]
  (let [[tg anomalies] (->> write-index
                            (reduce (fn [[tg anomalies] [w-kv w-op]]
                                      (let [r-ops (get read-index w-kv)
                                            ; don't self link, TODO: insure mop order checked in internal
                                            r-ops (disj r-ops w-op)
                                            ; first op in each process
                                            r-ops (->> r-ops
                                                       (group-by :process)
                                                       vals
                                                       (map (fn [ops]
                                                              (->> ops (sort-by :index) first))))]
                                        [(g/link-to-all tg w-op r-ops wr) anomalies]))
                                    [(b/linear (g/op-digraph)) nil]))]
    (cond-> {:graph     (b/forked tg)
             :explainer (WRExplainer.)}
      (seq anomalies)
      (assoc :anomalies anomalies))))

(defrecord RWExplainer [causal-kvg]
  elle/DataExplainer
  (explain-pair-data [_ a b]
    (let [a-txn     (:value a)
          b-txn     (:value b)
          read-kvs  (read-kvs-txn  a-txn)
          write-kvs (write-kvs-txn b-txn)]
      (->> read-kvs
           (reduce (fn [_ [r-k _r-v :as read-kv]]
                     (let [next-kvs       (->> (g/out (get causal-kvg r-k) read-kv)
                                               (into #{})) ; convert from bifurcan-clj Set
                           [w-k w-v
                            :as write-kv] (first (set/intersection write-kvs next-kvs))]
                       (when write-kv
                         (reduced {:type        :rw
                                   :read-kv     read-kv
                                   :write-kv    write-kv
                                   :a-mop-index (index-of a-txn (first (kv-read-mops-txn a-txn read-kv)))
                                   :b-mop-index (index-of b-txn [:append w-k w-v])}))))
                   nil))))

  (render-explanation [_ {:keys [read-kv write-kv]} a-name b-name]
    (str a-name " read version " (pr-str read-kv)
         " which <hb the version " (pr-str write-kv)
         " that " b-name " wrote")))

(defn rw-tg
  "Given a single object causal version graph, write index, history, and a process, returns
   ```
   {:graph     rw-transaction-graph
    :explainer rw-explainer
    :anomalies seq-of-anomalies}
   ```
   with read of [k v] <hb write of [k v]' based on version ordering.
   
   Important: can only check inferred rw relations per individual process!"
  [{:keys [causal-kvg write-index rw-process] :as _indexes} history]
  (let [history (->> history
                     (h/filter (fn [{:keys [process]}] (= process rw-process))))

        [tg anomalies]
        (->> history
             (reduce (fn [[tg anomalies] {:keys [value] :as op}]
                       (->> value
                            ext-reads
                            (reduce (fn [[tg anomalies] [k vs]]
                                      (let [this-kv  [k (last vs)]
                                            next-kvs (g/out (get causal-kvg k) this-kv)]
                                        (->> next-kvs
                                             (reduce (fn [[tg anomalies] next-kv]
                                                       [(g/link tg op (get write-index next-kv) rw) anomalies])
                                                     [tg anomalies]))))
                                    [tg anomalies])))
                     [(b/linear (g/op-digraph)) nil]))
        tg (b/forked tg)]

    (cond-> {:graph     tg
             :explainer (RWExplainer. causal-kvg)}
      (seq anomalies)
      (assoc :anomalies anomalies))))


