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
  "Given a history, returns `{:vg vg :k-vg k-vg :sccs sccs}`,
   a version graph that captures [k nil] <hb [k v] for the first observation of k in each process."
  [history]
  (let [[vg k-vg _observed] ; [vg {k vg} {process {k v}}]
        (->> history
             (reduce (fn [[vg k-vg observed] {:keys [process value] :as _op}]
                       (->> value
                            ext-reads
                            (reduce (fn [[vg k-vg observed] [k vs]]
                                      (let [this-v     (first vs)
                                            observed-v (get-in observed [process k])
                                            this-k-vg  (get k-vg k (b/linear (g/digraph)))]
                                        (if (or (nil? this-v)
                                                observed-v)
                                          [vg k-vg observed]
                                          [(g/link vg [k nil] [k this-v])
                                           (assoc k-vg k (g/link this-k-vg [k nil] [k this-v]))
                                           (assoc-in observed [process k] this-v)])))
                                    [vg k-vg observed])))
                     [(b/linear (g/digraph)) nil nil]))
        vg   (b/forked vg)
        k-vg (->> k-vg
                  (map (fn [[k vg]] (MapEntry. k (b/forked vg))))
                  (into {}))
        sccs (g/strongly-connected-components vg)]
    {:vg   vg
     :k-vg k-vg
     :sccs sccs}))

(defn read-prefix-vg
  "Given a history, returns `{:vg vg :k-vg k-vg :sccs sccs}`,
   a version graph that captures read prefix ordering, [k v] <hb [k v.next].
   All read mops in a transaction are considered."
  [history]
  (let [[vg k-vg] (->> history
                       ct/op-mops
                       (reduce (fn [[vg k-vg] [_op [f k vs :as _mop]]]
                                 (case f
                                   :append
                                   [vg k-vg]

                                   :r
                                   (->> vs
                                        (partition 2 1)
                                        (reduce (fn [[vg k-vg] [from-v to-v]]
                                                  (let [this-k-vg (get k-vg k (b/linear (g/digraph)))]
                                                    [(g/link vg [k from-v] [k to-v])
                                                     (assoc k-vg k (g/link this-k-vg [k from-v] [k to-v]))]))
                                                [vg k-vg]))))
                               [(b/linear (g/digraph)) nil]))
        vg   (b/forked vg)
        k-vg (->> k-vg
                  (map (fn [[k vg]] (MapEntry. k (b/forked vg))))
                  (into {}))
        sccs (g/strongly-connected-components vg)]
    {:vg   vg
     :k-vg k-vg
     :sccs sccs}))

(defn monotonic-writes-vg
  "Given a history, returns `{:vg vg :k-vg k-vg :sccs sccs}`,
   a version graph with monotonic writes ordering.
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
  (let [[vg k-vg _prev _prev-by-k] ; [vg {k vg} {process [k v]} {process {k v}}]
        (->> history
             ct/op-mops
             (reduce (fn [[vg k-vg prev prev-by-k] [{:keys [process] :as _op} [f k v :as _mop]]]
                       (case f
                         :r
                         [vg k-vg prev prev-by-k]

                         :append
                         (let [this-kv      [k v]
                               prev-kv      (get prev process [k nil])  ; no prev is writing after nil
                               prev'        (assoc prev process this-kv)
                               this-k-vg    (get k-vg k (b/linear (g/digraph)))
                               prev-by-k-kv (get-in prev-by-k [process k] [k nil])  ; no prev is writing after nil
                               prev-by-k'   (assoc-in prev-by-k [process k] this-kv)]
                           [(g/link vg prev-kv this-kv)
                            (assoc k-vg k (g/link this-k-vg prev-by-k-kv this-kv))
                            prev'
                            prev-by-k'])))
                     [(b/linear (g/digraph)) nil nil nil]))
        vg   (b/forked vg)
        k-vg (->> k-vg
                  (map (fn [[k vg]] (MapEntry. k (b/forked vg))))
                  (into {}))
        sccs (g/strongly-connected-components vg)]
    {:vg   vg
     :k-vg k-vg
     :sccs sccs}))

(defn writes-follow-reads-vg
  "Given a history, returns `{:vg vg :k-vg k-vg :sccs sccs}`,
   a version graph with writes follows reads ordering.
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
  (let [[vg k-vg _prev _prev-by-k]  ; [vg {k vg} {process #{[k v]}} {process {k #{[k v]}}}]
        (->> history
             ct/op-mops
             (reduce (fn [[vg k-vg prev prev-by-k] [{:keys [process] :as _op} [f k v :as mop]]]
                       (case f
                         :r
                         (let [read-kvs (read-mop-kvs mop)]
                           [vg k-vg
                            (update prev process set/union read-kvs)
                            (->> read-kvs
                                 (group-by first)
                                 (reduce (fn [prev-by-k [k kvs]]
                                           (update-in prev-by-k [process k] set/union kvs))
                                         prev-by-k))])

                         :append
                         (let [write-kv      [k v]
                               prev-kvs      (get prev process)
                               this-k-vg     (get k-vg k (b/linear (g/digraph)))
                               prev-by-k-kvs (get-in prev-by-k [process k])]
                           [(g/link-all-to vg prev-kvs write-kv)
                            (assoc k-vg k (g/link-all-to this-k-vg prev-by-k-kvs write-kv))
                            (dissoc prev process)
                            (update prev-by-k process dissoc k)])))
                     [(b/linear (g/digraph)) nil nil nil]))
        vg   (b/forked vg)
        k-vg (->> k-vg
                  (map (fn [[k vg]] (MapEntry. k (b/forked vg))))
                  (into {}))
        sccs (g/strongly-connected-components vg)]
    {:vg   vg
     :k-vg k-vg
     :sccs sccs}))

;; TODO: could be smarter, e.g. use common prefix to determine new kvs,
;;       last/first v as already have prefix order
(defn monotonic-reads-vg
  "Given a history, returns `{:vg vg :k-vg k-vg :sccs sccs}`,
   a version graph with monotonic reads ordering.
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
  (let [[vg k-vg _observed]  ; [vg {k vg} {process {k [k v]}}]
        (->> history
             ct/op-mops
             (reduce (fn [[vg k-vg observed] [{:keys [process] :as _op} [f k v :as _mop]]]
                       (case f
                         ; observe our writes for versioning
                         :append
                         [vg k-vg (assoc-in observed [process k] [k v])]

                         :r
                         (let [this-kv     [k (last v)]  ; no prefix
                               this-k-vg   (get k-vg k (b/linear (g/digraph)))
                               observed-kv (get-in observed [process k] [k nil])

                               [vg k-vg]   (if (not= this-kv observed-kv)
                                             [(g/link vg observed-kv this-kv)
                                              (assoc k-vg k (g/link this-k-vg observed-kv this-kv))]
                                             [vg k-vg])]
                           [vg k-vg (assoc-in observed [process k] this-kv)])))
                     [(b/linear (g/digraph)) nil nil]))
        vg   (b/forked vg)
        k-vg (->> k-vg
                  (map (fn [[k vg]] (MapEntry. k (b/forked vg))))
                  (into {}))
        sccs (g/strongly-connected-components vg)]
    {:vg   vg
     :k-vg k-vg
     :sccs sccs}))

(def observed-vg-sources
  "A map of sources to build an observed version graph."
  {:init-nil-vg            init-nil-vg
   :read-prefix-vg         read-prefix-vg
   :monotonic-writes-vg    monotonic-writes-vg
   :writes-follow-reads-vg writes-follow-reads-vg
   :monotonic-reads-vg     monotonic-reads-vg})

(def causal-vg-sources
  "A set of sources to build a causal version graph.
   See README.md for discussion of why monotonic reads are not included."
  (dissoc observed-vg-sources :monotonic-reads-vg))

(defn causal-vg
  "Given an index, a map of sources, {name fn}, and a history, returns
   ```
   {:causal-vg       multi-object-version-graph
    :causal-kvg      single-object-by-k-version-graph-map 
    :sources         #{version-sources-used-to-build-graph}
    :cyclic-versions sequence-of-versions-with-cycles}
   ```
   Version graph is multi-object, e.g. across keys 
   If any version source graph has cycles, or would create cycles if combined with other sources,
   it is not combined into the final graph and its cycles are reported."
  [indexes vg-sources history-oks]
  (let [[vg k-vg sources cyclic-versions]
        (->> vg-sources
             (reduce (fn [[vg k-vg sources cyclic-versions] [next-source next-grapher]]
                       ; check this version graph individually before combining
                       (let [{next-vg   :vg
                              next-k-vg :k-vg
                              next-sccs :sccs} (get indexes next-source
                                                    (next-grapher history-oks))]
                         (if (seq next-sccs)
                           [vg k-vg sources (conj cyclic-versions {:sources #{next-source}
                                                                   :sccs    next-sccs})]
                           ; now try combining
                           (let [combined-sources (conj sources next-source)
                                 combined-vg      (g/digraph-union vg next-vg)
                                 combined-k-vg    (merge-with g/digraph-union k-vg next-k-vg)
                                 combined-sccs    (g/strongly-connected-components combined-vg)]
                             (if (seq combined-sccs)
                               [vg k-vg sources (conj cyclic-versions {:sources combined-sources
                                                                       :sccs    combined-sccs})]
                               [combined-vg combined-k-vg combined-sources cyclic-versions])))))
                     [(b/linear (g/digraph)) nil #{} nil]))

        vg  (b/forked vg)]

    (cond-> {:causal-vg  vg
             :causal-kvg k-vg}
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
           :op          a
           :a-mop-index (index-of (:value a) [:append k v])
           :kv          [k v]
           :op'         b
           :kv'         [k' v']
           :b-mop-index (index-of (:value b) [:append k' v'])}))))

  (render-explanation [_ {:keys [kv kv']} a-name b-name]
    (str a-name " wrote " (pr-str kv) " which <hb in causal version order "
         b-name "'s write of " (pr-str kv'))))

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
             (reduce (fn [[tg anomalies] {:keys [value] :as read-op}]
                       (->> value
                            ext-reads
                            (reduce (fn [[tg anomalies] [k vs]]
                                      (let [this-kv   [k (last vs)]
                                            this-k-vg (get causal-kvg k (g/digraph))
                                            next-kvs  (g/out this-k-vg this-kv)]
                                        (->> next-kvs
                                             (reduce (fn [[tg anomalies] next-kv]
                                                       (let [write-op (get write-index next-kv)]
                                                         (if write-op
                                                           [(g/link tg read-op write-op rw) anomalies]
                                                           [tg (conj anomalies (MapEntry. :garbage-versions
                                                                                          {:type :rw
                                                                                           :op   read-op
                                                                                           :kv   this-kv
                                                                                           :kv'  next-kv}))])))
                                                     [tg anomalies]))))
                                    [tg anomalies])))
                     [(b/linear (g/op-digraph)) nil]))
        tg (b/forked tg)]

    (cond-> {:graph     tg
             :explainer (RWExplainer. causal-kvg)}
      (seq anomalies)
      (assoc :anomalies anomalies))))
