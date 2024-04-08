(ns causal.lww-list-append.checker.causal-consistency
  "A Causal Consistency checker for Last Write Wins List Append registers"
  (:require [bifurcan-clj [core :as b]
             [graph :as bg]
             [set :as bs]]
            [clojure.set :as set]
            [clojure.tools.logging :refer [info]]
            [dom-top.core :refer [loopr]]
            [elle
             [core :as elle]
             [txn :as ct]
             [graph :as g]
             [rels :refer [ww wr rw]]
             [util :as util :refer [index-of
                                    op-memoize]]]
            [jepsen
             [history :as h]
             [txn :as txn :refer [reduce-mops]]]
            [jepsen.history.fold :refer [loopf]]
            [slingshot.slingshot :refer [throw+]]
            [tesser.core :as t]
            [elle.graph :as g]
            [bifurcan-clj.graph :as bg])
  (:import (io.lacuna.bifurcan DirectedGraph
                               IEdge
                               IEntry
                               IMap
                               ISet
                               LinearMap
                               LinearSet
                               Map
                               Set)
           (java.util.function BinaryOperator)
           (jepsen.history Op)))

(defn kv-write-index
  "Given a history, returns a
   ```
   {[k v] op}  ; writes are asserted to be unique
   ```
   for all writes, including intermediate writes.
   Intermediate writes will be observed in the prefix of reads
   so can be used for causal ordering."
  [history]
  (->> history
       ct/op-mops
       (reduce (fn [index [op [f k v :as mop]]]
                 (case f
                   :r index
                   :w (if-let [existing (get index [k v])]
                        (throw+ {:type     :duplicate-write
                                 :op       op
                                 :mop      mop
                                 :existing existing})
                        (assoc index [k v] op))))
               nil)))

(defn kv-read-index
  "Given a history, returns a
   ```
   {[k v] #{ops}}
   ```
   for all reads of [k v], including
     - full read prefix v's
     - intermediate reads in transaction
   Including read prefixes and intermediate reads can be used for causal ordering."
  [history]
  (->> history
       ct/op-mops
       (reduce (fn [index [op [f k v :as _mop]]]
                 (case f
                   :w index
                   :r (->> v
                           (reduce (fn [index v']
                                     (update index [k v'] set/union #{op}))
                                   index))))
               nil)))

(defn txn-write-kvs
  "Given a transaction, returns a #{[k v]} for all write values including intermediate writes."
  [txn]
  (->> txn
       (reduce (fn [w-kv [f k v]]
                 (case f
                   :r w-kv
                   :w (set/union w-kv #{[k v]})))
               nil)))

(defn txn-read-kvs
  "Given a transaction, returns a #{[k v]} for all read values including prefixes and intermediate reads."
  [txn]
  (->> txn
       (reduce (fn [r-kv [f k v]]
                 (case f
                   :w r-kv
                   :r (set/union r-kv (->> v
                                           (map (fn [v'] [k v']))
                                           (into #{})))))
               nil)))

(defn txn-read-kv-mops
  "Given a transaction, returns a seq of the mops that read [k v]."
  [txn [k v]]
  (->> txn
       (filter (fn [[m-f m-k m-v :as _mop]]
                 (case m-f
                   :w false
                   :r (and (= m-k k)
                           (contains? m-v v)))))))

(defn op-internal-case
  "Given an op, returns a map describing internal consistency violations, or
  nil otherwise. Our maps are:

      {:op        The operation which went wrong
       :mop       The micro-operation which went wrong
       :expected  The state we expected to observe.}"
  [op]
  ; We maintain a map of keys to expected states.
  (->> (:value op)
       (reduce (fn [[state error] [f k v :as mop]]
                 (case f
                   :w [(assoc! state k v) error]
                   :r (let [s (get state k)]
                        (if (and s (not= s v))
                          ; Not equal!
                          (reduced [state
                                    {:op       op
                                     :mop      mop
                                     :expected s}])
                          ; OK! Either a match, or our first time seeing k.
                          [(assoc! state k v) error]))))
               [(transient {}) nil])
       second))

(defn internal-cases
  "Given a history, finds operations which exhibit internal consistency
  violations: e.g. some read [:r k v] in the transaction fails to observe a v
  consistent with that transaction's previous write to k."
  [history]
  (ct/ok-keep op-internal-case history))

(defn g1a-cases
  "G1a, or aborted read, is an anomaly where a transaction reads data from an
  aborted transaction. For us, an aborted transaction is one that we know
  failed. Info transactions may abort, but if they do, the only way for us to
  TELL they aborted is by observing their writes, and if we observe their
  writes, we can't conclude they aborted, sooooo...

  This function takes a history (which should include :fail events!), and
  produces a sequence of error objects, each representing an operation which
  read state written by a failed transaction."
  [history]
  ; Build a map of keys to maps of failed elements to the ops that appended
  ; them.
  (let [failed (ct/failed-write-indices #{:w} history)]
    ; Look for ok ops with a read mop of a failed append
    (->> history
         h/oks
         ct/op-mops
         (keep (fn [[^Op op [f k v :as mop]]]
                 (when (= :r f)
                   (when-let [writer-index (get-in failed [k v])]
                     {:op        op
                      :mop       mop
                      :writer    (h/get-index history writer-index)}))))
         seq)))

(defn g1b-cases
  "G1b, or intermediate read, is an anomaly where a transaction T2 reads a
  state for key k that was written by another transaction, T1, that was not
  T1's final update to k.

  This function takes a history (which should include :fail events!), and
  produces a sequence of error objects, each representing a read of an
  intermediate state."
  [history]
  ; Build a map of keys to maps of intermediate elements to the ops that wrote
  ; them
  (let [im (ct/intermediate-write-indices #{:w} history)]
    ; Look for ok ops with a read mop of an intermediate append
    (->> history
         h/oks
         ct/op-mops
         (keep (fn [[^Op op [f k v :as mop]]]
                 (when (= :r f)
									 ; We've got an illegal read if value came from an
				           ; intermediate append.
                   (when-let [writer-index (get-in im [k v])]
                     ; Internal reads are OK!
                     (when (not= (.index op) writer-index)
                       {:op       op
                        :mop      mop
                        :writer   (h/get-index history writer-index)})))))
         seq)))

(defn processes
  "Given a history, returns a set containing all of the processes in the history."
  [history]
  (->> history
       (map (fn [{:keys [process] :as _op}]
              process))
       (into #{})))

(defn monotonic-writes
  "Given a history, returns a version graph with monotonic writes ordering.
   Monotonic writes are per process.
   
   As causal is multi-object, i.e. multiple keys, so is our graph:
   ```
   [:w :x 1]
   [:w :y 2]

   [:x 1] <hb [:y 2]
   ```"
  [history]
  (let [[g _prev-writes]
        (->> history
             ct/op-mops
             (reduce (fn [[g prev-writes] [{:keys [process] :as _op} [f k v :as _mop]]]
                       (case f
                         :r [g prev-writes]
                         :w [(let [prev-write (get prev-writes process)]
                               (if (nil? prev-write)
                                 (g/link g [k nil]    [k v])
                                 (g/link g prev-write [k v])))
                             (assoc prev-writes process [k v])]))
                     [(b/linear (g/digraph)) nil]))]
    (b/forked g)))

(defn writes-follow-reads
  "Given a history, returns a version graph with writes follows reads ordering.
   Writes follows reads are per process.
   
   As causal is multi-object, i.e. multiple keys, so is our graph:
   ```
   [:r :x 1]
   [:w :y 2]

   [:x 1] <hb [:y 2]
   ```"
  [history]
  (let [[g _prev-reads]
        (->> history
             ct/op-mops
             (reduce (fn [[g prev-reads] [{:keys [process] :as _op} [f k v :as _mop]]]
                       (case f
                         :r [g (update prev-reads process set/union #{[k (last v)]})]
                         :w [(g/link-all-to g (get prev-reads process) [k v])
                             (dissoc prev-reads process)]))
                     [(b/linear (g/digraph)) nil]))]
    (b/forked g)))

(defn monotonic-reads
  "Given a history, returns a version graph with monotonic reads ordering.
   Monotonic reads are per process, per key.
   ```
   [:r :x 1]
   [:r :y 1]
   [:r :x 2]

   [:x 1] <hb [:x 2]
   [:x 1] ?hb [:y 1] and [:y 1] ?hb [:x 2]
   ```"
  [history]
  (let [[g _prev-reads]
        (->> history
             ct/op-mops
             (reduce (fn [[g prev-reads] [{:keys [process] :as _op} [f k v :as _mop]]]
                       (case f
                         ; writes count as an observation, reading your writes :)
                         :w [g (assoc-in prev-reads [process k] v)]
                         :r (let [v      (last v)
                                  prev-v (get-in prev-reads [process k])]
                              [; may be reading same v as previous read of v in the process
                               (if (= prev-v v)
                                 g
                                 (g/link g [k prev-v] [k v]))
                               (assoc-in prev-reads [process k] v)])))
                     [(b/linear (g/digraph)) nil]))]
    (b/forked g)))

(defn version-graph
  "Given a history, returns
   ```
   {:version-graph   version-graph
    :sources         #{version-sources-used-to-build-graph}
    :cyclic-versions sequence-of-versions-with-cycles}
   ```
   If any version source graph has cycles, or would create cycles if combined with other sources,
   it is not combined into the final graph and its cycles are reported."
  [history]
  (let [[vg sources cyclic-versions]
        (->> [[:monotonic-writes    monotonic-writes]
              [:writes-follow-reads writes-follow-reads]
              [:monotonic-reads     monotonic-reads]]
             (reduce (fn [[vg sources cyclic-versions] [next-source next-grapher]]
                       ; check this version graph individually before combining
                       (let [next-vg   (next-grapher history)
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
                     [(b/linear (g/digraph)) #{} nil]))]
    (merge {:version-graph (b/forked vg)}
           (when (seq sources)
             {:sources sources})
           (when (seq cyclic-versions)
             {:cyclic-versions cyclic-versions}))))

(defrecord WRExplainer []
  elle/DataExplainer
  (explain-pair-data [_ a b]
    (let [writes          (txn-write-kvs (:value a))
          reads           (txn-read-kvs  (:value b))
          [k v
           :as shared-kv] (first (set/intersection writes reads))
          r-mop           (first (txn-read-kv-mops (:value b) shared-kv))]
      (when r-mop
        {:type  :wr
         :key   k
         :value v
         :a-mop-index (index-of (:value a) [:w k v])
         :b-mop-index (index-of (:value b) r-mop)})))

  (render-explanation [_ {:keys [key value]} a-name b-name]
    (str a-name " wrote " (pr-str key) " = " (pr-str value)
         ", which was read by " b-name)))

(defn wr-graph
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
                                                       (reduce (fn [first-r's {:keys [process index] :as r-op}]
                                                                 (if-let [cur-first (get first-r's process)]
                                                                   (if (< index (:index cur-first))
                                                                     (assoc first-r's process r-op)
                                                                     first-r's)
                                                                   (assoc first-r's process r-op)))
                                                               {})
                                                       vals)]
                                        [(g/link-to-all tg w-op r-ops wr) anomalies]))
                                    [(b/linear (g/op-digraph)) nil]))]
    (merge {:graph     (b/forked tg)
            :explainer (WRExplainer.)}
           (when (seq anomalies)
             {:anomalies anomalies}))))

(defrecord WWExplainer [version-graph]
  elle/DataExplainer
  (explain-pair-data [_ a b]
    (let [writes   (txn-write-kvs (:value a))
          writes'  (txn-write-kvs  (:value b))
          edges    (->> version-graph
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
           :a-mop-index (index-of (:value a) [:w k v])
           :b-mop-index (index-of (:value b) [:w k' v'])}))))

  (render-explanation [_ {:keys [kv kv']} a-name b-name]
    (str a-name " wrote " (pr-str kv) " which <hb in causal version order " (pr-str kv')
         ", which was written by " b-name)))

(defn ww-graph
  "Given a version graph, a write index, and an unused history to match the calling API, returns
   ```
   {:graph     wr-transaction-graph
    :explainer wr-explainer
    :anomalies seq-of-anomalies}
   ```
   with write of [k v] <hb write of [k v]' based on version ordering.
   A ww edge is only created to the first write in each process that >hb the write
   as succeeding writes are transitively included by process order."
  [{:keys [version-graph write-index] :as _indexes} _history]
  (let [vg (g/collapse-graph (fn [[_k v :as vertex]]
                               (if (nil? v)
                                 ; all versions start at nil, nobody actually writes it, ever
                                 false
                                 (let [vertex-op (get write-index vertex)
                                       in's      (g/in version-graph vertex)]
                                   (if (not (seq in's))
                                     ; this is a bottom, starting vertex, keep
                                     true
                                     (let [in's (->> in's
                                                     (filter (fn [from-kv]
                                                               (let [from-op (get write-index from-kv)]
                                                                 (cond
                                                                 ; don't self link
                                                                   (= (:index from-op)
                                                                      (:index vertex-op))
                                                                   false

                                                                 ; later in same process
                                                                   (and (= (:process from-op)
                                                                           (:process vertex-op))
                                                                        (< (:index from-op)
                                                                           (:index vertex-op)))
                                                                   false

                                                                   :else
                                                                   true)))))]
                                     ; if relevant inbound edges remain, keep
                                       (if (seq in's)
                                         true
                                         false))))))
                             version-graph)

        [tg anomalies] (->> vg
                            bg/edges
                            (reduce (fn [[tg anomalies] edge]
                                      (let [from-op (->> edge .from (get write-index))
                                            to-op   (->> edge .to   (get write-index))]
                                        (cond
                                          ; don't self link
                                          (= (:index from-op)
                                             (:index to-op))
                                          [tg anomalies]

                                          ; later in same process
                                          (and (= (:process from-op)
                                                  (:process to-op))
                                               (< (:index from-op)
                                                  (:index to-op)))
                                          [tg anomalies]

                                          :else
                                          [(g/link tg from-op to-op ww) anomalies])))
                                    [(b/linear (g/op-digraph)) nil]))]
    (merge {:graph     (b/forked tg)
            :explainer (WWExplainer. vg)}
           (when (seq anomalies)
             {:anomalies anomalies}))))

(defn non-monotonic-read-order
  "Given a version graph, a kv-read-index, and a process, return a sequence of Monotonic Read anomalies.
   
   Monotonic Reads are checked per process against the version graph.
   Will find intermediate read anomalies within a transaction."
  [version-graph kv-read-index process]
  (let [; filter to just reads by this process
        kv-read-index (->> kv-read-index
                           (keep (fn [[kv ops]]
                                   (let [ops (->> ops
                                                  (filter (fn [op]
                                                            (= (:process op)
                                                               process)))
                                                  (into #{}))]
                                     (when (seq ops)
                                       [kv ops]))))
                           (into {}))
        ; collapse version graph to just include the read [k v] for this process
        vg (g/collapse-graph (fn [vertex]
                               (seq (get kv-read-index vertex)))
                             version-graph)]
    (->> vg
         bg/edges
         (reduce (fn [anomalies edge]
                   (let [from-kv (.from edge)
                         from-op (->>  (get kv-read-index from-kv)
                                       ; select last from op
                                       (reduce (fn
                                                 ([] (identity nil))
                                                 ([op op']
                                                  (if (< (:index op)
                                                         (:index op'))
                                                    op'
                                                    op)))))
                         to-kv   (.to edge)
                         to-op   (->> (get kv-read-index to-kv)
                                      ; select first to op
                                      (reduce (fn
                                                ([] (identity nil))
                                                ([op op']
                                                 (if (< (:index op)
                                                        (:index op'))
                                                   op
                                                   op')))))]
                     (cond
                       ; if self link, check mop ordering
                       (= (:index from-op)
                          (:index to-op))
                       (let [from-mop (-> (:value from-op)
                                          (txn-read-kv-mops from-kv)
                                          last)
                             to-mop   (-> (:value to-op)
                                          (txn-read-kv-mops to-kv)
                                          first)]
                         (when (> (index-of (:value from-op) from-mop)
                                  (index-of (:value to-op)   to-mop))
                           (conj anomalies {:intra-transaction? true
                                            :op   from-op
                                            :mop  from-mop
                                            :kv   from-kv
                                            :op'  to-op
                                            :mop' to-mop
                                            :kv'  to-kv})))

                       ; version order <> process order
                       (< (:index to-op)
                          (:index from-op))
                       (let [from-mop (-> (:value from-op)
                                          (txn-read-kv-mops from-kv)
                                          last)
                             to-mop   (-> (:value to-op)
                                          (txn-read-kv-mops to-kv)
                                          first)]
                         (conj anomalies {:op   from-op
                                          :mop  from-mop
                                          :kv   from-kv
                                          :op'  to-op
                                          :mop' to-mop
                                          :kv'  to-kv}))

                       ; later in same process
                       :else
                       anomalies)))
                 nil))))

(defn non-monotonic-reads
  "Given a version graph, a read-index, and processes, returns a sequence of Monotonic Read anomalies.
   
   Monotonic Reads are checked per process against the version graph.
   Will find intermediate read anomalies within a transaction."
  [{:keys [version-graph read-index processes] :as _indexes}]
  (->> processes
       (mapcat (fn [process]
                 (non-monotonic-read-order version-graph read-index process)))
       seq))

(defn graph
  "Given options and a history, computes a
   ```
   {:graph      transaction-graph
    :explainer  explainer-for-graph
    :anomalies  seq-of-anomalies}
   ```
   of causal dependencies for transactions.
   We combine several pieces:

     - w <hb r, from each write of [k v] to the first read of [k v] in each process

     - w <hb w', derived from version order
         - nil precedes every [k v] for first interaction with [k] in each process
         - Monotonic Writes
         - Writes Follow Reads
         - Monotonic Reads
   
     - additional graphs, as given by (:additional-graphs opts)"
  [opts indexes history]
  (let [; Build our combined analyzers
        analyzers (into [elle/process-graph
                         (partial wr-graph indexes)
                         (partial ww-graph indexes)]
                        (ct/additional-graphs opts))
        analyzer (apply elle/combine analyzers)]
    ; And go!
    (analyzer history)))

(defn check
  "Full checker for write-read registers. Options are:

    :consistency-models     A collection of consistency models we expect this
                            history to obey. Defaults to [:consistent-view].
                            See elle.consistency-model for available models.

    :anomalies              You can also specify a collection of specific
                            anomalies you'd like to look for. Performs limited
                            expansion as per
                            elle.consistency-model/implied-anomalies.

    :additional-graphs      A collection of graph analyzers (e.g. realtime)
                            which should be merged with our own dependency
                            graph.

    :cycle-search-timeout   How many milliseconds are we willing to search a
                            single SCC for a cycle?

    :directory              Where to output files, if desired. (default nil)

    :plot-format            Either :png or :svg (default :svg)

    :plot-timeout           How many milliseconds will we wait to render a SCC
                            plot?

    :max-plot-bytes         Maximum size of a cycle graph (in bytes of DOT)
                            which we're willing to try and render."
  ([history]
   (check {} history))
  ([opts history]
   (let [history      (->> history
                           h/client-ops)
         history-oks  (->> history
                           ; TODO: shouldn't be any :info in total sticky availability, handle explicitly
                           h/oks)

         type-sanity  (h/task history-oks :type-sanity []
                              (ct/assert-type-sanity history-oks))
        ;;  g1a          (h/task history     :g1a [] (g1a-cases history)) ; needs complete history including :fail
        ;;  g1b          (h/task history-oks :g1b [] (g1b-cases history-oks))
        ;;  internal     (h/task history-oks :internal [] (internal-cases history-oks))

         processes    (h/task history-oks :processes []
                              (processes history-oks))
         write-index  (h/task history-oks :write-index []
                              (kv-write-index history-oks))
         read-index   (h/task history-oks :read-index []
                              (kv-read-index history-oks))

         version-graph (h/task history-oks :version-graph []
                               (version-graph history-oks))

         ; derefs below start any blocking

         {:keys [version-graph cyclic-versions]} @version-graph

         indexes      {:version-graph version-graph
                       :processes     @processes
                       :write-index   @write-index
                       :read-index    @read-index}

         cycles       (:anomalies (ct/cycles! opts
                                              (partial graph opts indexes)
                                              history-oks))

         non-monotonic-reads (non-monotonic-reads indexes)

         _            @type-sanity ; Will throw if problems

         ; Build up anomaly map
         anomalies (cond-> cycles
                     ;;  @internal     (assoc :internal @internal)
                     ;;  @g1a          (assoc :G1a @g1a)
                     ;;  @g1b          (assoc :G1b @g1b)
                     cyclic-versions     (assoc :cyclic-versions cyclic-versions)
                     non-monotonic-reads (assoc :monotonic-reads non-monotonic-reads))]
     (ct/result-map opts anomalies))))

