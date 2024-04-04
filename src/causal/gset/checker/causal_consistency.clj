(ns causal.gset.checker.causal-consistency
  (:require [bifurcan-clj
             [core :as b]]
            [clojure.set :as set]
            [elle
             [core :as elle]
             [graph :as g]
             [rels :refer [ww wr rw]]
             [txn :as ct]
             [util :as util :refer [index-of]]]
            [jepsen
             [checker :as checker]
             [history :as h]
             [store :as store]]
            [slingshot.slingshot :refer [throw+]]))

(defn r-kvm
  "Given a transaction, returns a
   {k #{vs}} of all read values.
     - if multiple reads of k, last read is returned
     - will return {k nil} for nil reads of k
     - returns nil if no reads"
  [txn]
  (->> txn
       (reduce (fn [kvsm [f k v]]
                 (case f
                   :r (assoc kvsm k v)
                   :w kvsm))
               nil)))

(defn w-kvm
  "Given a transaction, returns a
   {k #{vs}} of all write values."
  [txn]
  (->> txn
       (reduce (fn [kvm [f k v]]
                 (case f
                   :w (update kvm k set/union #{v})
                   :r kvm))
               {})))

(defn op-internal-case
  "Given an op, returns nil or a map describing the first internal consistency violation:
   ```
   {:type   type of violation, :read-your-writes or :monotonic-reads
    :op     operation which went wrong
    :mop    micro-operation which went wrong
    :unread [k #{v}] missing from read}
   ```"
  [{:keys [value] :as op}]
  (let [[error _prev-r _new-w]
        (->> value
             (reduce (fn [[_error prev-r new-w] [f k v :as mop]]
                       (case f
                         :w [nil prev-r (update new-w k set/union #{v})]
                         :r (let [prev-r'  (get prev-r k)
                                  new-w'   (get new-w k)
                                  unread-w (set/difference new-w' v)
                                  missed-r (set/difference prev-r' v)]
                              (cond
                                (seq unread-w)
                                (reduced [{:type   :read-your-writes
                                           :op     op
                                           :mop    mop
                                           :unread [k unread-w]}
                                          nil nil])

                                (seq missed-r)
                                (reduced [{:type   :monotonic-reads
                                           :op     op
                                           :mop    mop
                                           :unread [k missed-r]}
                                          nil nil])

                                ; reasonable read
                                :else [nil
                                       (assoc prev-r k v)
                                       (dissoc new-w k)]))))
                     [nil nil nil]))]
    error))

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
  [{:keys [read-index] :as _indexes} history]
  (let [; map to [w-k w-vs op]
        all-w-kvs (->> history
                       h/fails
                       (mapcat (fn [{:keys [value] :as op}]
                                 (->>  value
                                       w-kvm
                                       (map (fn [[w-k w-vs]]
                                              [w-k w-vs op]))))))]
    (->> all-w-kvs
         (mapcat (fn [[w-k w-vs op]]
                   (let [; all read vs for write k
                         all-r-vs (->> w-k
                                       (get read-index) ; {k {#{vs} #{ops}}}
                                       keys)
                         ; read vs that read any failed write vs
                         failed-r-vs (->> all-r-vs
                                          (filter (fn [r-vs]
                                                    (let [common-vs (set/intersection r-vs w-vs)]
                                                      (seq common-vs)))))]
                     ; errors for all failed read vs
                     (->> failed-r-vs
                          (keep (fn [r-vs]
                                  (let [failed-r-ops (get-in read-index [w-k r-vs])
                                        ; don't self report, we're a failed read
                                        failed-r-ops (disj failed-r-ops op)]
                                    (when (seq failed-r-ops)
                                      {:writer   op
                                       :readers  failed-r-ops
                                       :read-of-failed  [w-k (into (sorted-set) (set/intersection w-vs r-vs))]}))))))))
         seq)))

(defn g1b-cases
  "G1b, or intermediate read, is an anomaly where a transaction T2 reads a
  state for key k that was written by another transaction, T1, that was not
  T1's final update to k.

  This function takes a read index and a history, and produces a sequence of error objects,
  each representing reads of an intermediate state."
  [{:keys [read-index] :as _indexes} history]
  (let [; map to [w-k w-vs op]
        all-w-kvs (->> history
                       (mapcat (fn [{:keys [value] :as op}]
                                 (->>  value
                                       w-kvm
                                       (map (fn [[w-k w-vs]]
                                              [w-k w-vs op]))))))]
    (->> all-w-kvs
         (mapcat (fn [[w-k w-vs op]]
                   (let [; all read vs for write k
                         all-r-vs (->> w-k
                                       (get read-index) ; {k {#{vs} #{ops}}}
                                       keys)
                         ; read vs that read some but not all write vs
                         inter-r-vs (->> all-r-vs
                                         (filter (fn [r-vs]
                                                   (let [common-vs (set/intersection r-vs w-vs)]
                                                     (and (seq common-vs)
                                                          (not= (count common-vs)
                                                                (count w-vs)))))))]
                     ; errors for all intermediate read vs
                     (->> inter-r-vs
                          (keep (fn [r-vs]
                                  (let [inter-r-ops (get-in read-index [w-k r-vs])
                                        ; don't self report, internal reads are OK
                                        inter-r-ops (disj inter-r-ops op)]
                                    (when (seq inter-r-ops)
                                      {:writer   op
                                       :readers  inter-r-ops
                                       :missing  [w-k (set/difference w-vs r-vs)]}))))))))
         seq)))

(defn reduce-kvm
  "Reduces over a kvm, {k #{vs}}, with (fn acc k v) for all ks/vs.
   Avoids having to nest reduces."
  [r-fn init-state kvm]
  (->> kvm
       (reduce-kv (fn [acc k vs]
                    (->> vs
                         (reduce (fn [acc v]
                                   (r-fn acc k v))
                                 acc)))
                  init-state)))

(defn reduce-nested
  "Convenience for nested maps, {k {k' v}}.
   Reduces with (fn acc k k' v) for all k and k'."
  [reduce-fn init-state coll]
  (->> coll
       (reduce-kv (fn [acc k inner-map]
                    (->> inner-map
                         (reduce-kv (fn [acc k' v]
                                      (reduce-fn acc k k' v))
                                    acc)))
                  init-state)))

(defn r-index
  "Given a history, returns a read index:
   ```
   {k {#{vs} #{ops}}}
   ```
   for *all* [k #{vs}] read, including intermediate reads in a transaction."
  [history]
  (->> history
       ct/op-mops
       (reduce (fn [index [op [f k v :as _mop]]]
                 (case f
                   :w index
                   :r (update-in index [k v] set/union #{op})))
               nil)))

(defn w-index
  "Given a history, returns a write index:
   ```
   {k {v op} ;; writes are unique
   ```"
  [history]
  (->> history
       ct/op-mops
       (reduce (fn [index [op [f k v :as mop]]]
                 (case f
                   :r index
                   :w (if-let [existing (get-in index [k v])]
                        (throw+ {:type     :non-unique-write
                                 :op       op
                                 :mop      mop
                                 :existing existing})
                        (assoc-in index [k v] op))))
               nil)))

(defrecord WRExplainer []
  elle/DataExplainer
  (explain-pair-data [_ a b]
    (let [a-w-kvm (w-kvm (:value a))
          b-r-kvm (r-kvm (:value b))]
      ; first shared kv is fine
      (->> a-w-kvm
           (reduce-kv (fn [_ w-k w-vs]
                        (let [r-vs      (get b-r-kvm w-k)
                              shared-vs (set/intersection w-vs r-vs)
                              w-v       (first shared-vs)]
                          (when (seq shared-vs)
                            (reduced {:type :wr
                                      :w-k  w-k
                                      :w-v  w-v
                                      :a-mop-index (index-of (:value a) [:w w-k w-v])
                                      :b-mop-index (index-of (:value b) [:r w-k r-vs])}))))
                      nil))))

  (render-explanation [_ {:keys [w-k w-v]} a-name b-name]
    (str a-name "'s write of [" (pr-str w-k) " " (pr-str w-v) "]"
         " was read by " b-name " (w->r)")))

(defn wr-graph
  "Given read and write indexes for a history, and an unused history to match the calling APIs,
   creates a w->r edge for every write of [k v] to the first read of [k v] in every process.
   Succeeding reads of [k v] are also transitively happens after due to process order."
  [{:keys [read-index write-index] :as _opts} _history]
  (let [g (->> write-index ; {k {v op}
               (reduce-nested (fn [g k v write-op]
                                (let [; read-index {k {#{vs} #{ops}}}
                                      read-ops (->> (get read-index k) ; {#{vs} #{ops}}
                                                    (filter (fn [[vs _ops]] (contains? vs v)))
                                                    (mapcat val) ; seq-ops
                                                    (into #{}))
                                      ; don't self link
                                      read-ops (disj read-ops write-op)
                                      ; first read op in each process
                                      read-ops (->> read-ops
                                                    (reduce (fn [first-reads {:keys [process] :as read-op}]
                                                              (let [curr-first (get first-reads process)]
                                                                (if (or
                                                                     ; first read op for process
                                                                     (nil? curr-first)
                                                                     ; this read op happened before
                                                                     (< (:index read-op) (:index curr-first)))
                                                                  (assoc first-reads process read-op)
                                                                  ; current first is still first
                                                                  first-reads)))
                                                            nil)
                                                    vals)]
                                  (g/link-to-all g write-op read-ops wr)))
                              (b/linear (g/op-digraph)))
               b/forked)]
    {:graph     g
     :explainer (WRExplainer.)}))


(defrecord WFRExplainer [read-index]
  elle/DataExplainer
  (explain-pair-data [_ a b]
    (let [a-writes  (w-kvm (:value a))
          b-writes  (w-kvm (:value b))
          b-index   (:index b)
          b-process (:process b)]
      (when (and (seq a-writes)
                 (seq b-writes))
        ; did b already read any of a's writes?
        ; if so, first found is fine
        (->> a-writes
             (reduce-kv (fn [_ a-k a-vs]
                          (->> (get read-index a-k) ; {k {#{vs} #{ops}}}
                               (reduce-kv (fn [_ read-vs read-ops]
                                            (let [shared-vs (set/intersection a-vs read-vs)]
                                              (when (seq shared-vs)
                                                (let [read-ops (->> read-ops
                                                                    (filter (fn [{:keys [process index]}]
                                                                              (and (= process b-process)
                                                                                   (< index b-index)))))]
                                                  (when (seq read-ops)
                                                    (let [a-v        (first shared-vs)
                                                          [b-k b-vs] (first b-writes)
                                                          b-v        (first b-vs)]
                                                      (reduced {:type        :ww
                                                                :a-mop-index (index-of (:value a) [:w a-k a-v])
                                                                :b-mop-index (index-of (:value b) [:w b-k b-v])
                                                                :process     b-process
                                                                :k           a-k
                                                                :v           a-v})))))))
                                          nil)))
                        nil)))))

  (render-explanation [_ {:keys [process k v]} a-name b-name]
    (str a-name "'s write of " [k v] " was observed by process " process " before it executed " b-name " (wfr)")))

(defn wfr-order
  "Given a write-index, history, and a process,
   create a w->w transaction graph with writes follow reads ordering for that process.
   
   Go through process history mop-by-mop to get WFR ordering both within a transaction and between transactions.
   Only create one w->w edge to the process for the first observation of a write of [k v].
   Succeeding observations are transitively happens before due to process order."
  [{:keys [write-index] :as _indexes} history process]
  (let [history (->> history
                     (h/filter (comp #{process} :process)))
        [g _observing _observed]
        (->> history
             ct/op-mops
             (reduce (fn [[g observing observed] [op [mop-f mop-k mop-v :as _mop]]]
                       (case mop-f
                         :r [g
                             (update observing mop-k set/union mop-v)   ; union all observations regardless of monotonic reads
                             observed]
                         :w (let [new-k-vs     (->> observing
                                                    (keep (fn [[o-k o-vs]]
                                                            (let [new-vs (set/difference o-vs (get observed o-k))]
                                                              (when (seq new-vs)
                                                                [o-k new-vs]))))
                                                    (into {}))
                                  before-w-ops (->> new-k-vs
                                                    (mapcat (fn [[new-k new-vs]]
                                                              (->> new-vs
                                                                   (map (fn [new-v]
                                                                          (get-in write-index [new-k new-v]))))))
                                                    (into #{}))
                                  ; don't link to self
                                  before-w-ops (disj before-w-ops op)]
                              [(g/link-all-to g before-w-ops op ww)
                               nil
                               (merge-with set/union observed new-k-vs)])))
                     [(b/linear (g/op-digraph)) nil nil]))]
    (b/forked g)))

(defn wfr-graph
  "Given processes, read/write indexes, and a history,
   creates a w->w transaction graph with writes follows reads ordering for each process."
  [{:keys [processes read-index write-index] :as _opts} history]
  (let [graph (->> processes
                   (map (partial wfr-order {:write-index write-index} history))
                   (apply g/digraph-union))]
    {:graph     graph
     :explainer (WFRExplainer. read-index)}))

(defrecord RWExplainer []
  elle/DataExplainer
  (explain-pair-data [_ a b]
    (let [a-r-kvm (r-kvm (:value a))
          b-w-kvm (w-kvm  (:value b))]
      (when (and (seq a-r-kvm)
                 (seq b-w-kvm))
        (->> a-r-kvm
             (reduce-kv (fn [_ k vs]
                          (let [unread-vs (set/difference (get b-w-kvm k) vs)]
                            (when (seq unread-vs)
                              (reduced {:type    :rw
                                        :r-key   k
                                        :r-value vs
                                        :w-value (first unread-vs)
                                        :a-mop-index (index-of (:value a) [:r k vs])
                                        :b-mop-index (index-of (:value b) [:w k (first unread-vs)])}))))
                        nil)))))

  (render-explanation [_ {:keys [r-key r-value w-value]} a-name b-name]
    (str a-name "'s read of [" (pr-str r-key) " " (pr-str r-value) "]"
         " did not observe " b-name "'s write of [" (pr-str r-key) " " (pr-str w-value) "] (r->w)")))

(defn rw-graph
  "Given write indexes, a process, and a history,
   creates an inferred r->w edge:
     - the last read in the process that did not observe the write of [k v]
     - any preceding reads that didn't observe [k v] are transitively happened before due to process order"
  [{:keys [write-index write-kvs read-process] :as _indexes} history]
  (let [history (->> history
                     (h/filter (comp #{read-process} :process)))

        last-r-of-kv (->> history
                          ct/op-mops
                          (reduce (fn [last-r-of-kv [op [f k v :as _mop]]]
                                    (case f
                                      :w last-r-of-kv
                                      :r (->> (set/difference (get write-kvs k) v)  ; unread v
                                              (reduce (fn [last-r-of-kv v']
                                                        (cond
                                                          ; don't self link if this read is same transaction as write
                                                          (= (:index op) (:index (get-in write-index [k v'])))
                                                          last-r-of-kv

                                                          ; first time [k v] unread
                                                          (nil? (get-in last-r-of-kv [k v']))
                                                          (assoc-in last-r-of-kv [k v'] op)

                                                          ; this read happened after prev last read of kv
                                                          (> (:index op) (:index (get-in last-r-of-kv [k v'])))
                                                          (assoc-in last-r-of-kv [k v'] op)

                                                          ; this read happened before prev last read of kv
                                                          :else
                                                          last-r-of-kv))
                                                      last-r-of-kv))))
                                  nil))
        g (->> last-r-of-kv ; {k {v last-r-op}}
               (reduce-nested (fn [g k v read-op]
                                (g/link g read-op (get-in write-index [k v]) rw))
                              (b/linear (g/op-digraph)))
               b/forked)]
    {:graph     g
     :explainer (RWExplainer.)}))

(defn graph
  "Given indexes/options and a history, creates an analyzer that computes a {:graph g, :explainer e}
   using causal ordering. We combine several pieces:

     - process graph
   
     - w->r graph, a write of v happens before all reads of v ordering
   
     - w->w graph, writes follow reads ordering

     - r->w graph, infer that all reads that don't observe [k v] happen before the write of [k v]
       ordering edges are only created for one process given by `:process`
   
     - additional graphs, as given by (:additional-graphs opts).

   The graph we return combines all this information."
  [opts history]
  (let [; Build our combined analyzers
        analyzers (into [elle/process-graph
                         (partial wr-graph    opts)
                         (partial wfr-graph   opts)
                         (partial rw-graph    opts)]
                        (ct/additional-graphs opts))
        analyzer (apply elle/combine analyzers)]
    ; And go!
    (analyzer history)))

(defn check
  "Full checker for a grow only set. Options are:

    :consistency-models     A collection of consistency models we expect this
                            history to obey. Defaults to [:strict-serializable].
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
                            (default 1000)

    :directory              Where to output files, relative to the test store directory, if desired.
                            (default nil)

    :plot-format            Either :png or :svg 
                            (default :svg)

    :plot-timeout           How many milliseconds will we wait to render a SCC
                            plot?

    :max-plot-bytes         Maximum size of a cycle graph (in bytes of DOT)
                            which we're willing to try and render.
"
  ([history]
   (check {} history))
  ([opts history]
   (let [history     (->> history
                          h/client-ops)
         history-oks (->> history
                          h/oks)  ; TODO: account for :info ops

         processes   (h/task history-oks :processes []
                             (->> history-oks
                                  (h/map :process)
                                  distinct))
         read-index  (h/task history-oks :read-index []
                             (r-index history-oks))
         write-index (h/task history-oks :write-index []
                             (w-index history-oks))

         type-sanity (h/task history-oks :type-sanity []
                             (ct/assert-type-sanity history-oks))
         internal    (h/task history-oks :internal []
                             (internal-cases history-oks))

         indexes     (h/task history-oks :indexes []
                             {:processes   @processes
                              :read-index  @read-index
                              :write-index @write-index
                              :write-kvs   (->> @write-index ; {k {v op}
                                                (map (fn [[k v->ops]]
                                                       [k (set (keys v->ops))]))
                                                (into {}))   ; {k #{vs}}
                              })

         G1a         (h/task history :G1a []
                             (g1a-cases @indexes history))  ; history includes {:type :fail} ops
         G1b         (h/task history-oks :G1b []
                             (g1b-cases @indexes history-oks))

         cycles      (h/task history-oks :cycles []
                             (let [opts (merge opts @indexes)]
                               (->> @processes
                                    (map (fn [process]
                                           (let [task-name (keyword (str "process-" process))]
                                             (h/task history-oks task-name []
                                                     (let [opts (-> opts
                                                                    (assoc  :read-process process)
                                                                    (update :directory str "/process-" process))]
                                                       (:anomalies (ct/cycles! opts (partial graph opts) history-oks)))))))
                                    (map deref)
                                    (apply merge-with conj))))

         _           @type-sanity ; Will throw if problems

         ; Build up anomaly map
         anomalies (cond-> @cycles
                     @internal (assoc :internal @internal)
                     @G1a      (assoc :G1a @G1a)
                     @G1b      (assoc :G1b @G1b))]
     (ct/result-map opts anomalies))))

(defn checker
  "For Jepsen test map."
  [defaults]
  (reify checker/Checker
    (check [_this test history opts]
      (let [opts (merge defaults opts)
            opts (update opts :directory (fn [old]
                                           (if (nil? old)
                                             nil
                                             (store/path test [old]))))]
        (check opts history)))))