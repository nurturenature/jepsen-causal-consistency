(ns causal.gset.causal-consistency
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
             [store :as store]])
  (:import (jepsen.history Op)))

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
   {k {#{vs} seq-ops}}
   ```
   for all :ok transactions."
  [history]
  (->> history
       h/oks
       (reduce (fn [index {:keys [value] :as op}]
                 (->> value
                      r-kvm
                      (reduce-kv (fn [index k vs]
                                   (update-in index [k vs] conj op))
                                 index)))
               nil)))

(defn w-index
  "Given a history, returns a write index:
   ```
   {k {v op} ;; writes are unique
   ```
   for all :ok transactions."
  [history]
  (->> history
       h/oks
       (reduce (fn [index {:keys [value] :as op}]
                 ; add every [k v] that was written to the map
                 (->> value
                      w-kvm
                      (reduce-kvm (fn [index k v]
                                    (assert (nil? (get-in index [k v]))
                                            (str "[" k " " v "] for " op " is already in the write index!"))
                                    (assoc-in index [k v] op))
                                  index)))
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
  "Given indexes for a history, and an unused history to match the calling APIs,
   creates a w->r edge for every write of [k v] to all reads of [k v]."
  [{:keys [read-index write-index] :as _opts} _history]
  (let [g (->> write-index ; {k {v op}
               (reduce-nested (fn [g k v write-op]
                                (let [; read-index {k {#{vs} seq-ops}}
                                      read-ops (->> (get read-index k) ; {#{vs} seq-ops}
                                                    (filter (fn [[vs _seq-ops]] (contains? vs v)))
                                                    (mapcat val) ; seq-ops
                                                    (into #{}))
                                      ; don't self link
                                      read-ops (disj read-ops write-op)]
                                  (g/link-to-all g write-op read-ops wr)))
                              (b/linear (g/op-digraph)))
               b/forked)]
    {:graph     g
     :explainer (WRExplainer.)}))

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
  "Given read/write indexes for a history, a read-pov as a set of processes, and an unused history to match the calling APIs,
   creates an inferred r->w edge for:
     - every read of [k #{vs}] by a process in `read=pov`
     - to all writes of [k v] that were not read by any process"
  [{:keys [read-index write-index read-pov] :as _opts} _history]
  (let [all-write-kvs (->> write-index ; {k {v op}
                           (map (fn [[k v->ops]]
                                  [k (set (keys v->ops))]))
                           (into {}))
        g (->> read-index ; {k {#{vs} seq-ops}}
               (reduce-nested (fn [g k vs read-ops]
                                (let [read-ops  (->> read-ops
                                                     (filter (fn [{:keys [process] :as _op}]
                                                               (contains? read-pov process)))
                                                     (into #{}))
                                      unread-vs (set/difference (get all-write-kvs k) vs)
                                      write-ops (->> unread-vs
                                                     (map (fn [v]
                                                            (get-in write-index [k v])))
                                                     (into #{}))
                                      ; don't self link
                                      write-ops (set/difference write-ops read-ops)]
                                  (g/link-all-to-all g read-ops write-ops rw)))
                              (b/linear (g/op-digraph)))
               b/forked)]
    {:graph     g
     :explainer (RWExplainer.)}))

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
                          (->> (get read-index a-k) ; {k {#{vs} seq-ops}}
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
  "Given a write-index, history and a process, create a w->w transaction graph with writes follow reads ordering."
  [{:keys [write-index] :as _opts} history process]
  (let [history (->> history
                     (h/filter (comp #{process} :process)))
        [g _observing]
        (->> history
             (reduce (fn [[g observing] {:keys [value] :as op}]
                       (let [r-kvm        (r-kvm value)
                             w-kvm        (w-kvm  value)
                             before-w-ops (->> observing
                                               (mapcat (fn [[k vs]]
                                                         (->> vs
                                                              (map (fn [v]
                                                                     (get-in write-index [k v]))))))
                                               (into #{}))
                             ; don't link to self
                             before-w-ops (disj before-w-ops op)]
                         (if (seq w-kvm)
                           [(g/link-all-to g before-w-ops op ww)
                            r-kvm]
                           [g
                            (merge-with set/union observing r-kvm)])))
                     [(b/linear (g/op-digraph)) nil]))]
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

(defn graph
  "Given indexes/options and a history, creates an analyzer that computes a {:graph g, :explainer e}
   using causal ordering. We combine several pieces:

     - process graph
   
     - w->r graph, a write of v happens before all reads of v ordering
   
     - w->w graph, writes follow reads ordering

     - r->w graph, infer that all read that don't observe v happen before the write of v
       ordering edges are only created for processes in `read-pov`
   
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
   (let [history    (->> history
                         h/client-ops
                         h/oks)  ; TODO: account for :info ops

         processes  (h/task history :processes []
                            (->> history
                                 (h/map :process)
                                 distinct))
         read-index  (h/task history :read-index' []
                             (r-index history))
         write-index (h/task history :write-index' []
                             (w-index history))

         indexes     {:processes   @processes
                      :read-index  @read-index
                      :write-index @write-index
                      :read-pov    (into #{} @processes)}
         opts        (merge opts indexes)

         type-sanity     (h/task history :type-sanity []
                                 (ct/assert-type-sanity history))
         cycles          (h/task history :cycles []
                                 (->> @processes
                                      (map (fn [process]
                                             (let [opts (assoc opts :read-pov #{process})]
                                               (:anomalies (ct/cycles! opts (partial graph opts) history)))))
                                      (apply merge-with conj)))

         _               @type-sanity ; Will throw if problems

         ; Build up anomaly map
         anomalies (cond-> @cycles
                     ;;  @internal     (assoc :internal @internal)
                     ;;  @g1a          (assoc :G1a @g1a)
                     ;;  @g1b          (assoc :G1b @g1b)
                     )]
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