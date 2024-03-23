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
             [history :as h]])
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
   TODO: does not include nil reads."
  [txn]
  (->> txn
       (reduce (fn [kvm [f k v]]
                 (case f
                   :r (if (seq v)
                        (update kvm k set/union v)
                        kvm)
                   :w kvm))
               {})))

(defn r-kvm'
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
  "Reduces over a kvm with (fn acc k v) for all ks/vs.
   Avoids having to nest reduces."
  [r-fn init-state kvm]
  (->> kvm
       (reduce-kv (fn [acc k vs]
                    (->> vs
                         (reduce (fn [acc v]
                                   (r-fn acc k v))
                                 acc)))
                  init-state)))

(defn r-kvs
  "Given a transaction, returns #{[k v] ...} of all [k v] read by the transaction.
   This is includes self-writes that were read.
   TODO: does not include [k nil] reads. How to accommodate?"
  [txn]
  (->> txn
       (reduce (fn [r-kvs [f k v :as _mop]]
                 (case f
                   :r (->> v
                           (map (fn [v']
                                  [k v']))
                           (into r-kvs))
                   :w r-kvs))
               #{})))

(defn w-kvs
  "Given a transaction, returns #{[k v] ...} of all [k v] written by the transaction.
   This includes multiple writes to the same k with a different v."
  [txn]
  (->> txn
       (reduce (fn [w-kvs [f k v :as _mop]]
                 (case f
                   :w (conj w-kvs [k v])
                   :r w-kvs))
               #{})))

(defn kvs-diff
  "Given kvs and kvs', two #{[k v] ...}, returns
   kvs with:
     - any kv in kvs' removed
     - any kv with k not in kvs' removed"
  [kvs kvs']
  (let [ks' (->> kvs'
                 (reduce (fn [ks' [k' _v']]
                           (conj ks' k'))
                         #{}))
        kvs (set/difference kvs kvs')
        kvs (->> kvs
                 (reduce (fn [kvs [k _v :as kv]]
                           (if (contains? ks' k)
                             kvs
                             (disj kvs kv)))
                         kvs))]
    kvs))

(defn r-index
  "Given a history, returns a read index:
   ```
   {[k v] {process [op ...]} ;; ops are in process order
   ```
   for all :ok transactions."
  [history]
  (->> history
       h/oks
       (reduce (fn [index {:keys [value process] :as op}]
                 ; add every [k v] that was read to the map
                 (->> (r-kvs value)
                      (reduce (fn [index kv]
                                (update-in index [kv process] (fn [old]
                                                                (if (nil? old)
                                                                  [op]
                                                                  (conj old op)))))
                              index)))
               {})))

(defn r-index'
  "Given a history, returns a read index:
   ```
   {k {#{vs} {process [op ...]}}} ;; ops are in history order
   ```
   for all :ok transactions."
  [history]
  (->> history
       h/oks
       (reduce (fn [index {:keys [value process] :as op}]
                 ; add every [k v] that was read to the map
                 (->> value
                      r-kvm'
                      (reduce-kv (fn [index k vs]
                                   (update-in index [k vs process] (fn [old]
                                                                     (if (nil? old)
                                                                       [op]
                                                                       (conj old op)))))
                                 index)))
               nil)))

(defn w-index
  "Given a history, returns a write index:
   ```
   {[k v] op} ;; writes are unique
   ```
   for all :ok transactions."
  [history]
  (->> history
       h/oks
       (reduce (fn [index {:keys [value] :as op}]
                 ; add every [k v] that was written to the map
                 (->> (w-kvs value)
                      (reduce (fn [index kv]
                                (assert (nil? (get index kv)) (str kv " for " op " is already in the index!"))
                                (assoc index kv op))
                              index)))
               {})))

(defn w-index'
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
    (let [a-w-kvs (w-kvs (:value a))
          b-r-kvs (r-kvs (:value b))
          ; first shared kv is fine
          [k v]   (->> (set/intersection a-w-kvs b-r-kvs)
                       first)]
      (when (and k v)
        (let [; 1st read mop of kv is fine
              r-mop (->> (:value b)
                         (reduce (fn [_ [mf mk mv _as mop]]
                                   (if (and (= mf :r)
                                            (= mk k)
                                            (contains? mv v))
                                     (reduced mop)
                                     nil))))]
          {:type  :wr
           :key   k
           :value v
           :a-mop-index (index-of (:value a) [:w k v])
           :b-mop-index (index-of (:value b) r-mop)}))))

  (render-explanation [_ {:keys [k v]} a-name b-name]
    (str a-name " wrote [" (pr-str k) " " (pr-str v) "]"
         ", which was read by " b-name " (w->r)")))

(defn wr-graph
  "Given indexes for a history, and an unused history to match the calling APIs,
   creates a w->r edge for every write of [k v] to the first read of [k v] in a process.
  
   Only create an edge to the first read of [k v] in a process as the process order graph
   will transitively order any successive reads of [k v] in that process."
  [{:keys [read-index write-index] :as _indexes} _history]
  (let [g (->> write-index
               (reduce (fn [g [kv write-op]]
                         (let [read-ops (->> (get kv read-index)
                                             (map (fn [[_process ops]]
                                                    (first ops)))
                                             (into #{}))
                               ; don't self link
                               read-ops (disj read-ops write-op)]
                           (if (seq read-ops)
                             (g/link-to-all write-op read-ops wr)
                             g)))
                       (b/linear (g/op-digraph))))]
    {:graph     (b/forked g)
     :explainer (WRExplainer.)}))

(defrecord RYWExplainer []
  elle/DataExplainer
  (explain-pair-data [_ a b]
    (let [a-process  (:process a)
          b-process  (:process b)
          a-reads    (r-kvs (:value a))
          b-writes   (w-kvs (:value b))
          [k v]      (first (set/difference b-writes a-reads))]
      (when (and (= a-process b-process)
                 k v)
        (let [; first mop that didn't read k v
              read-mop (->> (:value a)
                            (reduce (fn [_ [mf mk mv :as mop]]
                                      (case mf
                                        :r (if (and (= mk k)
                                                    (not (contains? mv v)))
                                             (reduced mop)
                                             nil)
                                        :w nil))))]
          {:type        :rw
           :a-mop-index (index-of (:value a) read-mop)
           :b-mop-index (index-of (:value b) [:w k v])
           :process     a-process
           :k           k
           :v           v}))))

  (render-explanation [_ {:keys [process k v]} a-name b-name]
    (str " process " process ", " a-name "'s read of key " k " did not observe " b-name "'s write of " v " (ryw)")))

(defn ryw-order
  "Given a history and a process, create a r->w transaction graph with read your writes ordering."
  [write-index history process]
  (let [history     (->> history
                         (h/filter (comp #{process} :process)))
        write-index (->> write-index
                         (filter (fn [[_kv op]]
                                   (= process (:process op))))
                         (into {}))
        all-w-kvs   (->> write-index
                         keys
                         (into #{}))]
    (->> history
         (reduce (fn [g {:keys [value] :as op}]
                   (let [r-kvs      (r-kvs value)
                         unread-kvs (when (seq r-kvs)
                                      (kvs-diff all-w-kvs r-kvs))
                         ; first in process order op whose's writes were not read
                         unread-op (->> unread-kvs
                                        (map (partial get write-index))
                                        ; don't link to self
                                        (remove (partial = op))
                                        (reduce (fn
                                                  ([] nil)
                                                  ([op op']
                                                   (if (< (:index op) (:index op'))
                                                     op
                                                     op')))))]
                     (if unread-op
                       (g/link g op unread-op rw)
                       g)))
                 (b/linear (g/op-digraph)))
         b/forked)))

(defn ryw-graph
  "Given a history, creates a r->w transaction graph with read your writes ordering in each process."
  [{:keys [processes write-index] :as _opts} history]
  (let [graph     (->> processes
                       (map (partial ryw-order write-index history))
                       (apply g/digraph-union))]
    {:graph     graph
     :explainer (RYWExplainer.)}))

(defrecord WFRExplainer [read-index]
  elle/DataExplainer
  (explain-pair-data [_ a b]
    (let [a-writes  (w-kvs (:value a))
          b-writes  (w-kvs (:value b))
          b-index   (:index b)
          b-process (:process b)]
      (when (and (seq a-writes)
                 (seq b-writes))
        ; did b already read any of a's writes?
        ; if so, first found is fine
        (let [[k v] (->> a-writes
                         (reduce (fn [_ kv]
                                   (let [b-read (->> (get-in read-index [kv b-process])
                                                     first)]
                                     (when (and b-read
                                                (< (:index b-read) b-index))
                                       (reduced kv))))
                                 nil))]
          (when (and k v)
            (let [; any write is fine
                  [bk bv] (first b-writes)]
              {:type        :ww
               :a-mop-index (index-of (:value a) [:w k v])
               :b-mop-index (index-of (:value b) [:w bk bv])
               :process     b-process
               :k           k
               :v           v}))))))

  (render-explanation [_ {:keys [process k v]} a-name b-name]
    (str a-name "'s write of " [k v] " was observed by process " process " before it executed " b-name " (wfr)")))

(defn wfr-order
  "Given a write-index, history and a process, create a w->w transaction graph with writes follow reads ordering.
   
   Only link to other process' happened before writes once.
   Process order transitively includes succeeding writes for the process."
  [{:keys [write-index] :as _opts} history process]
  (let [history (->> history
                     (h/filter (comp #{process} :process)))
        [g _observing _linked]
        (->> history
             (reduce (fn [[g observing linked] {:keys [value] :as op}]
                       (let [r-kvs        (r-kvs value)
                             w-kvs        (w-kvs value)
                             before-w-ops (when (seq w-kvs)
                                            (->> observing
                                                 (map (partial get write-index))
                                                 ;; TODO: will be nil if wasn't in write-index
                                                 ;;       confirm caught in strong convergence
                                                 ;;       report as :anomalies
                                                 (remove nil?)
                                                 (into #{})))
                             ; don't link to self
                             before-w-ops (disj before-w-ops op)]
                         (if (seq before-w-ops)
                           [(g/link-all-to g before-w-ops op ww)
                            r-kvs
                            (set/union linked observing)]
                           [g
                            (set/union observing r-kvs)
                            linked])))
                     [(b/linear (g/op-digraph)) #{} #{}]))]
    (b/forked g)))

(defn wfr-graph
  "Given processes, read/write index, and a history,
   creates a w->w transaction graph with writes follows reads ordering for each process."
  [{:keys [processes read-index write-index] :as _opts} history]
  (let [graph (->> processes
                   (map (partial wfr-order {:write-index write-index} history))
                   (apply g/digraph-union))]
    {:graph     graph
     :explainer (WFRExplainer. read-index)}))

(defn causal-version-order
  "Given a history and a process, create a [k v]->[k v]' graph with:
     - [k nil] precedes the first [k v], write or read, observed for each k 
     - monotonic writes
     - writes follow reads
     - monotonic reads
   ordering for the process.
   
   Returns {process {:vg version-graph :anomalies anomalies}}."
  [history process]
  (let [write-index (w-index history)  ; TODO: move all indexes to check and pass opts
        history     (->> history
                         (h/filter (comp #{process} :process)))
        state       {:vg (b/linear (g/digraph))}

        ; [k nil] -> [k v]
        state
        (->> history
             (reduce (fn [{:keys [observed-ks] :as state} {:keys [value] :as _op}]
                       (let [w-kvm   (w-kvm value)
                             r-kvm   (r-kvm value)
                             txn-kvm (merge-with set/union w-kvm r-kvm)
                             new-ks  (set/difference (set (keys txn-kvm)) observed-ks)
                             new-kvs (->> new-ks
                                          (mapcat (fn [k]
                                                    (->> (get txn-kvm k)
                                                         (map (fn [v] [k v]))))))
                             nil-kvs (->> new-ks
                                          (map (fn [k] [k nil])))]
                         (if (seq new-ks)
                           (-> state
                               (update :vg g/link-all-to-all nil-kvs new-kvs)
                               (update :observed-ks set/union new-ks))
                           state)))
                     (assoc state :observed-ks #{})))

        ; monotonic writes
        state
        (->> history
             (reduce (fn [{:keys [prev-writes] :as state} {:keys [value] :as _op}]
                       (let [w-kvs (w-kvs value)]
                         (if (seq w-kvs)
                           (-> state
                               (update :vg g/link-all-to-all prev-writes w-kvs)
                               (assoc :prev-writes w-kvs))
                           state)))
                     (assoc state :prev-writes #{})))

        ; writes follow reads
        state
        (->> history
             (reduce (fn [{:keys [observed] :as _state} {:keys [value] :as _op}]
                       (let [w-kvs (w-kvs value)
                             r-kvs (r-kvs value)]
                         (if (seq w-kvs)
                           (-> state
                               (update :vg g/link-all-to-all observed w-kvs)
                               (assoc :observed r-kvs))
                           (update state :observed set/union r-kvs))))
                     (assoc state  :observed #{})))

        ; monotonic reads anomalies
        ; simple superset per key
        state
        (->> history
             (reduce (fn [{:keys [prev-reads] :as state} {:keys [value index] :as _op}]
                       (if-let [r-kvm (r-kvm value)]
                         (let [state
                               (->> r-kvm
                                    (reduce-kv (fn [state r-k r-vs]
                                                 (let [prev-vs   (get prev-reads r-k)
                                                       missed-vs (set/difference prev-vs r-vs)]
                                                   (if (seq missed-vs)
                                                     (update state :anomalies (partial merge-with conj) {:monotonic-reads
                                                                                                         {:process process
                                                                                                          :index   index
                                                                                                          :kv      [r-k r-vs]
                                                                                                          :missing missed-vs}})
                                                     state)))
                                               state))]
                           (update state :prev-reads merge r-kvm))
                         state))
                     state))

        ; monotonic reads version order
        ; interpret read values relative to process that wrote the value for new/missed, but it's expensive?
        ; observed is {p {k #{vs}}}
        state
        (->> history
             (reduce (fn [{:keys [observed] :as state} {:keys [value] :as _op}]
                       (if-let [r-kvm (r-kvm value)]
                         (let [r-kvm (->> r-kvm
                                          (reduce-kv (fn [r-kvm k vs]
                                                       (->> vs
                                                            (reduce (fn [r-kvm v]
                                                                      (let [process (:process (get write-index [k v]))]
                                                                        (update-in r-kvm [process k] set/union #{v})))
                                                                    r-kvm)))
                                                     {}))]
                           (->> r-kvm
                                (reduce-kv (fn [state p kvm]
                                             (->> kvm
                                                  (reduce-kv (fn [state k vs]
                                                               (let [prev-vs (get-in observed [p k])
                                                                     new-vs  (set/difference vs prev-vs)
                                                                     prev-vs (->> prev-vs
                                                                                  (map (fn [v] [k v]))
                                                                                  (into #{}))
                                                                     new-vs  (->> new-vs
                                                                                  (map (fn [v] [k v]))
                                                                                  (into #{}))]
                                                                 (-> state
                                                                     (update :vg g/link-all-to-all prev-vs new-vs)
                                                                     (assoc-in [:observed p k] vs))))
                                                             state)))
                                           state)))
                         state))
                     (assoc state  :observed {})))

        state (update state :vg b/forked)]
    {process (select-keys state [:vg :anomalies])}))

(defn causal-versions
  "Given a history,
   returns a sequence of anomalies or nil by checking for causal ordering in each process."
  [history]
  (let [history   (->> history
                       h/oks)
        processes (h/task history :processes []
                          (->> history
                               (h/map :process)
                               distinct))

        ; build version graphs, may find anomalies
        vgs       (->> @processes
                       (map (partial causal-version-order history))
                       (apply merge))
        anomalies (->> vgs
                       (map (fn [[_process {:keys [anomalies]}]] anomalies))
                       (filter seq)
                       (apply (partial merge-with conj)))

        ; check for cycles in each process graph, if none try to combine graphs
        {:keys [_sources _vgs sccs]}
        (->> vgs
             (reduce (fn [{:keys [sources vgs sccs] :as state} [process {:keys [vg]}]]
                       (let [scc (g/strongly-connected-components vg)]
                         (if (seq scc)
                           (update-in state [:sccs :scc] conj {:processes #{process}
                                                               :scc       scc})
                           (let [sources' (conj sources process)
                                 vgs'     (g/digraph-union vgs vg)
                                 scc'     (g/strongly-connected-components vgs')]
                             (if (seq scc')
                               (update-in state [:sccs :scc] conj {:processes sources'
                                                                   :scc       scc'})
                               (assoc state
                                      :sources sources'
                                      :vgs     vgs'))))))
                     {:sources #{}
                      :vgs     (b/linear (g/digraph))}))

        anomalies (concat anomalies sccs)]
    (when (seq anomalies)
      anomalies)))

(defn graph
  "Given options and a history, computes a {:graph g, :explainer e} map of
   dependencies. We combine several pieces:

     - process graph
   
     - w->r graph, a write of v happens before all reads of v ordering
   
     - r->w graph, read your writes within a process ordering
   
     - w->w graph, writes follow reads ordering

     - additional graphs, as given by (:additional-graphs opts).

   The graph we return combines all this information.
   
   TODO: account for :info ops."
  [opts history]
  (let [history     (->> history
                         h/oks)
        processes   (h/task history :processes []
                            (->> history
                                 (h/map :process)
                                 distinct))
        read-index  (h/task history :read-index []
                            (r-index history))
        write-index (h/task history :write-index []
                            (w-index history))
        read-index'  (h/task history :read-index' []
                             (r-index' history))
        write-index' (h/task history :write-index' []
                             (w-index' history))
        indexes      {:read-index  @read-index'
                      :write-index @write-index'}

        ; Build our combined analyzers
        analyzers (into [elle/process-graph
                         (partial wr-graph     {:read-index @read-index :write-index @write-index})
                         (partial ryw-graph    {:processes @processes :write-index @write-index})
                         (partial wfr-graph    {:processes @processes :read-index @read-index :write-index @write-index})]
                        (ct/additional-graphs opts))
        analyzer (apply elle/combine analyzers)]
    ; And go!
    (analyzer history)))

(defn check
  "Full checker for write-read registers. Options are:

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

    :sequential-keys?       Assume that each key is independently sequentially
                            consistent, and use each processes' transaction
                            order to derive a version order.

    :linearizable-keys?     Assume that each key is independently linearizable,
                            and use the realtime process order to derive a
                            version order.

    :wfr-keys?              Assume that within each transaction, writes follow
                            reads, and use that to infer a version order.

    :directory              Where to output files, if desired. (default nil)

    :plot-format            Either :png or :svg (default :svg)

    :plot-timeout           How many milliseconds will we wait to render a SCC
                            plot?

    :max-plot-bytes         Maximum size of a cycle graph (in bytes of DOT)
                            which we're willing to try and render.
"
  ([history]
   (check {} history))
  ([opts history]
   (let [history         (h/client-ops history)
         type-sanity     (h/task history :type-sanity []
                                 (ct/assert-type-sanity history))
         causal-versions (h/task history :causal-versions []
                                 (causal-versions history))
         cycles          (h/task history :cycles []
                                 (:anomalies (ct/cycles! opts (partial graph opts) history)))
         _               @type-sanity ; Will throw if problems
         ; Build up anomaly map
         anomalies (cond-> @cycles
                     @causal-versions (assoc :cyclic-versions @causal-versions)
                    ;;  @internal     (assoc :internal @internal)
                    ;;  @g1a          (assoc :G1a @g1a)
                    ;;  @g1b          (assoc :G1b @g1b)
                    ;;  @lost-update  (assoc :lost-update @lost-update)
                     )]
     (ct/result-map opts anomalies))))

(defn checker
  "For Jepsen test map."
  [defaults]
  (reify checker/Checker
    (check [_this _test history opts]
      (let [opts (merge defaults opts)]
        (check opts history)))))