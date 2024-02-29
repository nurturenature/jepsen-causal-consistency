(ns causal.gset.causal-consistency
  (:require [bifurcan-clj
             [core :as b]]
            [clojure.set :as set]
            [elle [core :as elle]
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

(defn r-mop->kv-set
  [[f k v :as _r-mop]]
  (assert (= :r f))
  (->> v
       (reduce (fn [acc v']
                 (conj acc [k v']))
               #{})))

(defn kv-col->kv-map
  [kv-col]
  (->> kv-col
       (reduce (fn [acc [k v]]
                 (update acc k (fn [old]
                                 (if (nil? old)
                                   #{v}
                                   (conj old v)))))
               {})))

(defn ext-reads
  "Given a transaction, returns a map of {k #{v ...}} for its external reads:
  values that transaction observed which it did not write itself."
  [txn]
  (loop [ext      #{}
         ignore?  #{}
         txn      txn]
    (if (seq txn)
      (let [[f k v :as mop] (first txn)]
        (recur (case f
                 :r (set/union ext (set/difference (r-mop->kv-set mop) ignore?))
                 :w ext)
               (case f
                 :r ignore?
                 :w (conj ignore? [k v]))
               (next txn)))
      (kv-col->kv-map ext))))

(defn ext-writes
  "Given a transaction, returns the map of {k #{v ...}} for its external writes."
  [txn]
  (loop [ext #{}
         txn txn]
    (if (seq txn)
      (let [[f k v] (first txn)]
        (recur (case f
                 :r ext
                 :w (conj ext [k v]))
               (next txn)))
      (kv-col->kv-map ext))))

(defn ext-index
  "Given a function that takes a txn and returns a map of external keys to
  written values for that txn, and a history, computes a map like {k {v [op1,
  op2, ...]}}, where k is a key, v is a particular value for that key, and op1,
  op2, ... are operations which externally wrote k=v.

  Right now we index only :ok ops. Later we should do :infos too, but we need
  to think carefully about how to interpret the meaning of their nil reads.

  TODO: we fail to index external writes of type :info. That's bad."
  [ext-fn history]
  (->> history
       h/oks
       (reduce (fn [idx op]
                 (reduce (fn [idx [k v]]
                           (reduce (fn [idx v']
                                     (update-in idx [k v'] conj op))
                                   idx
                                   v))
                         idx
                         (ext-fn (:value op))))
               {})))

(defrecord WRExplainer []
  elle/DataExplainer
  (explain-pair-data [_ a b]
    (let [writes (ext-writes (:value a))
          reads  (ext-reads  (:value b))]
      ; v's are #{}
      (reduce (fn [_ [wk wv]]
                (when (and (contains? reads wk)
                           (seq (set/intersection wv (get reads wk))))
                  ; there may be more than one write that was read, first is fine
                  ; similar with reads, first is also fine
                  (let [v'    (first (set/intersection wv (get reads wk)))
                        r-mop (->> (:value b)
                                   (reduce (fn [_ [f rk rv :as mop]]
                                             (case f
                                               :r (if (and (= rk wk)
                                                           (contains? rv v'))
                                                    (reduced mop)
                                                    nil)
                                               :w nil))))]
                    (reduced
                     {:type  :wr
                      :key   wk
                      :value v'
                      :a-mop-index (index-of (:value a) [:w wk v'])
                      :b-mop-index (index-of (:value b) r-mop)}))))
              nil
              writes)))

  (render-explanation [_ {:keys [key value]} a-name b-name]
    (str a-name " wrote " (pr-str key) " = " (pr-str value)
         ", which was read by " b-name)))

(defn wr-graph
  "Given a history where ops are txns (e.g. [[:r :x 2] [:w :y 3]]), constructs
  an order over txns based on the external writes and reads of key k: any txn
  that reads value v must come after the txn that wrote v."
  [history]
  (let [ext-writes (ext-index ext-writes history)
        ext-reads  (ext-index ext-reads  history)]
    ; Take all reads and relate them to prior writes.
    {:graph
     (b/forked
      (reduce (fn [graph [k values->reads]]
                 ; OK, we've got a map of values to ops that read those values
                (reduce (fn [graph [v reads]]
                           ; Find ops that set k=v
                          (let [writes (-> ext-writes (get k) (get v))]
                            (case (count writes)
                               ; Huh. We read a value that came out of nowhere.
                               ; This is probably an initial state. Later on
                               ; we could do something interesting here, like
                               ; enforcing that there's only one of these
                               ; values and they have to precede all writes.
                              0 graph

                               ; OK, in this case, we've got exactly one
                               ; txn that wrote this value, which is good!
                               ; We can generate dependency edges here!
                              1 (g/link-to-all graph (first writes) reads wr)

                               ; But if there's more than one, we can't do this
                               ; sort of cycle analysis because there are
                               ; multiple alternative orders. Technically, it'd
                               ; be legal to ignore these, but I think it's
                               ; likely the case that users will want a big
                               ; flashing warning if they mess this up.
                              (assert (< (count writes) 2)
                                      (throw (IllegalArgumentException.
                                              (str "Key " (pr-str k)
                                                   " had value " (pr-str v)
                                                   " written by more than one op: "
                                                   (pr-str writes))))))))
                        graph
                        values->reads))
              (b/linear (g/op-digraph))
              ext-reads))
     :explainer (WRExplainer.)}))

(defrecord RWRYWExplainer []
  elle/DataExplainer
  (explain-pair-data [_ a b]
    (let [a-process (:process a)
          b-process (:process b)
          ; nil reads count
          a-reads   (->> (:value a)
                         (reduce (fn [acc [f k v :as _mop]]
                                   (case f
                                     :r (assoc acc k v)
                                     :w acc))
                                 {}))
          b-writes  (ext-writes (:value b))
          common-k  (set/intersection (set (keys a-reads)) (set (keys b-writes)))]
      (when (and (= a-process b-process)
                 (seq a-reads)
                 (seq b-writes)
                 (seq common-k))
        (->> common-k
             (reduce (fn [_ k]
                       (let [missing (set/difference (get b-writes k) (get a-reads k))
                             v       (first missing) ; any v will do
                             a-mop (->> (:value a)
                                        (reduce (fn [_ [mf mk mv :as mop]]
                                                  (when (and (= :r mf)
                                                             (= k mk)
                                                             (not (contains? mv v)))
                                                    (reduced mop)))
                                                nil))]
                         (when (seq missing)
                           (reduced {:type        :rw
                                     :a-mop-index (index-of (:value a) a-mop)
                                     :b-mop-index (index-of (:value b) [:w k v])
                                     :process     a-process
                                     :k           k
                                     :v           v}))))
                     nil)))))

  (render-explanation [_ {:keys [process k v]} a-name b-name]
    (str "in process " process ", " a-name "'s read of key " k " did not observe " b-name "'s write of " v)))

(defn rw-ryw-order
  "Given a history and a process, create a r->w transaction graph with read your writes ordering."
  [history process]
  (let [history         (->> history
                             (h/filter (comp #{process} :process)))
        ext-write-index (ext-index ext-writes history)
        all-w's         (->> ext-write-index
                             (mapcat (fn [[k v->ops]]
                                       (map (fn [[v _ops]]
                                              [k v])
                                            v->ops)))
                             kv-col->kv-map)]
    (->> history
         ct/op-mops
         (reduce (fn [g [op [f k v :as _mop]]]
                   (case f
                     :r (let [unread-w's (set/difference (get all-w's k) v)
                              unread-ops (->> unread-w's
                                              (reduce (fn [acc v]
                                                        (->> [k v]
                                                             (get-in ext-write-index)
                                                             first
                                                             (conj acc)))
                                                      #{}))
                              ; don't link to self
                              unread-ops (disj unread-ops op)]
                          (if (seq unread-ops)
                            (g/link-to-all g op unread-ops rw)
                            g))
                     :w g))
                 (b/linear (g/op-digraph)))
         b/forked)))

(defn rw-ryw-graph
  "Given a history, creates a r->w transaction graph with read your writes ordering in each process.
   TODO: account for :info writes"
  [history]
  (let [history   (h/oks history)
        processes (->> history
                       (h/map :process)
                       distinct)
        graph     (->> processes
                       (map (partial rw-ryw-order history))
                       (apply g/digraph-union))]
    {:graph     graph
     :explainer (RWRYWExplainer.)}))

(defn graph
  "Given options and a history, computes a {:graph g, :explainer e} map of
   dependencies. We combine several pieces:

     - process graph
   
     - w->r graph, a write of v happen before all reads of v ordering
   
     - r->w graph, read your writes within a process ordering

     - additional graphs, as given by (:additional-graphs opts).

    3. ww and rw dependencies, as derived from a version order, which we derive
       on the basis of...

       a. nil precedes *every* read value

       b. If either :linearizable-keys? or :sequential-keys? is passed, we
          assume individual keys are linearizable/sequentially consistent, and
          use that to infer (partial) version graphs from either the realtime
          or process order, respectively.

   The graph we return combines all this information."
  [opts history]
  (let [; Build our combined analyzers
        analyzers (into [elle/process-graph
                         wr-graph
                         rw-ryw-graph]
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
   (let [history      (h/client-ops history)
         type-sanity  (h/task history :type-sanity []
                              (ct/assert-type-sanity history))
         cycles       (:anomalies (ct/cycles! opts (partial graph opts)
                                              history))
         _            @type-sanity ; Will throw if problems
         ; Build up anomaly map
         anomalies (cond-> cycles
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