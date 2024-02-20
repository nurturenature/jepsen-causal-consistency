(ns causal.gset.causal-consistency
  (:require [bifurcan-clj
             [core :as b]]
            [elle [core :as elle]
             [txn :as ct]
             [graph :as g]
             [rels :refer [ww wr rw]]
             [util :as util :refer [index-of]]]
            [jepsen
             [history :as h]
             [txn :as txn]])
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
                           (update-in idx [k v] conj op))
                         idx
                         (ext-fn (:value op))))
               {})))

(defrecord WRExplainer []
  elle/DataExplainer
  (explain-pair-data [_ a b]
    (let [writes (txn/ext-writes (:value a))
          reads  (txn/ext-reads  (:value b))]
      (reduce (fn [_ [k v]]
                (when (and (contains? reads k)
                           (= v (get reads k)))
                  (reduced
                   {:type  :wr
                    :key   k
                    :value v
                    :a-mop-index (index-of (:value a) [:w k v])
                    :b-mop-index (index-of (:value b) [:r k v])})))
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
  (let [ext-writes (ext-index txn/ext-writes history)
        ext-reads  (ext-index txn/ext-reads  history)]
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

(defn graph
  "Given options and a history, computes a {:graph g, :explainer e} map of
  dependencies. We combine several pieces:

    1. A wr-dependency graph, which we obtain by directly comparing writes and
       reads.

    2. Additional graphs, as given by (:additional-graphs opts).

    3. ww and rw dependencies, as derived from a version order, which we derive
       on the basis of...

       a. nil precedes *every* read value

       b. If either :linearizable-keys? or :sequential-keys? is passed, we
          assume individual keys are linearizable/sequentially consistent, and
          use that to infer (partial) version graphs from either the realtime
          or process order, respectively.

  The graph we return combines all this information.

  TODO: maybe use writes-follow-reads(?) to infer more versions from wr deps?"
  [opts history]
  (let [; Build our combined analyzers
        analyzers (into [elle/process-graph wr-graph]
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
         anomalies (cond-> cycles)]
     (ct/result-map opts anomalies))))

