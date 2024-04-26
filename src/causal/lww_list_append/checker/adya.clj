(ns causal.lww-list-append.checker.adya
  "A Causal Consistency checker for Last Write Wins List Append registers"
  (:require [bifurcan-clj
             [graph :as bg]]
            [causal.util :as u]
            [causal.lww-list-append.checker.graph :as cc-g]
            [clojure.set :as set]
            [clojure.tools.logging :refer [info]]
            [elle
             [core :as elle]
             [txn :as ct]
             [graph :as g]]
            [jepsen
             [history :as h]]
            [jepsen.history.sim :as h-sim]
            [elle.core :as elle])
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
                   :append [(assoc! state k v) error]
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
  (let [failed (ct/failed-write-indices #{:append} history)]
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
  (let [im (ct/intermediate-write-indices #{:append} history)]
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
  [history-oks]
  (->> history-oks
       (map (fn [{:keys [process] :as _op}]
              process))
       (into #{})))

(defn lww-graph
  "Given options, indexes, and a history that includes invokes and completions, computes a
   ```
   {:graph      transaction-graph
    :explainer  explainer-for-graph
    :anomalies  seq-of-anomalies}
   ```
   of lww dependencies for transactions.
   We combine several pieces:

     - w <hb w', derived from version order
         - nil precedes every [k v] for first interaction with [k] in each process
         - read prefix order
         - Monotonic Writes
         - Writes Follow Reads
         - Monotonic Reads
   
     - real-time order"
  [_opts indexes history]
  (let [; use the `observed-vg` to order ww for a more complete graph
        indexes (assoc indexes
                       :causal-vg  (:observed-vg  indexes)
                       :causal-kvg (:observed-kvg indexes))
        ; Build our combined analyzers
        analyzers [elle/realtime-graph
                   (partial cc-g/ww-tg indexes)]
        analyzer (apply elle/combine analyzers)]
    ; And go!
    (analyzer history)))

(defn graph
  "Given options, indexes, and a history, computes a
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
         - read prefix order
         - Monotonic Writes
         - Writes Follow Reads
         - no Monotonic Reads to determine causal order
   
     - r <hb w, derived from version order
         - read of v, earlier version, <hb write of v', later version
         - can only infer and check one process, `rw-process`, at a time
   
     - additional graphs, as given by (:additional-graphs opts)"
  [opts indexes history-oks]
  (let [; Build our combined analyzers
        analyzers (into [elle/process-graph
                         (partial cc-g/ww-tg indexes)
                         (partial cc-g/wr-tg indexes)
                         (partial cc-g/rw-tg indexes)]
                        (ct/additional-graphs opts))
        analyzer (apply elle/combine analyzers)]
    ; And go!
    (analyzer history-oks)))

(defn indexes
  "Convenience fn for repl."
  [history-oks]
  (let [processes    (h/task history-oks :processes []
                             (processes history-oks))
        write-index  (h/task history-oks :write-index []
                             (cc-g/write-index history-oks))
        read-index   (h/task history-oks :read-index []
                             (cc-g/read-index history-oks))
        causal-vg    (h/task history-oks :causal-vg []
                             (cc-g/causal-vg cc-g/causal-vg-sources history-oks))]
    (merge {:processes     @processes
            :write-index   @write-index
            :read-index    @read-index}
           (select-keys @causal-vg [:causal-vg :causal-kvg :cyclic-versions]))))

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
                              (cc-g/write-index history-oks))
         read-index   (h/task history-oks :read-index []
                              (cc-g/read-index history-oks))

         observed-vg  (h/task history-oks :observed-vg []
                              (cc-g/causal-vg cc-g/observed-vg-sources history-oks))

         causal-vg    (h/task history-oks :causal-vg []
                              (cc-g/causal-vg cc-g/causal-vg-sources history-oks))


         ; derefs below start any blocking

         {observed-vg              :causal-vg
          observed-kvg             :causal-kvg
          observed-cyclic-versions :cyclic-versions} @observed-vg

         {:keys [causal-vg causal-kvg _cyclic-versions]} @causal-vg

         indexes      {:processes     @processes
                       :write-index   @write-index
                       :read-index    @read-index
                       :observed-vg   observed-vg
                       :observed-kvg  observed-kvg
                       :causal-vg     causal-vg
                       :causal-kvg    causal-kvg}

         cycles       (->> @processes
                           (map (fn [process]
                                  (let [opts      (update opts :directory str "/process-" process)
                                        indexes   (assoc indexes :rw-process process)
                                        task-name (keyword (str "cycles-" process))]
                                    (h/task history-oks task-name []
                                            (ct/cycles! opts
                                                        (partial graph opts indexes)
                                                        history-oks)))))
                           (map deref)
                           (map :anomalies)
                           (apply merge-with conj))
         _            @type-sanity ; Will throw if problems

         ; Build up anomaly map
         anomalies (cond-> cycles
                     ;;  @internal     (assoc :internal @internal)
                     ;;  @g1a          (assoc :G1a @g1a)
                     ;;  @g1b          (assoc :G1b @g1b)
                     observed-cyclic-versions (assoc :cyclic-versions observed-cyclic-versions))]
     (ct/result-map opts anomalies))))
