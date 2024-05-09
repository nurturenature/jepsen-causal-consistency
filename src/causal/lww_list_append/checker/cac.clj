(ns causal.lww-list-append.checker.cac
  "A Causal Consistency checker adapted from Consistency, Availability, and Convergence, Mahajan."
  (:require [bifurcan-clj
             [core :as b]
             [graph :as bg]]
            [causal.lww-list-append.checker
             [adya :as cc]
             [graph :as cc-g]]
            [elle
             [core :as elle]
             [graph :as g]]
            [jepsen
             [history :as h]])
  (:import (clojure.lang MapEntry)))

(defn op-tops
  "Given an `op-digraph`, returns a sorted set of top vertices, 
   i.e. vertices with no inbound edges.
   Set is sorted by `Op` `:index`.
   Does not check/care if graph is cyclic."
  [op-digraph]
  (->> op-digraph
       bg/vertices
       (filter #(not (seq (g/in op-digraph %1))))
       (into (sorted-set-by #(compare (:index %1) (:index %2))))))

(defn op-topo-order
  "Given a causal transaction graph, op-digraph, returns the vertices topologically sorted.
   When sorting the graph, non-causal ops will be ordered by their history index.
   Returns nil if graph has cycles."
  [txn-g]
  ;; L ← Empty list that will contain the sorted elements
  ;; S ← Set of all nodes with no incoming edge

  ;; while S is not empty do
  ;; remove a node n from S
  ;; add n to L
  ;; for each node m with an edge e from n to m do
  ;; remove edge e from the graph
  ;; if m has no other incoming edges then
  ;; insert m into S

  ;; if graph has edges then
  ;; return error   (graph has at least one cycle)
  ;; else
  ;; return L   (a topologically sorted order)
  (loop [txn-g        txn-g
         avail-verts  (op-tops txn-g)
         sorted-verts nil]
    (if (seq avail-verts)
      (let [this-vert    (first avail-verts)
            avail-verts  (rest avail-verts)
            sorted-verts (conj sorted-verts this-vert)
            [txn-g
             avail-verts] (->> (g/out txn-g this-vert)
                               (reduce (fn [[txn-g avail-verts] to-vertex]
                                         (let [txn-g       (g/unlink txn-g this-vert to-vertex)
                                               avail-verts (if (not (seq (g/in txn-g to-vertex)))
                                                             (conj avail-verts to-vertex)
                                                             avail-verts)]
                                           [txn-g avail-verts]))
                                       [txn-g avail-verts]))]
        (recur txn-g avail-verts sorted-verts))
      ; done
      (if (seq (bg/edges txn-g))
        nil                        ; graph has cycle(s)
        (reverse sorted-verts))))) ; was built w/conj

(defn op-topo-index
  "Given an `op-digraph`, returns {op topo-order}"
  [txn-g]
  (->> txn-g
       op-topo-order
       (map-indexed (fn [i op] (MapEntry. op i)))
       (into {})))

(defn op-transitive-reduction
  "Given an `op-digraph`, returns the graph transitively reduced."
  [txn-g]

  ;; For a graph with n vertices, m edges, and r edges in the transitive reduction, it is possible to find the transitive reduction using an output-sensitive algorithm in an amount of time that depends on r in place of m. The algorithm is:[6]

  ;;     For each vertex v, in the reverse of a topological order of the input graph:
  ;;         Initialize a set of vertices reachable from v, initially the singleton set {v}.
  ;;         For each edge vw, in topological order by w, test whether w is in the reachable set of v, and if not:
  ;;             Output edge vw as part of the transitive reduction.
  ;;             Replace the set of vertices reachable from v by its union with the reachable set of w.

  ;; The ordering of the edges in the inner loop can be obtained by using two passes of counting sort or another stable sorting algorithm to sort the edges, first by the topological numbering of their end vertex, and secondly by their starting vertex.

  (let [topo-index   (op-topo-index txn-g)]
    (->> topo-index
         (sort-by val)
         (map key)
         reverse
         (reduce (fn [tr-g this-op]
                   (let [reachable #{this-op}
                         this-outs (->> (g/out txn-g this-op)
                                        (sort-by (partial get topo-index)))
                         [tr-g _reachable]
                         (->> this-outs
                              (reduce (fn [[tr-g reachable] out-op]
                                        (if (not (contains? reachable out-op))
                                          [(g/link tr-g this-op out-op (bg/edge txn-g this-op out-op))  ; TODO: includes original rels in link
                                           (-> reachable
                                               (conj out-op)
                                               (into (g/out txn-g out-op)))]
                                          [tr-g reachable]))
                                      [tr-g reachable]))]
                     tr-g))
                 (b/linear (g/op-digraph)))
         b/forked)))

(defn execute-txn->op-state
  "Given an op-state and an op, executes the op into the op-state and returns it."
  [op-state {:keys [index process value] :as op}]
  (comment
    ; op-state
    {:processes {:process :process-index}
     :kvs       {:k [:vs :writer-index]}
     :anomalies :seq})
  (let [prev-p-index (get-in op-state [:processes process] -1)
        ext-reads    (cc-g/ext-reads value)
        ext-writes   (->> (cc-g/ext-writes value) ; map to same structure as :kvs
                          (map (fn [[k vs]]
                                 (MapEntry. k [vs index])))
                          (into {}))

        ; CC1 process order
        op-state (if (> prev-p-index index)
                   (update op-state :anomalies conj {:type    :CC1
                                                     :process process
                                                     :index   prev-p-index
                                                     :index'  index
                                                     :op'     op})
                   op-state)

        ; CC2 read currency
        op-state (->> ext-reads
                      (reduce (fn [op-state [k vs]]
                                (let [mop                 [:r k vs]
                                      v                   (last vs)
                                      [prev-v
                                       prev-writer-index] (get-in op-state [:kvs k] [nil -1])
                                      prev-v              (last prev-v)]
                                  (cond-> op-state
                                    ; valid read, do nothing
                                    (= v prev-v)
                                    identity

                                    ; invalid read
                                    (not= v prev-v)
                                    (update :anomalies conj {:type       :CC2
                                                             :op         op
                                                             :mop        mop
                                                             :expected   [:r k [prev-v]]
                                                             :written-by prev-writer-index}))))
                              op-state))

        ; CC3 real-time
        op-state (->> op-state
                      :processes
                      (remove (fn [[_hb-p hb-index]]
                                (< hb-index index)))
                      (reduce (fn [op-state [hb-p hb-index]]
                                (update op-state :anomalies conj {:type     :CC3
                                                                  :process  hb-p
                                                                  :index    hb-index
                                                                  :process' process
                                                                  :index'   index
                                                                  :op'      op}))
                              op-state))

        ; update state with txn process and writes
        op-state (-> op-state
                     (assoc-in [:processes process] index)
                     (update :kvs
                             (partial merge-with (fn [[kvs-vs _kvs-writer-index] [ext-w-vs ext-w-writer-index]]
                                                   [(into kvs-vs ext-w-vs)
                                                    ext-w-writer-index]))
                             ext-writes))]
    op-state))

(defn merge-op-states
  "Given a sequence of op states, merges them together.
   The merge is done in sequence order with last write wins."
  [op-states]
  (comment
    ; op-state
    {:processes {:process :process-index}
     :kvs       {:k [:v :writer-index]}
     :anomalies :seq})
  (->> op-states
       (reduce (fn
                 ([] nil)
                 ([{:keys [processes kvs anomalies] :as _op-state}
                   {processes' :processes kvs' :kvs anomalies' :anomalies :as _op-state'}]
                  (let [processes (merge-with max processes processes')
                        kvs       (merge-with (fn [[v writer-index] [v' writer-index']]
                                                (cond
                                                  (<= writer-index writer-index')
                                                  [v' writer-index']

                                                  (> writer-index writer-index')
                                                  [v writer-index]))
                                              kvs kvs')
                        anomalies (into anomalies anomalies')]
                    {:processes processes
                     :kvs       kvs
                     :anomalies anomalies}))))))

(def sorted-op-set
  (sorted-set-by (fn [{index :index} {index' :index}]
                   (compare index index'))))

(defn evaluate-tr-g
  "Given a transitively reduced `op-digraph`, returns a sequence of anomalies."
  [tr-g]
  (comment
    ; op-states
    {:op-index {:in-index :in-state}})
  (let [in-play (-> tr-g op-tops seq)]
    (if (not (seq in-play))
      ; cycles in tr-g?
      (seq [{:type :transitive-reduction-graph-cycles?}])
      (loop [in-play   in-play
             op-states nil
             anomalies nil]
        ; more to do?
        (if (seq in-play)
          (let [this-op    (first in-play)
                in-play    (rest in-play)
                this-ins   (->> this-op (g/in tr-g) (map :index) (into (sorted-set)))
                this-ready (->> this-op :index (get op-states) keys (into (sorted-set)))
                this-outs  (->> this-op (g/out tr-g) (into sorted-op-set))]
            ; does this op have all of it's inbound state?
            (if (= this-ins this-ready)
              (let [; merge inbound states
                    op-state  (merge-op-states (->> this-op :index (get op-states) vals))
                    ; execute this-op into it
                    op-state  (execute-txn->op-state op-state this-op)
                    ; lift any anomies from op-state to op-states, don't pass them on
                    anomalies (into anomalies (:anomalies op-state))
                    op-state  (dissoc op-state :anomalies)
                    ; remove this-op
                    op-states (dissoc op-states (:index this-op))]
                ; pass my state to my outs, add outs to in-play 
                (if (seq this-outs)
                  (let [op-states (->> this-outs
                                       (reduce (fn [op-states {out-index :index :as _out-op}]
                                                 (assoc-in op-states [out-index (:index this-op)] op-state))
                                               op-states))
                        in-play   (->> this-outs
                                       reverse
                                       (into in-play))]
                    (recur in-play op-states anomalies))
                  ; no outbound, vertex is a bottom
                  (recur in-play
                         op-states
                         anomalies)))
              ; not ready, let others in-play move traversal forward
              (recur in-play op-states anomalies)))
          ; done
          anomalies)))))

(defn causal-tg
  "Given indexes and a history, returns
   ```
   {:causal-tg           transaction-graph
    :sources             #{sources-used-to-build-graph}
    :cyclic-transactions sequence-of-transaction-cycles}
   ```
   If any transaction source graph has cycles, or would create cycles if combined with other sources,
   it is not combined into the final graph and its cycles are reported."
  [indexes history]
  (let [[tg sources cyclic-transactions]
        (->> [[:process elle/process-graph]
              ;; TODO: cac: confirm cannot ww from version order as each processes doesn't
              ;;            have to see all prev writes in path before seeing this write
              ;; TODO: try wfr only
              [:ww-tg   (partial cc-g/ww-tg indexes)]
              [:wr-tg   (partial cc-g/wr-tg indexes)]]
             (reduce (fn [[tg sources cyclic-transactions] [next-source next-grapher]]
                       ; check this transaction graph individually before combining
                       (let [{next-tg        :graph
                              next-anomalies :anomalies} (next-grapher history)
                             cyclic-transactions         (cond-> cyclic-transactions
                                                           (seq next-anomalies)
                                                           (conj next-anomalies))
                             next-sccs                   (g/strongly-connected-components next-tg)]
                         (if (seq next-sccs)
                           [tg sources (conj cyclic-transactions {:type    :cyclic-transactions
                                                                  :sources #{next-source}
                                                                  :sccs    next-sccs})]
                           ; now try combining
                           (let [combined-sources (conj sources next-source)
                                 combined-tg      (g/digraph-union tg next-tg)
                                 combined-sccs    (g/strongly-connected-components combined-tg)]
                             (if (seq combined-sccs)
                               [tg sources (conj cyclic-transactions {:type    :cyclic-transactions
                                                                      :sources combined-sources
                                                                      :sccs    combined-sccs})]
                               [combined-tg combined-sources cyclic-transactions])))))
                     [(b/linear (g/digraph)) #{} nil]))]
    (merge {:causal-tg (b/forked tg)}
           (when (seq sources)
             {:sources sources})
           (when (seq cyclic-transactions)
             {:cyclic-transactions cyclic-transactions}))))
(defn cac
  "Given a history, returns a sequence of cac anomalies."
  [history]
  (let [{:keys [cyclic-versions]
         :as indexes}                 (cc/indexes history)
        {:keys [causal-tg
                cyclic-transactions]} (causal-tg indexes history)
        tr-g                          (op-transitive-reduction causal-tg)
        cac-anomalies                 (evaluate-tr-g tr-g)]
    (cond-> cac-anomalies
      (seq  cyclic-versions)
      (conj cyclic-versions)

      (seq  cyclic-transactions)
      (conj cyclic-transactions))))

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
  ([_opts history]
   (let [history      (->> history
                           h/client-ops)
         history-oks  (->> history
                           ; TODO: shouldn't be any :info in total sticky availability, handle explicitly
                           h/oks)

         cac          (cac history-oks)]
     (merge {:valid true}
            (when (seq cac)
              {:valid false
               :cac   cac})))))
