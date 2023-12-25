(ns causal.lww-register
  "A test which looks for cycles in write/read transactions.
   Writes are assumed to be unique, but this is the only constraint.
   See jepsen.tests.cycle.wr and elle.rw-register for docs."
  (:refer-clojure :exclude [test])
  (:require [bifurcan-clj
             [core :as b]
             [graph :as bg]
             [set :as bs]]
            [clojure
             [pprint :refer [pprint]]
             [set :as set]]
            [clojure.tools.logging :refer [info]]
            [elle
             [consistency-model :as cm]
             [core :as ec]
             [graph :as g]
             [list-append :as la]
             [rels :as rels]
             [rw-register :as rw]
             [txn :as et]
             [util :as eu]]
            [jepsen
             [history :as h]
             [txn :as txn]]
            [jepsen.tests.cycle.wr :as wr]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn ww+wr-version-links
  "v < v'
   v <ww v'
   v <rw v'"
  [g v v' ext-read-index ext-write-index]
  (let [v (->> v
               (reduce-kv (fn [acc k vs]
                            (reduce (fn [acc v]
                                      (conj acc [k v]))
                                    acc
                                    vs))
                          []))
        v' (->> v'
                (reduce-kv (fn [acc k vs]
                             (reduce (fn [acc v]
                                       (conj acc [k v]))
                                     acc
                                     vs))
                           []))
        v-writes  (->> v
                       (mapcat (partial get-in ext-write-index))
                       (into #{}))
        v-reads   (->> v
                       (mapcat (partial get-in ext-read-index))
                       (into #{}))
        v'-writes (->> v'
                       (mapcat (partial get-in ext-write-index))
                       (into #{}))
        all-ops   (set/union v-writes v-reads v'-writes)]
    (-> g
        (g/link-all-to-all v-writes v'-writes rels/ww)
        (g/link-all-to-all v-reads  v'-writes rels/rw)
        (g/remove-self-edges all-ops))))

(defrecord WFRMRExplainer [graph]
  ec/DataExplainer
  (explain-pair-data [_ a b]
    (when (try+ (bg/edge graph a b)
                (catch [] _ nil))
      {:type :wfr
       :w  (txn/ext-writes (:value a))
       :w' (txn/ext-writes (:value b))}))

  (render-explanation [_ {:keys [w w']} a-name b-name]
    (str a-name "'s write(s) of " w " were read by " b-name " before its write(s) of " w')))

(defn wfr+mr-order
  "Given a history and a process ID, constructs a partial order graph based on
   writes follow reads and monotonic writes."
  [history process]
  (let [ext-write-index (rw/ext-index txn/ext-writes history)
        ext-read-index  (rw/ext-index txn/ext-reads  history)
        [g _observed-vers _linked-vers]
        (->> history
             (h/filter (comp #{process} :process))
             (reduce (fn [[g observed-vers linked-vers] {:keys [value] :as _op}]
                       (let [this-writes (->> (txn/ext-writes value)
                                              (map (fn [[k v]]
                                                     {k #{v}}))
                                              (into {}))
                             this-reads  (->> (txn/ext-reads value)
                                              (map (fn [[k v]]
                                                     {k #{v}}))
                                              (into {}))
                             observed-vers' (merge-with set/union observed-vers this-reads this-writes)]

                         (if (seq this-writes)
                           (let [new-vers (reduce-kv (fn [acc k v]
                                                       (update acc k set/difference v))
                                                     observed-vers
                                                     linked-vers)]
                             [(ww+wr-version-links g new-vers this-writes ext-read-index ext-write-index)
                              observed-vers' (merge-with set/union linked-vers new-vers)])
                           [g observed-vers' linked-vers])))
                     [(b/linear (g/op-digraph)) {} {}]))]
    (b/forked g)))

(defn wfr+mr-graph
  "Given a history, creates a <ww partial order graph with writes follow reads ordering.
   WFR is per process."
  [history]
  (let [history (->> history
                     h/oks)
        graph (->> history
                   (h/map :process)
                   distinct
                   (map (partial wfr+mr-order history))
                   (apply g/digraph-union))]
    {:graph     graph
     :explainer (WFRMRExplainer. graph)}))

(defn lww-realtime-graph
  "The target systems to be tested claim last write == realtime, they do not claim full realtime causal,
   so we order ww realtime.
   
   Real-time-causal consistency.
   An execution e is said to be real time causally consistent (RTC) if its HB graph satisfies the following check in addition to the checks for causal consistency:
     - CC3 Time doesn’t travel backward. For any operations u, v: u.endT ime < v.startT ime ⇒ v 6 ≺G u
   _Consistency, Availability, and Convergence (UTCS TR-11-22)_"
  [history]
  (let [ext-write-index (rw/ext-index txn/ext-writes history)
        graph (->> ext-write-index
                   (map (fn [[_k vs]]
                          (let [history' (->> vs vals (apply concat) distinct
                                              (mapcat (fn [op]
                                                        [(h/invocation history op) op]))
                                              (sort-by :index)
                                              h/history)
                                graph (ec/realtime-graph history')]
                            (:graph graph))))
                   (apply g/digraph-union))]
    {:graph     graph
     :explainer (ec/->RealtimeExplainer history)}))

(def causal-opts
  "Opts to configure Elle for causal consistency."
  ; rw_register provides:
  ;   - initial nil -> all versions for all keys
  ;   - w->r
  ;   - ww and rw dependencies, as derived from a version order
  {:consistency-models [:strong-session-consistent-view] ; Elle's strong-session with Adya's formalism for causal consistency
   :anomalies [:internal]                                ; basic hygiene
   :sequential-keys? true                                ; infer version order from elle/process-graph
   :wfr-keys? true                                       ; rw/wfr-version-graph within txns
   :additional-graphs [wfr+mr-graph                      ; wfr+mr txn ordering, all txns including disjoint keys
                       ; TODO: LWW
                       ]})

(defn test
  "A partial test, including a generator and a checker.
   You'll need to provide a client which can understand operations of the form:
   ```
    {:type :invoke, :f :txn, :value [[:r 3 nil] [:w 3 6]}
   ```
   and return completions like:
   ```
    {:type :ok, :f :txn, :value [[:r 3 1] [:w 3 6]]}
   ```
   Where the key 3 identifies some register whose value is initially 1, and
   which this transaction sets to 6.

   Options are merged with causal consistency:
   ```
    {:consistency-models [:causal]
     :sequential-keys? true
     :wfr-keys? true}
   ```
   and then passed to elle.rw-register/check and elle.rw-register/gen;
   see their docs for full options."
  ([] (test {}))
  ([opts]
   (let [opts (merge opts
                     causal-opts)]
     {:generator (rw/gen opts)
      :checker   (rw/check opts)}))) ; TODO: real checker

(defn workload
  "A list append workload."
  [opts]
  (-> (wr/test (assoc (select-keys opts [:key-count
                                         :max-txn-length
                                         :max-writes-per-key])
                      :min-txn-length 1
                      :consistency-models [(:expected-consistency-model opts)]))
      ; (assoc :client (Client. nil nil nil))
      ; (update :generator ro-gen)
      ))

(defn op
  "Generates an operation from a string language like so:

  wx1       set x = 1
  ry1       read y = 1
  wx1wx2    set x=1, x=2"
  ([string]
   (let [[txn mop] (reduce (fn [[txn [f k _v :as mop]] c]
                             (case c
                               \w [(conj txn mop) [:w]]
                               \r [(conj txn mop) [:r]]
                               \x [txn (conj mop :x)]
                               \y [txn (conj mop :y)]
                               \z [txn (conj mop :z)]
                               (let [e (if (= \_ c)
                                         nil
                                         (Long/parseLong (str c)))]
                                 [txn [f k e]])))
                           [[] nil]
                           string)
         txn (-> txn
                 (subvec 1)
                 (conj mop))]
     {:process 0, :type :ok, :f :txn :value txn}))
  ([process type string]
   (assoc (op string) :process process :type type)))

(defn fail
  "Fails an op."
  [op]
  (assoc op :type :fail))

(defn invoke
  "Takes a completed op and returns an invocation."
  [completion]
  (-> completion
      (assoc :type :invoke)
      (update :value (partial map (fn [[f k _v :as mop]]
                                    (if (= :r f)
                                      [f k nil]
                                      mop))))))

(defn op-pair
  ([txn] (op-pair 0 txn))
  ([p txn]
   (let [op     (op p :ok txn)
         invoke (invoke op)]
     [invoke op])))

(def causal-ok
  (->> [[0 "wx0"]
        [1 "rx0wy1"]
        [2 "ry1rx0"]]
       (mapcat #(apply op-pair %))
       h/history))

(def causal-2-mops-anomaly
  (->> [[0 "wx0"]
        [1 "rx0wy1"]
        [2 "ry1rx_"]]
       (mapcat #(apply op-pair %))
       h/history))

(def ryw-ok
  (->> [[0 "wx0"]
        [0 "rx0"]]
       (mapcat #(apply op-pair %))
       h/history))

(def ryw-anomaly
  (->> [[0 "wx0"]
        [0 "rx_"]]
       (mapcat #(apply op-pair %))
       h/history))

(def monotonic-writes-ok
  (->> [[0 "wx0"]
        [0 "wx1"]
        [1 "rx0"]
        [1 "rx1"]]
       (mapcat #(apply op-pair %))
       h/history))

(def monotonic-writes-anomaly
  (->> [[0 "wx0"]
        [0 "wx1"]
        [1 "rx1"]
        [1 "rx0"]]
       (mapcat #(apply op-pair %))
       h/history))

(def monotonic-writes-diff-key-ok
  (->> [[0 "wx0"]
        [0 "wx1"]
        [0 "wy2"]
        [1 "ry2"]
        [1 "rx1"]]
       (mapcat #(apply op-pair %))
       h/history))

(def monotonic-writes-diff-key-anomaly
  (->> [[0 "wx0"]
        [0 "wy1"]
        [1 "ry1"]
        [1 "rx_"]]
       (mapcat #(apply op-pair %))
       h/history))

(def wfr-ok
  (->> [[0 "wx0"]
        [1 "rx0"]
        [1 "wy1"]
        [2 "ry1rx0"]]
       (mapcat #(apply op-pair %))
       h/history))

(def wfr-1-mop-anomaly
  (->> [[0 "wx0"]
        [1 "rx0"]
        [1 "wy1"]
        [2 "ry1"]
        [2 "rx_"]]
       (mapcat #(apply op-pair %))
       h/history))

(def wfr-2-mop-anomaly
  (->> [[0 "wx0"]
        [1 "rx0"]
        [1 "wy1"]
        [2 "rx_ry1"]]
       (mapcat #(apply op-pair %))
       h/history))

(def wfr-anomaly
  (->> [[0 "wx0"]
        [1 "rx0wy1"]
        [2 "ry1rx_"]]
       (mapcat #(apply op-pair %))
       h/history))

(def internal-ok
  (->> [[0 "wx0"]
        [0 "wx1wx2rx2"]]
       (mapcat #(apply op-pair %))
       h/history))

(def internal-anomaly
  (->> [[0 "wx0"]
        [0 "wx1wx2rx1"]]
       (mapcat #(apply op-pair %))
       h/history))

(def lww-ok
  (let [[p0-wx0 p0-wx0'] (op-pair 0 "wx0")
        [p1-wx1 p1-wx1'] (op-pair 1 "wx1")
        [p0-rx0 p0-rx0'] (op-pair 0 "rx0")
        [p1-rx1 p1-rx1'] (op-pair 1 "rx1")]
    (->> [p0-wx0
          p1-wx1
          p0-wx0'
          p1-wx1'
          p0-rx0
          p0-rx0'
          p1-rx1
          p1-rx1']
         h/history)))

(def lww-anomaly
  (let [[p0-wx0 p0-wx0'] (op-pair 0 "wx0")
        [p1-wx1 p1-wx1'] (op-pair 1 "wx1")
        [p0-rx0 p0-rx0'] (op-pair 0 "rx0")
        [p0-rx1 p0-rx1'] (op-pair 0 "rx1")
        [p1-rx0 p1-rx0'] (op-pair 1 "rx0")
        [p1-rx1 p1-rx1'] (op-pair 1 "rx1")]
    (->> [p0-wx0
          p1-wx1
          p0-wx0'
          p1-wx1'
          p0-rx1
          p0-rx1'
          p1-rx0
          p1-rx0']
         h/history)))

(def g-monotonic-anomaly
  "Adya Weak Consistency 4.2.2
   Hnon2L : w1 (x 1) w1 (y 1) c1 w2 (y 2) w2 (x 2) w2 (z 2) r3 (x 2) w3 (z 3) r3 (y 1) c2 c3
   [x 1 << x 2 , y 1 << y 2 , z 2 << z 3]"
  (->> [{:process 1 :type :invoke :value [[:w :x 1] [:w :y 1]] :f :txn}
        {:process 1 :type :ok     :value [[:w :x 1] [:w :y 1]] :f :txn}
        {:process 2 :type :invoke :value [[:w :y 2] [:w :x 2] [:w :z 2]] :f :txn}
        {:process 3 :type :invoke :value [[:r :x nil] [:w :z 3] [:r :y nil]] :f :txn}
        {:process 2 :type :ok     :value [[:w :y 2] [:w :x 2] [:w :z 2]] :f :txn}
        {:process 3 :type :ok     :value [[:r :x 2] [:w :z 3] [:r :y 1]] :f :txn}]
       h/history))

(def g-monotonic-list-append-anomaly
  (->> [{:process 1 :type :invoke :value [[:append :x 1] [:append :y 1]] :f :txn}
        {:process 1 :type :ok     :value [[:append :x 1] [:append :y 1]] :f :txn}
        {:process 2 :type :invoke :value [[:append :y 2] [:append :x 2] [:append :z 2]] :f :txn}
        {:process 3 :type :invoke :value [[:r :x nil] [:append :z 3] [:r :y nil]] :f :txn}
        {:process 2 :type :ok     :value [[:append :y 2] [:append :x 2] [:append :z 2]] :f :txn}
        {:process 3 :type :ok     :value [[:r :x [1,2]] [:append :z 3] [:r :y [1]]] :f :txn}]
       h/history))
