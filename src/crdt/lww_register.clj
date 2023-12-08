(ns crdt.lww-register
  "A test which looks for cycles in write/read transactions.
   Writes are assumed to be unique, but this is the only constraint.
   See jepsen.tests.cycle.wr and elle.rw-register for docs."
  (:refer-clojure :exclude [test])
  (:require [clojure.pprint :refer [pprint]]
            [clojure.set :as set]
            [elle.consistency-model :as cm]
            [elle.core :as ec]
            [elle.rw-register :as rw]
            [elle.txn :as et]
            [jepsen.history :as h]
            [jepsen.tests.cycle.wr :as wr]))

(defn lww-realtime-graph
  "The target systems to be tested claim last write == realtime, they do not claim full realtime causal.
   
   Real-time-causal consistency.
   An execution e is said to be real time causally consistent (RTC) if its HB graph satisfies the following check in addition to the checks for causal consistency:
     - CC3 Time doesn’t travel backward. For any operations u, v: u.endT ime < v.startT ime ⇒ v 6 ≺G u
   _Consistency, Availability, and Convergence (UTCS TR-11-22)_"
  [history]
  (->> history
       h/possible
       (h/filter (fn [op]
                   (->> op
                        :value
                        (reduce (fn [_acc [f _k _v]]
                                  (if (#{:w} f)
                                    (reduced true)
                                    false))
                                false))))
       (ec/realtime-graph)))

(def causal-opts
  "Opts to configure Elle for causal consistency."
  ; w->r done by rw_register
  {:consistency-models [:consistent-view]     ; Adya formalism for causal consistency
   :anomalies [:internal]                   ; think Adya requires? add to cm/consistency-models?
   :sequential-keys? true                   ; elle/process-graph
   :wfr-keys? true                          ; rw/wfr-version-graph
   :additional-graphs [lww-realtime-graph]} ; writes are realtime for lww 
  )

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
     {:generator (wr/gen opts)
      :checker   (wr/checker opts)})))

(defn op
  "Generates an operation from a string language like so:

  wx1       set x = 1
  ry1       read y = 1
  wx1wx2    set x=1, x=2"
  ([string]
   (let [[txn mop] (reduce (fn [[txn [f k v :as mop]] c]
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
      (update :value (partial map (fn [[f k v :as mop]]
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
        [2 "rx0ry1"]]
       (mapcat #(apply op-pair %))
       h/history))

(def causal-anomaly
  (->> [[0 "wx0"]
        [1 "rx0wy1"]
        [2 "rx_ry1"]]
       (mapcat #(apply op-pair %))
       h/history))

(def internal-ok
  (->> (op-pair "wx0wx1rx1")
       h/history))

(def internal-anomaly
  (->> (op-pair "wx1rx0")
       h/history))

(def ryw-ok
  (->> [[0 "wx0"]
        [0 "wx1"]
        [0 "rx1"]]
       (mapcat #(apply op-pair %))
       h/history))

(def ryw-anomaly
  (->> [[0 "wx0"]
        [0 "wx1"]
        [0 "rx0"]]
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

(def wfr-ok
  (->> [[0 "wx0"]
        [1 "rx0wy1"]
        [2 "rx0ry1"]]
       (mapcat #(apply op-pair %))
       h/history))

(def wfr-anomaly
  (->> [[0 "wx0"]
        [1 "rx0wy1"]
        [2 "rx_ry1"]]
       (mapcat #(apply op-pair %))
       h/history))

