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
            [jepsen.txn :as txn]))

(defn lww-realtime-graph
  "The target systems to be tested claim last write == realtime, they do not claim full realtime causal,
   so we order ww realtime.
   
   Real-time-causal consistency.
   An execution e is said to be real time causally consistent (RTC) if its HB graph satisfies the following check in addition to the checks for causal consistency:
     - CC3 Time doesn’t travel backward. For any operations u, v: u.endT ime < v.startT ime ⇒ v 6 ≺G u
   _Consistency, Availability, and Convergence (UTCS TR-11-22)_"
  [history]
  (->> history
       (h/filter #(->> %
                       :value
                       txn/ext-writes
                       seq))
       (ec/realtime-graph)))

(def causal-opts
  "Opts to configure Elle for causal consistency."
  ; rw_register provides:
  ;   - wr, w->r
  ;   - ww and rw dependencies, as derived from a version order
  ;   - initial, nil->r value
  {:consistency-models [:consistent-view]   ; Adya formalism for causal consistency
   :anomalies [:internal]                   ; think Adya requires? add to cm/consistency-models?
   :sequential-keys? true                   ; elle/process-graph
   :wfr-keys? true                          ; rw/wfr-version-graph
   ; TODO: :additional-graphs [lww-realtime-graph]  ; writes are realtime for lww
   })

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

; cannot catch
(def causal-1-mop-anomaly
  (->> [[0 "wx0"]
        [1 "rx0"]
        [1 "wy1"]
        [2 "ry1"]
        [2 "rx_"]]
       (mapcat #(apply op-pair %))
       h/history))

; cannot catch
(def causal-2-mop-anomaly
  (->> [[0 "wx0"]
        [1 "rx0"]
        [1 "wy1"]
        [2 "ry1rx_"]]
       (mapcat #(apply op-pair %))
       h/history))

; {:G-single-item [{:cycle [{:index 5, :time -1, :type :ok, :process 2, :f :txn, :value [[:r :x nil] [:r :y 1]]} {:index 1, :time -1, :type :ok, :process 0, :f :txn, :value [[:w :x 0]]} {:index 3, :time -1, :type :ok, :process 1, :f :txn, :value [[:r :x 0] [:w :y 1]]} {:index 5, :time -1, :type :ok, :process 2, :f :txn, :value [[:r :x nil] [:r :y 1]]}], :steps ({:key :x, :value nil, :value' 0, :type :rw, :a-mop-index 0, :b-mop-index 0} {:type :wr, :key :x, :value 0, :a-mop-index 0, :b-mop-index 0} {:type :wr, :key :y, :value 1, :a-mop-index 1, :b-mop-index 1}), :type :G-single-item}]}
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

; {:cyclic-versions [{:key :x, :scc #{nil 0}, :sources [:initial-state :wfr-keys :sequential-keys]}]}
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

; {:cyclic-versions [{:key :x, :scc #{0 1}, :sources [:initial-state :wfr-keys :sequential-keys]}]}, :not #{:read-uncommitted}, :also-not #{:ROLA :causal-cerone :consistent-view :cursor-stability :forward-consistent-view :monotonic-atomic-view :monotonic-snapshot-read :monotonic-view :parallel-snapshot-isolation :prefix :read-atomic :read-committed :repeatable-read :serializable :snapshot-isolation :strong-read-committed :strong-read-uncommitted :strong-serializable :strong-session-read-committed :strong-session-read-uncommitted :strong-session-serializable :strong-session-snapshot-isolation :strong-snapshot-isolation :update-serializable}
(def monotonic-writes-anomaly
  (->> [[0 "wx0"]
        [0 "wx1"]
        [1 "rx1"]
        [1 "rx0"]]
       (mapcat #(apply op-pair %))
       h/history))

(def wfr-ok
  (->> [[0 "wx0"]
        [1 "rx0"]
        [1 "wy1"]
        [2 "ry1rx0"]]
       (mapcat #(apply op-pair %))
       h/history))

; {:G-single-item [{:cycle [{:index 5, :time -1, :type :ok, :process 2, :f :txn, :value [[:r :y 1] [:r :x nil]]} {:index 1, :time -1, :type :ok, :process 0, :f :txn, :value [[:w :x 0]]} {:index 3, :time -1, :type :ok, :process 1, :f :txn, :value [[:r :x 0] [:w :y 1]]} {:index 5, :time -1, :type :ok, :process 2, :f :txn, :value [[:r :y 1] [:r :x nil]]}], :steps ({:key :x, :value nil, :value' 0, :type :rw, :a-mop-index 1, :b-mop-index 0} {:type :wr, :key :x, :value 0, :a-mop-index 0, :b-mop-index 0} {:type :wr, :key :y, :value 1, :a-mop-index 1, :b-mop-index 0}), :type :G-single-item}]}
(def wfr-anomaly
  (->> [[0 "wx0"]
        [1 "rx0wy1"]
        [2 "ry1rx_"]]
       (mapcat #(apply op-pair %))
       h/history))

(def internal-ok
  (->> (->> [[0 "wx0"]
             [0 "wx1wx2rx2"]]
            (mapcat #(apply op-pair %))
            h/history)
       h/history))

; {:internal ({:op {:index 3, :time -1, :type :ok, :process 0, :f :txn, :value [[:w :x 1] [:w :x 2] [:r :x 1]]}, :mop [:r :x 1], :expected 2})}
(def internal-anomaly
  (->> (->> [[0 "wx0"]
             [0 "wx1wx2rx1"]]
            (mapcat #(apply op-pair %))
            h/history)
       h/history))
