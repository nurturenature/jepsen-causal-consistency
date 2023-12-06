(ns crdt.lww-wr
  "A test which looks for cycles in write/read transactions.
   Writes are assumed to be unique, but this is the only constraint.
   See jepsen.tests.cycle.wr and elle.rw-register for docs."
  (:refer-clojure :exclude [test])
  (:require [elle.rw-register :as rw]
            [jepsen.history :as h]
            [jepsen.tests.cycle.wr :as wr]))

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
                     {:consistency-models [:causal] ; causal w->r
                                                    ; TODO: what ww, wr graphs?
                      ; TODO? :anomalies
                      ; TODO? :additional-graphs
                      :sequential-keys? true         ; elle/process-graph
                      :wfr-keys? true})]             ; rw/wfr-version-graph
     {:generator (wr/gen opts)
      :checker   (wr/checker opts)})))

(def internal-anomaly
  (->> [{:process 0, :type :invoke, :f :txn, :value [[:w 0 0] [:r 0 nil]]}
        {:process 0, :type :ok,     :f :txn, :value [[:w 0 0] [:r 0 nil]]}
        {:process 1, :type :invoke, :f :txn, :value [[:w 0 1] [:r 0 nil]]}
        {:process 1, :type :ok,     :f :txn, :value [[:w 0 1] [:r 0 0]]}]
       h/history))

(def causal-ok
  (->> [{:process 0, :type :invoke, :f :txn, :value [[:w 0 0]]}
        {:process 0, :type :ok,     :f :txn, :value [[:w 0 0]]}
        {:process 1, :type :invoke, :f :txn, :value [[:w 0 1]]}
        {:process 1, :type :ok,     :f :txn, :value [[:w 0 1]]}
        {:process 2, :type :invoke, :f :txn, :value [[:r 0 nil]]}
        {:process 2, :type :ok,     :f :txn, :value [[:r 0 0]]}
        {:process 2, :type :invoke, :f :txn, :value [[:r 0 nil]]}
        {:process 2, :type :ok,     :f :txn, :value [[:r 0 1]]}]
       h/history))

(def lww-anomaly
  (->> [{:process 0, :type :invoke, :f :txn, :value [[:w 0 0]]}
        {:process 0, :type :ok,     :f :txn, :value [[:w 0 0]]}
        {:process 1, :type :invoke, :f :txn, :value [[:w 0 1]]}
        {:process 1, :type :ok,     :f :txn, :value [[:w 0 1]]}
        {:process 2, :type :invoke, :f :txn, :value [[:r 0 nil]]}
        {:process 2, :type :ok,     :f :txn, :value [[:r 0 1]]}
        {:process 2, :type :invoke, :f :txn, :value [[:r 0 nil]]}
        {:process 2, :type :ok,     :f :txn, :value [[:r 0 0]]}]
       h/history))

(def ryw-anomaly
  (->> [{:process 0, :type :invoke, :f :txn, :value [[:w 0 0]]}
        {:process 0, :type :ok,     :f :txn, :value [[:w 0 0]]}
        {:process 0, :type :invoke, :f :txn, :value [[:r 0 nil]]}
        {:process 0, :type :ok,     :f :txn, :value [[:r 0 nil]]}]
       h/history))

