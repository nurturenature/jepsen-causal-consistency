(ns causal.checker.graph-test
  (:require [clojure.test :refer [deftest is testing]]
            [elle.graph :as g]
            [jepsen.history :as h]))

(def sample-txns-index
  {1  (h/op {:process 0, :type :ok, :f :txn, :value [[:r :x nil]], :index 1})
   2  (h/op {:process 0, :type :ok, :f :txn, :value [[:r :x nil]], :index 2})
   3  (h/op {:process 0, :type :ok, :f :txn, :value [[:r :x nil]], :index 3})
   4  (h/op {:process 0, :type :ok, :f :txn, :value [[:r :x nil]], :index 4})
   5  (h/op {:process 0, :type :ok, :f :txn, :value [[:r :x nil]], :index 5})
   6  (h/op {:process 0, :type :ok, :f :txn, :value [[:r :x nil]], :index 6})
   7  (h/op {:process 0, :type :ok, :f :txn, :value [[:r :x nil]], :index 7})
   8  (h/op {:process 0, :type :ok, :f :txn, :value [[:r :x nil]], :index 8})
   9  (h/op {:process 0, :type :ok, :f :txn, :value [[:r :x nil]], :index 9})
   10 (h/op {:process 0, :type :ok, :f :txn, :value [[:r :x nil]], :index 10})
   11 (h/op {:process 0, :type :ok, :f :txn, :value [[:r :x nil]], :index 11})})

(defn sample-txn-link
  [g index index']
  (g/link g (get sample-txns-index index) (get sample-txns-index index')))

(def sample-txn-g
  ;; topo order: 3, 5, 7, 8, 11, 2, 9, 10 (smallest-numbered available vertex first)
  (-> (g/op-digraph)
      (sample-txn-link 3  8)
      (sample-txn-link 3 10)
      (sample-txn-link 5 11)
      (sample-txn-link 7  8)
      (sample-txn-link 7 11)
      (sample-txn-link 8  9)
      (sample-txn-link 11 2)
      (sample-txn-link 11 9)
      (sample-txn-link 11 10)))

(def sample-txn-g2
  ;; 1, 2, 3, 4, 5 (smallest-numbered available vertex first)
  (-> (g/op-digraph)
      (sample-txn-link 1  2)
      (sample-txn-link 1  3)
      (sample-txn-link 1  4)
      (sample-txn-link 1  5)
      (sample-txn-link 2  4)
      (sample-txn-link 3  4)
      (sample-txn-link 3  5)
      (sample-txn-link 4  5)))

(def sample-txn-g3
  ;; 1, 2, 3, 4, 5 (smallest-numbered available vertex first)
  (-> (g/op-digraph)
      (sample-txn-link 1  3)
      (sample-txn-link 1  4)
      (sample-txn-link 1  5)
      (sample-txn-link 2  3)
      (sample-txn-link 2  4)
      (sample-txn-link 2  5)
      (sample-txn-link 3  4)
      (sample-txn-link 3  5)))

(comment
  ; op-state
  {:processes {:process :index}
   :kvs       {:k [:v :writer-index]}
   :anomalies :seq})

(def op-state-1
  {:processes {0 11}
   :kvs       {0 [0 1]}
   :anomalies nil})

(def op-state-2
  {:processes {1 13}
   :kvs       {0 [1 3]}
   :anomalies nil})

(def op-state-3
  {:processes {0 15}
   :kvs       {0 [2 5]}
   :anomalies '({:type :anomaly-3})})

(def op-state-4
  {:processes {0 9}
   :kvs       {0 [1 3]}
   :anomalies '({:type :anomaly-4})})
