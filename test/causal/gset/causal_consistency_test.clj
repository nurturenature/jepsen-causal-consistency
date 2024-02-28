(ns causal.gset.causal-consistency-test
  (:require [clojure.test :refer [deftest is testing]]
            [causal.gset
             [causal-consistency :as cc]
             [workload :as workload]]
            [jepsen.history :as h]))

(def valid-causal
  (->> [{:process 0, :type :invoke, :f :txn, :value [[:w :x 0]], :index 0, :time -1}
        {:process 0, :type :ok, :f :txn, :value [[:w :x 0]], :index 1, :time -1}
        {:process 1, :type :invoke, :f :txn, :value [[:w :y 0]], :index 2, :time -1}
        {:process 1, :type :ok, :f :txn, :value [[:w :y 0]], :index 3, :time -1}
        {:process 2, :type :invoke, :f :txn, :value [[:r :x nil] [:r :y nil]], :index 4, :time -1}
        {:process 2, :type :ok, :f :txn, :value [[:r :x #{0}] [:r :y nil]], :index 5, :time -1}
        {:process 2, :type :invoke, :f :txn, :value [[:r :x nil] [:r :y nil]], :index 6, :time -1}
        {:process 2, :type :ok, :f :txn, :value [[:r :x #{0}] [:r :y #{0}]], :index 7, :time -1}]
       h/history))

(def invalid-wr
  (->> [{:process 0, :type :invoke, :f :txn, :value [[:r :x nil]], :index 0, :time -1}
        {:process 0, :type :ok, :f :txn, :value [[:r :x #{0}]], :index 1, :time -1}
        {:process 0, :type :invoke, :f :txn, :value [[:w :y 0]], :index 2, :time -1}
        {:process 0, :type :ok, :f :txn, :value [[:w :y 0]], :index 3, :time -1}
        {:process 2, :type :invoke, :f :txn, :value [[:r :y nil]], :index 4, :time -1}
        {:process 2, :type :ok, :f :txn, :value [[:r :y #{0}]], :index 5, :time -1}
        {:process 2, :type :invoke, :f :txn, :value [[:w :x 0]], :index 6, :time -1}
        {:process 2, :type :ok, :f :txn, :value [[:w :x 0]], :index 7, :time -1}]
       h/history))

(deftest causal-consistency
  (testing "causal-consistency"
    (is (= {:valid? true}
           (cc/check workload/causal-opts valid-causal)))
    (is (= {:valid? false :anomaly-types [:G1c-process]}
           (-> (cc/check workload/causal-opts invalid-wr)
               (select-keys [:valid? :anomaly-types]))))))


