(ns causal.checker.strong-convergence-test
  (:require [clojure.test :refer [deftest is testing]]
            [causal.checker
             [opts :as causal-opts]
             [strong-convergence :as sc]]
            [jepsen
             [checker :as checker]
             [history :as h]]))

(def valid-final-reads
  (->> [{:process 0, :type :invoke, :f :txn, :value [[:append 0 0]], :node "n1", :index 0, :time -1}
        {:process 0, :type :ok, :f :txn, :value [[:append 0 0]], :node "n1", :index 1, :time -1}
        {:process 1, :type :invoke, :f :txn, :value [[:append 1 1]], :node "n2", :index 2, :time -1}
        {:process 1, :type :ok, :f :txn, :value [[:append 1 1]], :node "n2", :index 3, :time -1}
        {:process 0, :type :invoke, :f :txn, :value [[:r 0 nil]], :final-read? true, :node "n1", :index 4, :time -1}
        {:process 0, :type :ok, :f :txn, :value [[:r 0 [0]]], :final-read? true, :node "n1", :index 5, :time -1}
        {:process 0, :type :invoke, :f :txn, :value [[:r 1 nil]], :final-read? true, :node "n1", :index 6, :time -1}
        {:process 0, :type :ok, :f :txn, :value [[:r 1 [1]]], :final-read? true, :node "n1", :index 7, :time -1}
        {:process 1, :type :invoke, :f :txn, :value [[:r 0 nil]], :final-read? true, :node "n2", :index 8, :time -1}
        {:process 1, :type :ok, :f :txn, :value [[:r 0 [0]]], :final-read? true, :node "n2", :index 9, :time -1}
        {:process 1, :type :invoke, :f :txn, :value [[:r 1 nil]], :final-read? true, :node "n2", :index 10, :time -1}
        {:process 1, :type :ok, :f :txn, :value [[:r 1 [1]]], :final-read? true, :node "n2", :index 11, :time -1}]
       h/history))

(def missing-nodes
  (->> [{:process 0, :type :invoke, :f :txn, :value [[:append 0 0]], :node "n1", :index 0, :time -1}
        {:process 0, :type :ok, :f :txn, :value [[:append 0 0]], :node "n1", :index 1, :time -1}
        {:process 1, :type :invoke, :f :txn, :value [[:append 1 1]], :node "n2", :index 2, :time -1}
        {:process 1, :type :ok, :f :txn, :value [[:append 1 1]], :node "n2", :index 3, :time -1}
        {:process 0, :type :invoke, :f :txn, :value [[:r 0 nil]], :final-read? true, :node "n1", :index 4, :time -1}
        {:process 0, :type :ok, :f :txn, :value [[:r 0 [0]]], :final-read? true, :node "n1", :index 5, :time -1}
        {:process 0, :type :invoke, :f :txn, :value [[:r 1 nil]], :final-read? true, :node "n1", :index 6, :time -1}
        {:process 0, :type :ok, :f :txn, :value [[:r 1 [1]]], :final-read? true, :node "n1", :index 7, :time -1}]
       h/history))

(def divergent-final-reads
  (->> [{:process 0, :type :invoke, :f :txn, :value [[:append 0 0]], :node "n1", :index 0, :time -1}
        {:process 0, :type :ok, :f :txn, :value [[:append 0 0]], :node "n1", :index 1, :time -1}
        {:process 0, :type :invoke, :f :txn, :value [[:append 1 1]], :node "n1", :index 2, :time -1}
        {:process 0, :type :ok, :f :txn, :value [[:append 1 1]], :node "n1", :index 3, :time -1}
        {:process 0, :type :invoke, :f :txn, :value [[:r 0 nil]], :final-read? true, :node "n1", :index 4, :time -1}
        {:process 0, :type :ok, :f :txn, :value [[:r 0 [0]]], :final-read? true, :node "n1", :index 5, :time -1}
        {:process 0, :type :invoke, :f :txn, :value [[:r 1 nil]], :final-read? true, :node "n1", :index 6, :time -1}
        {:process 0, :type :ok, :f :txn, :value [[:r 1 [1]]], :final-read? true, :node "n1", :index 7, :time -1}
        {:process 1, :type :invoke, :f :txn, :value [[:r 0 nil]], :final-read? true, :node "n2", :index 8, :time -1}
        {:process 1, :type :ok, :f :txn, :value [[:r 0 [0]]], :final-read? true, :node "n2", :index 9, :time -1}
        {:process 1, :type :invoke, :f :txn, :value [[:r 1 nil]], :final-read? true, :node "n2", :index 10, :time -1}
        {:process 1, :type :ok, :f :txn, :value [[:r 1 nil]], :final-read? true, :node "n2", :index 11, :time -1}]
       h/history))

(def invalid-final-reads
  (->> [{:process 0, :type :invoke, :f :txn, :value [[:append 0 0]], :node "n1", :index 0, :time -1}
        {:process 0, :type :ok, :f :txn, :value [[:append 0 0]], :node "n1", :index 1, :time -1}
        {:process 0, :type :invoke, :f :txn, :value [[:append 1 1]], :node "n1", :index 2, :time -1}
        {:process 0, :type :ok, :f :txn, :value [[:append 1 1]], :node "n1", :index 3, :time -1}
        {:process 0, :type :invoke, :f :txn, :value [[:r 0 nil]], :final-read? true, :node "n1", :index 4, :time -1}
        {:process 0, :type :ok, :f :txn, :value [[:r 0 [0]]], :final-read? true, :node "n1", :index 5, :time -1}
        {:process 0, :type :invoke, :f :txn, :value [[:r 1 nil]], :final-read? true, :node "n1", :index 6, :time -1}
        {:process 0, :type :ok, :f :txn, :value [[:r 1 [1]]], :final-read? true, :node "n1", :index 7, :time -1}
        {:process 1, :type :invoke, :f :txn, :value [[:r 0 nil]], :final-read? true, :node "n2", :index 8, :time -1}
        {:process 1, :type :ok, :f :txn, :value [[:r 0 [0]]], :final-read? true, :node "n2", :index 9, :time -1}
        {:process 1, :type :invoke, :f :txn, :value [[:r 1 nil]], :final-read? true, :node "n2", :index 10, :time -1}
        {:process 1, :type :ok, :f :txn, :value [[:r 1 [1]]], :final-read? true, :node "n2", :index 11, :time -1}
        {:process 1, :type :invoke, :f :txn, :value [[:r 1 nil]], :final-read? true, :node "n2", :index 12, :time -1}
        {:process 1, :type :ok, :f :txn, :value [[:r 1 [2]]], :final-read? true, :node "n2", :index 13, :time -1}]
       h/history))

(def output-dir
  "Base output directory for anomalies, graphs."
  "target/strong-convergence")

(deftest strong-convergence
  (testing "strong-convergence"
    (let [opts     (assoc causal-opts/causal-opts :directory output-dir)
          test-map {:name "strong-convergence" :start-time "" :nodes ["n1" "n2"]}]
      (is (= {:valid? true}
             (checker/check (sc/final-reads opts) test-map
                            valid-final-reads opts)))
      (is (= {:valid? false
              :missing-nodes {0 #{"n2"} 1 #{"n2"}}}
             (checker/check (sc/final-reads opts) test-map
                            missing-nodes opts)))
      (is (= {:valid? false
              :divergent-reads {1 {["n1"] [1], ["n2"] nil}}}
             (checker/check (sc/final-reads opts) test-map
                            divergent-final-reads opts)))
      (is (= {:valid? false
              :divergent-reads {1 {["n1" "n2"] [1], ["n2"] [2]}},
              :invalid-reads {1 {2 #{"n2"}}}}
             (checker/check (sc/final-reads opts) test-map
                            invalid-final-reads opts))))))
