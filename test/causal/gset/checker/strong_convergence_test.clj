(ns causal.gset.checker.strong-convergence-test
  (:require [clojure.test :refer [deftest is testing]]
            [causal.gset.checker.strong-convergence :as sc]
            [jepsen
             [checker :as checker]
             [history :as h]]))

(def valid-final-reads
  (->> [{:process 0 :type :ok :f :txn :value [[:w 0 0]] :node "n1"}
        {:process 1 :type :ok :f :txn :value [[:w 1 1]] :node "n2"}
        {:process 0 :type :ok :f :txn :value [[:r 0 #{0}]] :final-read? true :node "n1"}
        {:process 0 :type :ok :f :txn :value [[:r 1 #{1}]] :final-read? true :node "n1"}
        {:process 1 :type :ok :f :txn :value [[:r 0 #{0}]] :final-read? true :node "n2"}
        {:process 1 :type :ok :f :txn :value [[:r 1 #{1}]] :final-read? true :node "n2"}]
       h/history))

(def invalid-final-reads
  (->> [{:process 0 :type :ok :f :txn :value [[:w 0 0]] :node "n1"}
        {:process 0 :type :ok :f :txn :value [[:w 1 1]] :node "n1"}
        {:process 0 :type :ok :f :txn :value [[:r 0 #{0}]] :final-read? true :node "n1"}
        {:process 0 :type :ok :f :txn :value [[:r 1 #{1}]] :final-read? true :node "n1"}
        {:process 1 :type :ok :f :txn :value [[:r 0 #{0}]] :final-read? true :node "n2"}
        {:process 1 :type :ok :f :txn :value [[:r 1 #{0}]] :final-read? true :node "n2"}]
       h/history))

(def unexpected-final-reads
  (->> [{:process 0 :type :ok :f :txn :value [[:w 0 0]] :node "n1"}
        {:process 0 :type :ok :f :txn :value [[:w 1 1]] :node "n1"}
        {:process 0 :type :ok :f :txn :value [[:r 0 #{0}]] :final-read? true :node "n1"}
        {:process 0 :type :ok :f :txn :value [[:r 1 #{1}]] :final-read? true :node "n1"}
        {:process 1 :type :ok :f :txn :value [[:r 0 #{0}]] :final-read? true :node "n2"}
        {:process 1 :type :ok :f :txn :value [[:r 1 #{1}]] :final-read? true :node "n2"}
        {:process 1 :type :ok :f :txn :value [[:r 1 #{2}]] :final-read? true :node "n2"}]
       h/history))

(deftest strong-convergence
  (testing "strong-convergence"
    (is (= {:valid? true
            :expected-read-count 2}
           (checker/check (sc/final-reads) {:nodes ["n1" "n2"]} valid-final-reads {})))
    (is (= {:valid?
            false,
            :expected-read-count 2
            :incomplete-final-reads {"n2" {:missing-count 1, :missing {1 {1 "n1"}}}},
            :unexpected-final-reads {"n2" {:unexpected-count 1, :unexpected {1 #{0}}}}}
           (checker/check (sc/final-reads) {:nodes ["n1" "n2"]} invalid-final-reads {})))
    (is (= {:valid? false,
            :expected-read-count 2
            :unexpected-final-reads {"n2" {:unexpected-count 1, :unexpected {1 #{2}}}}}
           (checker/check (sc/final-reads) {:nodes ["n1" "n2"]} unexpected-final-reads {})))))


