(ns causal.checker.strong-convergence-test
  (:require [clojure.test :refer :all]
            [causal.checker.strong-convergence :as sc]
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
    (is (= {:valid? true}
           (checker/check (sc/final-reads) {:nodes ["n1" "n2"]} valid-final-reads {})))
    (is (= {:valid? false, :incomplete-final-reads {"n2" #{[1 1]}}, :unexpected-final-reads {"n2" #{[1 0]}}}
           (checker/check (sc/final-reads) {:nodes ["n1" "n2"]} invalid-final-reads {})))
    (is (= {:valid? false, :unexpected-final-reads {"n2" #{[1 2]}}}
           (checker/check (sc/final-reads) {:nodes ["n1" "n2"]} unexpected-final-reads {})))))


