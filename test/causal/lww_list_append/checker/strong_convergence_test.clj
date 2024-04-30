(ns causal.lww-list-append.checker.strong-convergence-test
  (:require [clojure.test :refer [deftest is testing]]
            [causal.lww-list-append.checker.strong-convergence :as sc]
            [jepsen
             [checker :as checker]
             [history :as h]]))

(def valid-final-reads
  (->> [{:process 0 :type :ok :f :txn :value [[:append 0 0]] :node "n1"}
        {:process 1 :type :ok :f :txn :value [[:append 1 1]] :node "n2"}
        {:process 0 :type :ok :f :txn :value [[:r 0 [0]]] :final-read? true :node "n1"}
        {:process 0 :type :ok :f :txn :value [[:r 1 [1]]] :final-read? true :node "n1"}
        {:process 1 :type :ok :f :txn :value [[:r 0 [0]]] :final-read? true :node "n2"}
        {:process 1 :type :ok :f :txn :value [[:r 1 [1]]] :final-read? true :node "n2"}]
       h/history))

(def missing-nodes
  (->> [{:process 0 :type :ok :f :txn :value [[:append 0 0]] :node "n1"}
        {:process 1 :type :ok :f :txn :value [[:append 1 1]] :node "n2"}
        {:process 0 :type :ok :f :txn :value [[:r 0 [0]]] :final-read? true :node "n1"}
        {:process 0 :type :ok :f :txn :value [[:r 1 [1]]] :final-read? true :node "n1"}]
       h/history))

(def divergent-final-reads
  (->> [{:process 0 :type :ok :f :txn :value [[:append 0 0]] :node "n1"}
        {:process 0 :type :ok :f :txn :value [[:append 1 1]] :node "n1"}
        {:process 0 :type :ok :f :txn :value [[:r 0 [0]]] :final-read? true :node "n1"}
        {:process 0 :type :ok :f :txn :value [[:r 1 [1]]] :final-read? true :node "n1"}
        {:process 1 :type :ok :f :txn :value [[:r 0 [0]]] :final-read? true :node "n2"}
        {:process 1 :type :ok :f :txn :value [[:r 1 nil]] :final-read? true :node "n2"}]
       h/history))

(def invalid-final-reads
  (->> [{:process 0 :type :ok :f :txn :value [[:append 0 0]] :node "n1"}
        {:process 0 :type :ok :f :txn :value [[:append 1 1]] :node "n1"}
        {:process 0 :type :ok :f :txn :value [[:r 0 [0]]] :final-read? true :node "n1"}
        {:process 0 :type :ok :f :txn :value [[:r 1 [1]]] :final-read? true :node "n1"}
        {:process 1 :type :ok :f :txn :value [[:r 0 [0]]] :final-read? true :node "n2"}
        {:process 1 :type :ok :f :txn :value [[:r 1 [1]]] :final-read? true :node "n2"}
        {:process 1 :type :ok :f :txn :value [[:r 1 [2]]] :final-read? true :node "n2"}]
       h/history))

(deftest strong-convergence
  (testing "strong-convergence"
    (is (= {:valid? true}
           (checker/check (sc/final-reads) {:nodes ["n1" "n2"]}
                          valid-final-reads {})))
    (is (= {:valid? false
            :missing-nodes {0 #{"n2"} 1 #{"n2"}}}
           (checker/check (sc/final-reads) {:nodes ["n1" "n2"]}
                          missing-nodes {})))
    (is (= {:valid? false
            :divergent-reads {1 {[1] #{"n1"}, nil #{"n2"}}}}
           (checker/check (sc/final-reads) {:nodes ["n1" "n2"]}
                          divergent-final-reads {})))
    (is (= {:valid? false
            :divergent-reads {1 {[1] #{"n2" "n1"}, [2] #{"n2"}}}, :invalid-reads {1 {2 #{"n2"}}}}
           (checker/check (sc/final-reads) {:nodes ["n1" "n2"]}
                          invalid-final-reads {})))))


