(ns causal.lww-list-append.checker.lww-test
  (:require [clojure.test :refer [deftest is testing]]
            [causal.lww-list-append.checker.lww :as lww]
            [causal.util :as util]
            [jepsen.history :as h]))

(def invalid-realtime
  (->> [{:process 0, :type :invoke, :f :txn, :value [[:append :x 0]], :index 0, :time -1}
        {:process 0, :type :ok, :f :txn, :value [[:append :x 0]], :index 1, :time -1}

        {:process 1, :type :invoke, :f :txn, :value [[:append :x 1]], :index 2, :time -1}
        {:process 1, :type :ok, :f :txn, :value [[:append :x 1]], :index 3, :time -1}

        {:process 2, :type :invoke, :f :txn, :value [[:r :x nil]], :index 4, :time -1}
        {:process 2, :type :ok, :f :txn, :value [[:r :x [1]]], :index 5, :time -1}

        {:process 2, :type :invoke, :f :txn, :value [[:r :x nil]], :index 6, :time -1}
        {:process 2, :type :ok, :f :txn, :value [[:r :x [0]]], :index 7, :time -1}]
       h/history))

(def output-dir
  "Base output directory for anomalies, graphs."
  "target/lww")

(def results-of-interest
  "Select these keys from the result map to determine pass/fail."
  [:valid? :anomaly-types :not])

(deftest lww
  (testing "lww"
    (let [output-dir (str output-dir "/lww")
          opts       (assoc util/causal-opts :directory output-dir)]
      (is (= {:valid? false
              :anomaly-types [:G0-realtime :strong-PL-1-cycle-exists]}
             (-> (lww/check opts invalid-realtime)
                 (select-keys results-of-interest)))))))
