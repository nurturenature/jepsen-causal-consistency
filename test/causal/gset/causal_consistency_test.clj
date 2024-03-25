(ns causal.gset.causal-consistency-test
  (:require [clojure.test :refer [deftest is testing]]
            [causal.gset
             [causal-consistency :as cc]
             [workload :as workload]]
            [jepsen.history :as h]))

(def valid-causal
  (->> [{:process 0, :type :ok, :f :txn, :value [[:w :x 0]], :index 1, :time -1}
        {:process 1, :type :ok, :f :txn, :value [[:w :y 0]], :index 3, :time -1}
        {:process 2, :type :ok, :f :txn, :value [[:r :x #{0}] [:r :y nil]], :index 5, :time -1}
        {:process 2, :type :ok, :f :txn, :value [[:r :x #{0}] [:r :y #{0}]], :index 7, :time -1}]
       h/history))

(def valid-wr
  (->> [{:process 0, :type :ok, :f :txn, :value [[:r :x #{0}]], :index 1, :time -1}
        {:process 0, :type :ok, :f :txn, :value [[:w :y 0]], :index 3, :time -1}
        {:process 2, :type :ok, :f :txn, :value [[:w :x 0]], :index 5, :time -1}
        {:process 2, :type :ok, :f :txn, :value [[:r :y #{0}]], :index 7, :time -1}]
       h/history))

(def invalid-wr-single-process
  (->> [{:process 0, :type :ok, :f :txn, :value [[:r :x #{0}]], :index 1, :time -1}
        {:process 0, :type :ok, :f :txn, :value [[:w :x 0]], :index 3, :time -1}]
       h/history))

(def invalid-wr
  (->> [{:process 0, :type :ok, :f :txn, :value [[:r :x #{0}]], :index 1, :time -1}
        {:process 0, :type :ok, :f :txn, :value [[:w :y 0]], :index 3, :time -1}
        {:process 2, :type :ok, :f :txn, :value [[:r :y #{0}]], :index 5, :time -1}
        {:process 2, :type :ok, :f :txn, :value [[:w :x 0]], :index 7, :time -1}]
       h/history))

(def valid-ryw (->> [{:process 0, :type :ok, :f :txn, :value [[:r :x nil]], :index 1, :time -1}
                     {:process 0, :type :ok, :f :txn, :value [[:w :x 0]],   :index 3, :time -1}
                     {:process 0, :type :ok, :f :txn, :value [[:r :x #{0}]], :index 5, :time -1}]
                    h/history))

(def invalid-ryw (->> [{:process 0, :type :ok, :f :txn, :value [[:w :x 0]],   :index 1, :time -1}
                       {:process 0, :type :ok, :f :txn, :value [[:r :x nil]], :index 3, :time -1}]
                      h/history))

(def invalid-ryw-not-nil (->> [{:process 0, :type :ok, :f :txn, :value [[:w :x 0]],   :index 1, :time -1}
                               {:process 0, :type :ok, :f :txn, :value [[:w :x 1]],   :index 3, :time -1}
                               {:process 0, :type :ok, :f :txn, :value [[:r :x #{0}]], :index 5, :time -1}]
                              h/history))

(def valid-monotonic-writes (->> [{:process 0, :type :ok, :f :txn, :value [[:w :x 0]], :index 1}
                                  {:process 0, :type :ok, :f :txn, :value [[:w :x 1]], :index 3}
                                  {:process 1, :type :ok, :f :txn, :value [[:r :x #{0}]], :index 5}
                                  {:process 1, :type :ok, :f :txn, :value [[:r :x #{0 1}]], :index 7}]
                                 h/history))

(def invalid-monotonic-writes (->> [{:process 0, :type :ok, :f :txn, :value [[:w :x 0]], :index 1}
                                    {:process 0, :type :ok, :f :txn, :value [[:w :x 1]], :index 3}
                                    {:process 1, :type :ok, :f :txn, :value [[:r :x #{1}]], :index 5}
                                    {:process 1, :type :ok, :f :txn, :value [[:r :x #{0 1}]], :index 7}]
                                   h/history))

(def valid-wfr (->> [{:process 0, :type :ok, :f :txn, :value [[:w :y 1]], :index 1}
                     {:process 0, :type :ok, :f :txn, :value [[:r :y #{1}]], :index 3}
                     {:process 0, :type :ok, :f :txn, :value [[:w :x 0]], :index 5}
                     {:process 1, :type :ok, :f :txn, :value [[:r :x #{0}]], :index 7}
                     {:process 1, :type :ok, :f :txn, :value [[:w :x 1]], :index 9}
                     {:process 1, :type :ok, :f :txn, :value [[:r :y #{1}]], :index 11}]
                    h/history))

(def invalid-wfr (->> [{:process 0, :type :ok, :f :txn, :value [[:w :x 0]], :index 1}
                       {:process 1, :type :ok, :f :txn, :value [[:r :x #{0}]], :index 3}
                       {:process 1, :type :ok, :f :txn, :value [[:w :y 0]], :index 5}
                       {:process 2, :type :ok, :f :txn, :value [[:r :y #{0}]], :index 7}
                       {:process 2, :type :ok, :f :txn, :value [[:r :x nil]], :index 9}]
                      h/history))

(def invalid-wfr-mop (->> [{:process 0, :type :ok, :f :txn, :value [[:w :x 0]], :index 1}
                           {:process 1, :type :ok, :f :txn, :value [[:r :x #{0}]], :index 3}
                           {:process 1, :type :ok, :f :txn, :value [[:w :y 1]], :index 5}
                           {:process 2, :type :ok, :f :txn, :value [[:r :y #{1}]], :index 7}
                           {:process 2, :type :ok, :f :txn, :value [[:r :x nil]], :index 9}]
                          h/history))

(def invalid-wfr-mops (->> [{:process 0, :type :ok, :f :txn, :value [[:w :x 0]], :index 1}
                            {:process 1, :type :ok, :f :txn, :value [[:r :x #{0}]], :index 3}
                            {:process 1, :type :ok, :f :txn, :value [[:w :y 1]], :index 5}
                            {:process 2, :type :ok, :f :txn, :value [[:r :y #{1}] [:r :x nil]], :index 7}]
                           h/history))

(def invalid-monotonic-reads (->> [{:process 0, :type :ok, :f :txn, :value [[:w :x 0]], :index 1}
                                   {:process 1, :type :ok, :f :txn, :value [[:w :x 1]], :index 3}
                                   {:process 2, :type :ok, :f :txn, :value [[:r :x #{0}]], :index 5}
                                   {:process 2, :type :ok, :f :txn, :value [[:r :x #{0 1}]], :index 7}
                                   {:process 2, :type :ok, :f :txn, :value [[:r :x #{1}]], :index 9}]
                                  h/history))

(def output-dir
  "Base output directory for anomalies, graphs."
  "target/causal")

(deftest causal-consistency
  (testing "causal-consistency"
    (let [output-dir (str output-dir "/cc")
          opts       (assoc workload/causal-opts :directory output-dir)]
      (is (= {:valid? true}
             (cc/check opts valid-causal))))))

(deftest wr
  (testing "w->r"
    (let [output-dir (str output-dir "/wr")
          opts       (assoc workload/causal-opts :directory output-dir)]
      (is (= {:valid? true}
             (cc/check opts valid-wr)))
      (is (= {:valid? false :anomaly-types [:G1c-process]}
             (-> (cc/check opts invalid-wr-single-process)
                 (select-keys [:valid? :anomaly-types]))))
      (is (= {:valid? false :anomaly-types [:G0]}
             (-> (cc/check opts invalid-wr)
                 (select-keys [:valid? :anomaly-types])))))))

(deftest ryw
  (testing "ryw"
    (let [output-dir (str output-dir "/ryw")
          opts       (assoc workload/causal-opts :directory output-dir)]
      (is (= {:valid? true}
             (cc/check opts valid-ryw)))
      (is (= {:valid? false :anomaly-types [:G-single-item-process]}
             (-> (cc/check opts invalid-ryw)
                 (select-keys [:valid? :anomaly-types]))))
      (is (= {:valid? false :anomaly-types [:G-single-item-process]}
             (-> (cc/check opts invalid-ryw-not-nil)
                 (select-keys [:valid? :anomaly-types])))))))

(deftest monotonic-writes
  (testing "monotonic-writes"
    (let [output-dir (str output-dir "/monotonic-writes")
          opts       (assoc workload/causal-opts :directory output-dir)]
      (is (= {:valid? true}
             (cc/check opts valid-monotonic-writes)))
      (is (= {:valid? false :anomaly-types [:G-single-item-process]}
             (-> (cc/check opts invalid-monotonic-writes)
                 (select-keys [:valid? :anomaly-types])))))))

(deftest wfr
  (testing "writes-follow-reads"
    (let [output-dir (str output-dir "/writes-follow-reads")
          opts       (assoc workload/causal-opts :directory output-dir)]
      (is (= {:valid? true}
             (cc/check opts valid-wfr)))
      (is (= {:valid? false :anomaly-types [:G0-single-item-process]}
             (-> (cc/check opts invalid-wfr)
                 (select-keys [:valid? :anomaly-types]))))
      (is (= {:valid? false :anomaly-types [:G-single-item-process]}
             (-> (cc/check opts invalid-wfr-mop)
                 (select-keys [:valid? :anomaly-types]))))
      (is (= {:valid? false :anomaly-types [:G-single-item]}
             (-> (cc/check opts invalid-wfr-mops)
                 (select-keys [:valid? :anomaly-types])))))))

(deftest monotonic-reads
  (testing "monotonic-reads"
    (let [output-dir (str output-dir "/monotonic-reads")
          opts       (assoc workload/causal-opts :directory output-dir)]
      (is (= {:valid? false :anomaly-types [:G-single-item-process]}
             (-> (cc/check opts invalid-monotonic-reads)
                 (select-keys [:valid? :anomaly-types])))))))
