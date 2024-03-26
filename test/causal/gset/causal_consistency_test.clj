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

(def valid-not-G-single
  (->> [{:process 0, :type :ok, :f :txn, :value [[:w :x 0]], :index 1}
        {:process 0, :type :ok, :f :txn, :value [[:r :y nil]], :index 3}
        {:process 1, :type :ok, :f :txn, :value [[:r :x nil] [:w :y 1]], :index 5}]
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

(def invalid-wfr-all-mops (->> [{:process 0, :type :ok, :f :txn, :value [[:w :x 0] [:w :y 0]], :index 1}
                                {:process 1, :type :ok, :f :txn, :value [[:r :y #{0}] [:w :x 1]], :index 3}
                                {:process 2, :type :ok, :f :txn, :value [[:r :x #{0 1}] [:r :y nil]], :index 5}]
                               h/history))

(def invalid-monotonic-reads (->> [{:process 0, :type :ok, :f :txn, :value [[:w :x 0]], :index 1}
                                   {:process 1, :type :ok, :f :txn, :value [[:w :x 1]], :index 3}
                                   {:process 2, :type :ok, :f :txn, :value [[:r :x #{0}]], :index 5}
                                   {:process 2, :type :ok, :f :txn, :value [[:r :x #{0 1}]], :index 7}
                                   {:process 2, :type :ok, :f :txn, :value [[:r :x #{1}]], :index 9}]
                                  h/history))

(def valid-internal (->> [{:process 0, :type :ok, :f :txn, :value [[:w :x 0]], :index 1}
                          {:process 1, :type :ok, :f :txn, :value [[:r :x #{0}] [:w :x 1] [:r :x #{0 1}]], :index 3}
                          {:process 2, :type :ok, :f :txn, :value [[:w :x 2] [:r :x #{0 2}]], :index 5}]
                         h/history))

(def invalid-internal (->> [{:process 0, :type :ok, :f :txn, :value [[:w :x 0]], :index 1}
                            {:process 1, :type :ok, :f :txn, :value [[:r :x #{0}] [:w :x 1] [:r :x #{0}]], :index 3}
                            {:process 2, :type :ok, :f :txn, :value [[:w :x 2] [:r :x #{0 1}]], :index 5}]
                           h/history))

;; On Verifying Causal Consistency (POPL'17), Bouajjani

(def example-a-CM-but-not-CCv (->> [{:process 0, :type :ok, :f :txn, :value [[:w :x 1]], :index 1}
                                    {:process 0, :type :ok, :f :txn, :value [[:r :x #{1 2}]], :index 3}
                                    {:process 1, :type :ok, :f :txn, :value [[:w :x 2]], :index 5}
                                    {:process 1, :type :ok, :f :txn, :value [[:r :x #{1}]], :index 7}]
                                   h/history))

(def example-b-CCv-but-not-CM (->> [{:process 0, :type :ok, :f :txn, :value [[:w :z 1]], :index 1}
                                    {:process 0, :type :ok, :f :txn, :value [[:w :x 1]], :index 3}
                                    {:process 0, :type :ok, :f :txn, :value [[:w :y 1]], :index 5}
                                    {:process 1, :type :ok, :f :txn, :value [[:w :x 2]], :index 7}
                                    {:process 1, :type :ok, :f :txn, :value [[:r :z nil]], :index 9}
                                    {:process 1, :type :ok, :f :txn, :value [[:r :y #{1}]], :index 11}
                                    {:process 1, :type :ok, :f :txn, :value [[:r :x #{2}]], :index 13}]
                                   h/history))

(def example-c-CC-but-not-CM-nor-CCv (->> [{:process 0, :type :ok, :f :txn, :value [[:w :x 1]], :index 1}
                                           {:process 1, :type :ok, :f :txn, :value [[:w :x 2]], :index 3}
                                           {:process 1, :type :ok, :f :txn, :value [[:r :x #{1}]], :index 5}
                                           {:process 1, :type :ok, :f :txn, :value [[:r :x #{1 2}]], :index 7}]
                                          h/history))

(def example-d-CC-CM-and-CCv-but-not-sequentially-consistent
  (->> [{:process 0, :type :ok, :f :txn, :value [[:w :x 1]], :index 1}
        {:process 0, :type :ok, :f :txn, :value [[:r :y nil]], :index 3}
        {:process 0, :type :ok, :f :txn, :value [[:w :y 1]], :index 5}
        {:process 0, :type :ok, :f :txn, :value [[:r :x #{1}]], :index 7}
        {:process 1, :type :ok, :f :txn, :value [[:w :x 2]], :index 9}
        {:process 1, :type :ok, :f :txn, :value [[:r :y nil]], :index 11}
        {:process 1, :type :ok, :f :txn, :value [[:w :y 2]], :index 13}
        {:process 1, :type :ok, :f :txn, :value [[:r :x #{2}]], :index 15}]
       h/history))

(def example-e-not-CC-nor-CM-nor-CCv-interpretation-1
  (->> [{:process 0, :type :ok, :f :txn, :value [[:w :x 1]], :index 1}
        {:process 0, :type :ok, :f :txn, :value [[:w :y 1]], :index 3}
        {:process 1, :type :ok, :f :txn, :value [[:r :y #{1}]], :index 5}
        {:process 1, :type :ok, :f :txn, :value [[:w :x 2]], :index 7}
        {:process 2, :type :ok, :f :txn, :value [[:r :x #{2}]], :index 9}
        {:process 2, :type :ok, :f :txn, :value [[:r :x #{1}]], :index 11}]
       h/history))

(def example-e-not-CC-nor-CM-nor-CCv-interpretation-2
  (->> [{:process 0, :type :ok, :f :txn, :value [[:w :x 1]], :index 1}
        {:process 0, :type :ok, :f :txn, :value [[:w :y 1]], :index 3}
        {:process 1, :type :ok, :f :txn, :value [[:r :y #{1}]], :index 5}
        {:process 1, :type :ok, :f :txn, :value [[:w :x 2]], :index 7}
        {:process 2, :type :ok, :f :txn, :value [[:r :x #{1 2}]], :index 9}
        {:process 2, :type :ok, :f :txn, :value [[:r :x #{1}]], :index 11}]
       h/history))

(def output-dir
  "Base output directory for anomalies, graphs."
  "target/causal")

(def results-of-interest
  "Select these keys from the result map to determine pass/fail."
  [:valid? :anomaly-types :not])

(deftest causal-consistency
  (testing "causal-consistency"
    (let [output-dir (str output-dir "/causal")
          opts       (assoc workload/causal-opts :directory output-dir)]
      (is (= {:valid? true}
             (cc/check opts valid-causal)))
      (is (= {:valid? true}
             (cc/check opts valid-not-G-single))))))

(deftest wr
  (testing "w->r"
    (let [output-dir (str output-dir "/wr")
          opts       (assoc workload/causal-opts :directory output-dir)]
      (is (= {:valid? true}
             (cc/check opts valid-wr)))
      (is (= {:valid? false
              :anomaly-types [:G1c-process]
              :not #{:strong-session-read-committed}}
             (-> (cc/check opts invalid-wr-single-process)
                 (select-keys results-of-interest))))
      (is (= {:valid? false
              :anomaly-types [:G0]
              :not #{:read-uncommitted}}
             (-> (cc/check opts invalid-wr)
                 (select-keys results-of-interest)))))))

(deftest read-your-writes
  (testing "read-your-writes"
    (let [output-dir (str output-dir "/read-your-writes")
          opts       (assoc workload/causal-opts :directory output-dir)]
      (is (= {:valid? true}
             (cc/check opts valid-ryw)))
      (is (= {:valid? false
              :anomaly-types [:G-single-item-process]
              :not #{:strong-session-consistent-view}}
             (-> (cc/check opts invalid-ryw)
                 (select-keys results-of-interest))))
      (is (= {:valid? false
              :anomaly-types [:G-single-item-process]
              :not #{:strong-session-consistent-view}}
             (-> (cc/check opts invalid-ryw-not-nil)
                 (select-keys results-of-interest)))))))

(deftest monotonic-writes
  (testing "monotonic-writes"
    (let [output-dir (str output-dir "/monotonic-writes")
          opts       (assoc workload/causal-opts :directory output-dir)]
      (is (= {:valid? true}
             (cc/check opts valid-monotonic-writes)))
      (is (= {:valid? false
              :anomaly-types [:G-single-item-process]
              :not #{:strong-session-consistent-view}}
             (-> (cc/check opts invalid-monotonic-writes)
                 (select-keys results-of-interest)))))))

(deftest writes-follow-reads
  (testing "writes-follow-reads"
    (let [output-dir (str output-dir "/writes-follow-reads")
          opts       (assoc workload/causal-opts :directory output-dir)]
      (is (= {:valid? true}
             (cc/check opts valid-wfr)))
      (is (= {:valid? false
              :anomaly-types [:G-single-item-process]
              :not #{:strong-session-consistent-view}}
             (-> (cc/check opts invalid-wfr)
                 (select-keys results-of-interest))))
      (is (= {:valid? false
              :anomaly-types [:G-single-item-process]
              :not #{:strong-session-consistent-view}}
             (-> (cc/check opts invalid-wfr-mop)
                 (select-keys results-of-interest))))
      (is (= {:valid? false
              :anomaly-types [:G-single-item]
              :not #{:consistent-view :repeatable-read}}
             (-> (cc/check opts invalid-wfr-mops)
                 (select-keys results-of-interest))))
      (is (= {:valid? false
              :anomaly-types [:G-single-item]
              :not #{:consistent-view :repeatable-read}}
             (-> (cc/check opts invalid-wfr-all-mops)
                 (select-keys results-of-interest)))))))

(deftest monotonic-reads
  (testing "monotonic-reads"
    (let [output-dir (str output-dir "/monotonic-reads")
          opts       (assoc workload/causal-opts :directory output-dir)]
      (is (= {:valid? false
              :anomaly-types [:G-single-item-process]
              :not #{:repeatable-read :snapshot-isolation :strong-session-consistent-view}}
             (-> (cc/check opts invalid-monotonic-reads)
                 (select-keys results-of-interest)))))))

(deftest internal
  (testing "internal"
    (let [output-dir (str output-dir "/internal")
          opts       (assoc workload/causal-opts :directory output-dir)]
      (is (= {:valid? true}
             (-> (cc/check opts valid-internal)
                 (select-keys results-of-interest))))
      (is (= {:valid? false
              :anomaly-types [:internal]
              :not #{:read-atomic}}
             (-> (cc/check opts invalid-internal)
                 (select-keys results-of-interest)))))))

(deftest Bouajjani
  (testing "Bouajjani"
    (let [output-dir (str output-dir "/Bouajjani")
          opts       (assoc workload/causal-opts :directory output-dir)]
      (is (= {:valid? false :anomaly-types [:G-single-item-process]}
             (-> (cc/check opts example-a-CM-but-not-CCv)
                 (select-keys [:valid? :anomaly-types]))))
      (is (= {:valid? false :anomaly-types [:G-single-item-process]}
             (-> (cc/check opts example-b-CCv-but-not-CM)
                 (select-keys [:valid? :anomaly-types]))))
      (is (= {:valid? false :anomaly-types [:G-single-item-process]}
             (-> (cc/check opts example-c-CC-but-not-CM-nor-CCv)
                 (select-keys [:valid? :anomaly-types]))))

      ;; note, we are making the argument that "sequentially consistent"
      ;; isn't part of the common definition of Causal Consistency
      (is (= {:valid? true}
             (-> (cc/check opts example-d-CC-CM-and-CCv-but-not-sequentially-consistent)
                 (select-keys [:valid? :anomaly-types]))))

      (is (= {:valid? false :anomaly-types [:G-single-item-process]}
             (-> (cc/check opts example-e-not-CC-nor-CM-nor-CCv-interpretation-1)
                 (select-keys [:valid? :anomaly-types]))))
      (is (= {:valid? false :anomaly-types [:G-single-item-process]}
             (-> (cc/check opts example-e-not-CC-nor-CM-nor-CCv-interpretation-2)
                 (select-keys [:valid? :anomaly-types])))))))
