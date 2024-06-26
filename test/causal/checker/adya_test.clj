(ns causal.checker.adya-test
  (:require [clojure.set :as set]
            [clojure.test :refer [deftest is testing]]
            [causal.checker
             [adya :as adya]
             [opts :as causal-opts]]
            [jepsen
             [history :as h]
             [store :as store]]
            [jepsen.history.sim :as h-sim]))

(def valid-cc
  (->> [{:process 0, :type :ok, :f :txn, :value [[:append :x 0]], :index 1, :time -1}
        {:process 1, :type :ok, :f :txn, :value [[:append :x 1]], :index 3, :time -1}
        {:process 0, :type :ok, :f :txn, :value [[:r :x [0 1]]], :index 5, :time -1}]
       h/history))

(def valid-cc-rw
  (->> [{:process 0, :type :ok, :f :txn, :value [[:append :x 0]], :index 1, :time -1}
        {:process 1, :type :ok, :f :txn, :value [[:append :y 0]], :index 3, :time -1}
        {:process 2, :type :ok, :f :txn, :value [[:r :x [0]] [:r :y nil]], :index 5, :time -1}
        {:process 3, :type :ok, :f :txn, :value [[:r :x nil] [:r :y [0]]], :index 7, :time -1}]
       h/history))

(def invalid-cc
  (->> [{:process 0, :type :ok, :f :txn, :value [[:append :x 0]], :index 1, :time -1}
        {:process 1, :type :ok, :f :txn, :value [[:r :x [0]] [:append :y 0]], :index 3, :time -1}
        {:process 2, :type :ok, :f :txn, :value [[:r :y [0]] [:r :x nil]], :index 5, :time -1}]
       h/history))

(def invalid-wr
  (->> [{:process 0, :type :ok, :f :txn, :value [[:r :x [0]]], :index 1, :time -1}
        {:process 0, :type :ok, :f :txn, :value [[:append :y 0]], :index 3, :time -1}
        {:process 2, :type :ok, :f :txn, :value [[:r :y [0]]], :index 5, :time -1}
        {:process 2, :type :ok, :f :txn, :value [[:append :x 0]], :index 7, :time -1}]
       h/history))

(def invalid-wr-single-process
  (->> [{:process 0, :type :ok, :f :txn, :value [[:r :x [0]]], :index 1, :time -1}
        {:process 0, :type :ok, :f :txn, :value [[:append :x 0]], :index 3, :time -1}]
       h/history))

(def invalid-wr-realtime
  (->> [{:process 0, :type :ok, :f :txn, :value [[:append :x 0]], :index 1, :time -1}
        {:process 0, :type :ok, :f :txn, :value [[:r :y [0]]], :index 3, :time -1}
        {:process 2, :type :ok, :f :txn, :value [[:r :x [0]]], :index 5, :time -1}
        {:process 2, :type :ok, :f :txn, :value [[:append :y 0]], :index 7, :time -1}]
       h/history))

(def valid-internal (->> [{:process 0, :type :ok, :f :txn, :value [[:append :x 0]], :index 1}
                          {:process 1, :type :ok, :f :txn, :value [[:r :x [0]] [:append :x 1] [:r :x [0 1]]], :index 3}
                          {:process 2, :type :ok, :f :txn, :value [[:append :x 2] [:r :x [0 2]]], :index 5}]
                         h/history))

(def invalid-internal-ryw (->> [{:process 0, :type :ok, :f :txn, :value [[:append :x 0]], :index 1}
                                {:process 1, :type :ok, :f :txn, :value [[:r :x [0]] [:append :x 1] [:r :x [0]]], :index 3}
                                {:process 2, :type :ok, :f :txn, :value [[:append :x 2] [:r :x [0 1]]], :index 5}]
                               h/history))

(def invalid-internal-mono-reads (->> [{:process 0, :type :ok, :f :txn, :value [[:append :x 0]], :index 1}
                                       {:process 1, :type :ok, :f :txn, :value [[:r :x [0]] [:append :x 1] [:r :x [1]]], :index 3}
                                       {:process 2, :type :ok, :f :txn, :value [[:r :x [0]] [:r :x [0 1]] [:r :x [0]]], :index 5}]
                                      h/history))

(def invalid-internal-future-read (->> [{:process 0, :type :ok, :f :txn, :value [[:r :x nil] [:append :x 0] [:r :x [0]]], :index 1}
                                        {:process 0, :type :ok, :f :txn, :value [[:r :y [0]] [:append :y 0]], :index 3}]
                                       h/history))

(def invalid-G1a (->> [{:process 0, :type :ok,   :f :txn, :value [[:append :x 0]], :index 1}
                       {:process 0, :type :fail, :f :txn, :value [[:append :x 1]], :index 3}
                       {:process 1, :type :ok,   :f :txn, :value [[:r :x [0 1]]], :index 5}]
                      h/history))

(def invalid-G1a-mops (->> [{:process 0, :type :ok,   :f :txn, :value [[:append :x 0] [:append :x 1]], :index 1}
                            {:process 0, :type :fail, :f :txn, :value [[:append :x 2] [:append :x 3]], :index 3}
                            {:process 1, :type :ok,   :f :txn, :value [[:r :x [0 1 2 3]]], :index 5}]
                           h/history))

(def invalid-G1b (->> [{:process 0, :type :ok, :f :txn, :value [[:append :x 0] [:append :x 1]], :index 1, :time -1}
                       {:process 1, :type :ok, :f :txn, :value [[:r :x [0]]],   :index 3, :time -1}]
                      h/history))

(def invalid-G1b-read-of-final (->> [{:process 0, :type :ok, :f :txn, :value [[:append :x 0] [:append :x 1]], :index 1, :time -1}
                                     {:process 1, :type :ok, :f :txn, :value [[:r :x [0]]],   :index 3, :time -1}
                                     {:process 1, :type :ok, :f :txn, :value [[:r :x [0 1]]],   :index 5, :time -1}
                                     {:process 2, :type :ok, :f :txn, :value [[:r :x [0 1]]],   :index 7, :time -1}]
                                    h/history))

(def valid-ryw (->> [{:process 0, :type :ok, :f :txn, :value [[:r :x nil]], :index 1, :time -1}
                     {:process 0, :type :ok, :f :txn, :value [[:append :x 0]],   :index 3, :time -1}
                     {:process 0, :type :ok, :f :txn, :value [[:r :x [0]]], :index 5, :time -1}]
                    h/history))

(def invalid-ryw (->> [{:process 0, :type :ok, :f :txn, :value [[:append :x 0]],   :index 1, :time -1}
                       {:process 0, :type :ok, :f :txn, :value [[:r :x nil]], :index 3, :time -1}]
                      h/history))

(def invalid-ryw-not-nil (->> [{:process 0, :type :ok, :f :txn, :value [[:append :x 0]],   :index 1, :time -1}
                               {:process 0, :type :ok, :f :txn, :value [[:append :x 1]],   :index 3, :time -1}
                               {:process 0, :type :ok, :f :txn, :value [[:r :x [0]]], :index 5, :time -1}]
                              h/history))

(def invalid-ryw-+2 (->> [{:process 0, :type :ok, :f :txn, :value [[:append :x 0]],   :index 1, :time -1}
                          {:process 0, :type :ok, :f :txn, :value [[:append :x 1]],   :index 3, :time -1}
                          {:process 0, :type :ok, :f :txn, :value [[:r :x [0]]], :index 5, :time -1}
                          {:process 0, :type :ok, :f :txn, :value [[:r :x [0 1]]], :index 7, :time -1}
                          {:process 0, :type :ok, :f :txn, :value [[:r :x [0]]], :index 9, :time -1}]
                         h/history))

(def valid-monotonic-writes (->> [{:process 0, :type :ok, :f :txn, :value [[:append :x 0]], :index 1}
                                  {:process 0, :type :ok, :f :txn, :value [[:append :x 1]], :index 3}
                                  {:process 1, :type :ok, :f :txn, :value [[:r :x [0]]], :index 5}
                                  {:process 1, :type :ok, :f :txn, :value [[:r :x [0 1]]], :index 7}]
                                 h/history))

(def invalid-monotonic-writes (->> [{:process 0, :type :ok, :f :txn, :value [[:append :x 0]], :index 1}
                                    {:process 0, :type :ok, :f :txn, :value [[:append :x 1]], :index 3}
                                    {:process 1, :type :ok, :f :txn, :value [[:r :x [1]]], :index 5}
                                    {:process 1, :type :ok, :f :txn, :value [[:r :x [0]]], :index 7}]
                                   h/history))

(def valid-wfr (->> [{:process 0, :type :ok, :f :txn, :value [[:append :y 0]], :index 1}
                     {:process 1, :type :ok, :f :txn, :value [[:r :y [0]]], :index 3}
                     {:process 1, :type :ok, :f :txn, :value [[:append :x 0]], :index 5}
                     {:process 2, :type :ok, :f :txn, :value [[:r :x nil]], :index 7}
                     {:process 2, :type :ok, :f :txn, :value [[:r :x [0]]], :index 9}
                     {:process 2, :type :ok, :f :txn, :value [[:r :y [0]]], :index 11}]
                    h/history))

(def invalid-wfr (->> [{:process 0, :type :ok, :f :txn, :value [[:append :x 0]], :index 1}
                       {:process 1, :type :ok, :f :txn, :value [[:r :x [0]]], :index 3}
                       {:process 1, :type :ok, :f :txn, :value [[:append :y 0]], :index 5}
                       {:process 2, :type :ok, :f :txn, :value [[:r :y [0]]], :index 7}
                       {:process 2, :type :ok, :f :txn, :value [[:r :x nil]], :index 9}]
                      h/history))

(def invalid-wfr-mop (->> [{:process 0, :type :ok, :f :txn, :value [[:append :x 0]], :index 1}
                           {:process 1, :type :ok, :f :txn, :value [[:r :x [0]]], :index 3}
                           {:process 1, :type :ok, :f :txn, :value [[:append :y 0]], :index 5}
                           {:process 2, :type :ok, :f :txn, :value [[:r :y [0]]], :index 7}
                           {:process 2, :type :ok, :f :txn, :value [[:r :x nil]], :index 9}]
                          h/history))

(def invalid-wfr-mops (->> [{:process 0, :type :ok, :f :txn, :value [[:append :x 0]], :index 1}
                            {:process 1, :type :ok, :f :txn, :value [[:r :x [0]]], :index 3}
                            {:process 1, :type :ok, :f :txn, :value [[:append :y 0]], :index 5}
                            {:process 2, :type :ok, :f :txn, :value [[:r :y [0]] [:r :x nil]], :index 7}]
                           h/history))

(def invalid-wfr-all-mops (->> [{:process 0, :type :ok, :f :txn, :value [[:append :x 0] [:append :y 0]], :index 1}
                                {:process 1, :type :ok, :f :txn, :value [[:r :y [0]] [:append :x 1]], :index 3}
                                {:process 2, :type :ok, :f :txn, :value [[:r :x [0 1]] [:r :y nil]], :index 5}]
                               h/history))

(def invalid-monotonic-reads (->> [{:process 0, :type :ok, :f :txn, :value [[:append :x 0]], :index 1}
                                   {:process 1, :type :ok, :f :txn, :value [[:append :x 1]], :index 3}
                                   {:process 2, :type :ok, :f :txn, :value [[:r :x [0]]], :index 5}
                                   {:process 2, :type :ok, :f :txn, :value [[:r :x [0 1]]], :index 7}
                                   {:process 2, :type :ok, :f :txn, :value [[:r :x [0]]], :index 9}]
                                  h/history))

;; On Verifying Causal Consistency (POPL'17), Bouajjani

(def example-a-CM-but-not-CCv (->> [{:process 0, :type :ok, :f :txn, :value [[:append :x 1]], :index 1}
                                    {:process 0, :type :ok, :f :txn, :value [[:r :x #{1 2}]], :index 3}
                                    {:process 1, :type :ok, :f :txn, :value [[:append :x 2]], :index 5}
                                    {:process 1, :type :ok, :f :txn, :value [[:r :x #{1}]], :index 7}]
                                   h/history))

(def example-b-CCv-but-not-CM (->> [{:process 0, :type :ok, :f :txn, :value [[:append :z 1]], :index 1}
                                    {:process 0, :type :ok, :f :txn, :value [[:append :x 1]], :index 3}
                                    {:process 0, :type :ok, :f :txn, :value [[:append :y 1]], :index 5}
                                    {:process 1, :type :ok, :f :txn, :value [[:append :x 2]], :index 7}
                                    {:process 1, :type :ok, :f :txn, :value [[:r :z nil]], :index 9}
                                    {:process 1, :type :ok, :f :txn, :value [[:r :y #{1}]], :index 11}
                                    {:process 1, :type :ok, :f :txn, :value [[:r :x #{2}]], :index 13}]
                                   h/history))

(def example-c-CC-but-not-CM-nor-CCv (->> [{:process 0, :type :ok, :f :txn, :value [[:append :x 1]], :index 1}
                                           {:process 1, :type :ok, :f :txn, :value [[:append :x 2]], :index 3}
                                           {:process 1, :type :ok, :f :txn, :value [[:r :x #{1}]], :index 5}
                                           {:process 1, :type :ok, :f :txn, :value [[:r :x #{1 2}]], :index 7}]
                                          h/history))

(def example-d-CC-CM-and-CCv-but-not-sequentially-consistent
  (->> [{:process 0, :type :ok, :f :txn, :value [[:append :x 1]], :index 1}
        {:process 0, :type :ok, :f :txn, :value [[:r :y nil]], :index 3}
        {:process 0, :type :ok, :f :txn, :value [[:append :y 1]], :index 5}
        {:process 0, :type :ok, :f :txn, :value [[:r :x #{1}]], :index 7}
        {:process 1, :type :ok, :f :txn, :value [[:append :x 2]], :index 9}
        {:process 1, :type :ok, :f :txn, :value [[:r :y nil]], :index 11}
        {:process 1, :type :ok, :f :txn, :value [[:append :y 2]], :index 13}
        {:process 1, :type :ok, :f :txn, :value [[:r :x #{2}]], :index 15}]
       h/history))

(def example-e-not-CC-nor-CM-nor-CCv-interpretation-1
  (->> [{:process 0, :type :ok, :f :txn, :value [[:append :x 1]], :index 1}
        {:process 0, :type :ok, :f :txn, :value [[:append :y 1]], :index 3}
        {:process 1, :type :ok, :f :txn, :value [[:r :y #{1}]], :index 5}
        {:process 1, :type :ok, :f :txn, :value [[:append :x 2]], :index 7}
        {:process 2, :type :ok, :f :txn, :value [[:r :x #{2}]], :index 9}
        {:process 2, :type :ok, :f :txn, :value [[:r :x #{1}]], :index 11}]
       h/history))

(def example-e-not-CC-nor-CM-nor-CCv-interpretation-2
  (->> [{:process 0, :type :ok, :f :txn, :value [[:append :x 1]], :index 1}
        {:process 0, :type :ok, :f :txn, :value [[:append :y 1]], :index 3}
        {:process 1, :type :ok, :f :txn, :value [[:r :y #{1}]], :index 5}
        {:process 1, :type :ok, :f :txn, :value [[:append :x 2]], :index 7}
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
    (let [output-dir (str output-dir "/cc")
          opts       (assoc causal-opts/causal-opts :directory output-dir)]
      (is (= {:valid? true}
             (adya/check opts valid-cc)))
      (is (= {:valid? true}
             (adya/check opts valid-cc-rw)))
      (is (= {:valid? false
              :anomaly-types [:G-single-item]
              :not #{:consistent-view :repeatable-read}}
             (-> (adya/check opts invalid-cc)
                 (select-keys results-of-interest)))))))

(deftest wr
  (testing "w->r"
    (let [output-dir (str output-dir "/wr")
          opts       (assoc causal-opts/causal-opts :directory output-dir)]
      (is (= {:valid? false
              :anomaly-types [:G1c-process :cyclic-versions]
              :not #{:read-uncommitted}}
             (-> (adya/check opts invalid-wr)
                 (select-keys results-of-interest))))
      (is (= {:valid? false
              :anomaly-types [:G1c-process :cyclic-versions]
              :not #{:read-uncommitted}}
             (-> (adya/check opts invalid-wr-single-process)
                 (select-keys results-of-interest))))
      ;; TODO: real real-time
      ;; (is (= {:valid? false
      ;;         :anomaly-types [:cac]
      ;;         :not #{}}
      ;;        (-> (cc/check opts invalid-wr-realtime)
      ;;            (select-keys results-of-interest))))
      )))

(deftest internal
  (testing "internal"
    (let [output-dir (str output-dir "/internal")
          opts       (assoc causal-opts/causal-opts :directory output-dir)]
      (is (= {:valid? true}
             (-> (adya/check opts valid-internal)
                 (select-keys results-of-interest))))
      (is (= {:valid? false
              :anomaly-types [:cyclic-versions :internal]
              :not #{:read-uncommitted}}
             (-> (adya/check opts invalid-internal-ryw)
                 (select-keys results-of-interest))))
      (is (= {:valid? false
              :anomaly-types [:G-single-item :cyclic-versions :internal]
              :not #{:read-uncommitted}}
             (-> (adya/check opts invalid-internal-mono-reads)
                 (select-keys results-of-interest))))
      (is (= {:valid? false
              :anomaly-types [:cyclic-versions :future-read]
              :not #{:read-uncommitted}}
             (-> (adya/check opts invalid-internal-future-read)
                 (select-keys results-of-interest)))))))

(deftest G1a
  (testing "G1a"
    (let [output-dir (str output-dir "/G1a")
          opts       (-> causal-opts/causal-opts
                         (assoc :directory output-dir)
                         (update :anomalies conj :garbage-versions))]
      (is (= {:valid? false
              :anomaly-types [:G1a :garbage-versions]
              :not #{:read-committed}}
             (-> (adya/check opts invalid-G1a)
                 (select-keys results-of-interest))))
      (is (= {:valid? false
              :anomaly-types [:G1a :garbage-versions]
              :not #{:read-committed}}
             (-> (adya/check opts invalid-G1a-mops)
                 (select-keys results-of-interest)))))))

(deftest G1b
  (testing "G1b"
    (let [output-dir (str output-dir "/G1b")
          opts       (assoc causal-opts/causal-opts :directory output-dir)]
      (is (= {:valid? false
              :anomaly-types [:G-single-item :G1b]
              :not #{:read-committed}}
             (-> (adya/check opts invalid-G1b)
                 (select-keys results-of-interest))))
      (is (= {:valid? false
              :anomaly-types [:G-single-item :G1b]
              :not #{:read-committed}}
             (-> (adya/check opts invalid-G1b-read-of-final)
                 (select-keys results-of-interest)))))))

(deftest read-your-writes
  (testing "read-your-writes"
    (let [output-dir (str output-dir "/read-your-writes")
          opts       (assoc causal-opts/causal-opts :directory output-dir)]
      (is (= {:valid? true}
             (adya/check opts valid-ryw)))
      (is (= {:valid? false
              :anomaly-types [:G-single-item-process :cyclic-versions]
              :not #{:read-uncommitted}}
             (-> (adya/check opts invalid-ryw)
                 (select-keys results-of-interest))))
      (is (= {:valid? false
              :anomaly-types [:G-single-item-process :cyclic-versions]
              :not #{:read-uncommitted}}
             (-> (adya/check opts invalid-ryw-not-nil)
                 (select-keys results-of-interest))))
      (is (= {:valid? false
              :anomaly-types [:G-single-item-process :cyclic-versions]
              :not #{:read-uncommitted}}
             (-> (adya/check opts invalid-ryw-+2)
                 (select-keys results-of-interest)))))))

(deftest monotonic-writes
  (testing "monotonic-writes"
    (let [output-dir (str output-dir "/monotonic-writes")
          opts       (assoc causal-opts/causal-opts :directory output-dir)]
      (is (= {:valid? true}
             (adya/check opts valid-monotonic-writes)))
      (is (= {:valid? false
              :anomaly-types [:G-single-item-process :cyclic-versions]
              :not #{:read-uncommitted}}
             (-> (adya/check opts invalid-monotonic-writes)
                 (select-keys results-of-interest)))))))

(deftest writes-follow-reads
  (testing "writes-follow-reads"
    (let [output-dir (str output-dir "/writes-follow-reads")
          opts       (assoc causal-opts/causal-opts :directory output-dir)]
      (is (= {:valid? true}
             (adya/check opts valid-wfr)))
      (is (= {:valid? false
              :anomaly-types [:G-single-item-process]
              :not #{:strong-session-snapshot-isolation}}
             (-> (adya/check opts invalid-wfr)
                 (select-keys results-of-interest))))
      (is (= {:valid? false
              :anomaly-types [:G-single-item-process]
              :not #{:strong-session-snapshot-isolation}}
             (-> (adya/check opts invalid-wfr-mop)
                 (select-keys results-of-interest))))
      (is (= {:valid? false
              :anomaly-types [:G-single-item]
              :not #{:consistent-view :repeatable-read}}
             (-> (adya/check opts invalid-wfr-mops)
                 (select-keys results-of-interest))))
      (is (= {:valid? false
              :anomaly-types [:G-single-item]
              :not #{:consistent-view :repeatable-read}}
             (-> (adya/check opts invalid-wfr-all-mops)
                 (select-keys results-of-interest)))))))

(deftest monotonic-reads
  (testing "monotonic-reads"
    (let [output-dir (str output-dir "/monotonic-reads")
          opts       (assoc causal-opts/causal-opts :directory output-dir)]
      (is (= {:valid? false
              :anomaly-types [:G-single-item-process :cyclic-versions]
              :not #{:read-uncommitted}}
             (-> (adya/check opts invalid-monotonic-reads)
                 (select-keys results-of-interest)))))))

(deftest simulated
  (testing "simulated"
    ; different seeds
    ;; TODO: PR history.sim
    ;; (doseq [seed [23 42 69]]
    ;;   (let [output-dir (str output-dir "/simulated/" seed)
    ;;         opts       (assoc causal-opts/causal-opts :directory output-dir)
    ;;         history    (->> {:db          :causal-lww
    ;;                          :limit       10000
    ;;                          :concurrency 10
    ;;                          :seed        seed}
    ;;                         h-sim/run
    ;;                         :history)]
    ;;     (is (= {:valid? true}
    ;;            (-> (adya/check opts history)
    ;;                (select-keys results-of-interest))))))
    ; brat db
    (let [output-dir (str output-dir "/simulated/brat")
          opts       (-> causal-opts/causal-opts
                         (assoc :directory output-dir)
                         (update :anomalies conj :garbage-versions))
          history    (->> {:db          :brat
                           :limit       50
                           :concurrency 5}
                          h-sim/run
                          :history)]
      (is (= {:valid? false
              :anomaly-types [:garbage-versions :internal]
              :not #{:read-atomic}}
             (-> (adya/check opts history)
                 (select-keys results-of-interest)))))
    ; prefix db
    (let [output-dir (str output-dir "/simulated/prefix")
          opts       (assoc causal-opts/causal-opts :directory output-dir)
          history    (->> {:db          :prefix
                           :limit       10000
                           :concurrency 10}
                          h-sim/run
                          :history)]
      (is (= {:valid? true}
             (-> (adya/check opts history)
                 (select-keys results-of-interest)))))
    ; si db
    (let [output-dir (str output-dir "/simulated/si")
          opts       (assoc causal-opts/causal-opts :directory output-dir)
          history    (->> {:db          :si
                           :limit       10000
                           :concurrency 10}
                          h-sim/run
                          :history)]
      (is (= {:valid? true}
             (-> (adya/check opts history)
                 (select-keys results-of-interest)))))
    ; causal-lww db with failures
    ;; TODO: PR history.sim
    ;; (let [output-dir (str output-dir "/simulated/failure")
    ;;       opts       (assoc causal-opts/causal-opts :directory output-dir)
    ;;       history    (->> {:db           :causal-lww
    ;;                        :limit        10000
    ;;                        :concurrency  10
    ;;                        :failure-rate 500}
    ;;                       h-sim/run
    ;;                       :history)]
    ;;   (is (= {:valid? false
    ;;           :anomaly-types [:G-single-item-process]
    ;;           :not #{:repeatable-read :snapshot-isolation :strong-session-consistent-view}}
    ;;          (-> (adya/check opts history)
    ;;              (select-keys results-of-interest)))))
    ))

;; (deftest internal
;;   (testing "internal"
;;     (let [output-dir (str output-dir "/internal")
;;           opts       (assoc workload/causal-opts :directory output-dir)]
;;       (is (= {:valid? true}
;;              (-> (cc/check opts valid-internal)
;;                  (select-keys results-of-interest))))
;;       (is (= {:valid? false
;;               :anomaly-types [:internal]
;;               :not #{:read-atomic}}
;;              (-> (cc/check opts invalid-internal-ryw)
;;                  (select-keys results-of-interest))))
;;       (is (= {:valid? false
;;               :anomaly-types [:G-single-item :internal :monotonic-reads]
;;               :not #{:consistent-view :read-atomic :repeatable-read}}
;;              (-> (cc/check opts invalid-internal-mono-reads)
;;                  (select-keys results-of-interest)))))))

;; (deftest G1a
;;   (testing "G1a"
;;     (let [output-dir (str output-dir "/G1a")
;;           opts       (assoc workload/causal-opts :directory output-dir)]
;;       (is (= {:valid? false
;;               :anomaly-types [:G1a]
;;               :not #{:read-committed}}
;;              (-> (cc/check opts invalid-G1a)
;;                  (select-keys results-of-interest))))
;;       (is (= {:valid? false
;;               :anomaly-types [:G1a]
;;               :not #{:read-committed}}
;;              (-> (cc/check opts invalid-G1a-mops)
;;                  (select-keys results-of-interest)))))))

;; (deftest G1b
;;   (testing "G1b"
;;     (let [output-dir (str output-dir "/G1b")
;;           opts       (assoc workload/causal-opts :directory output-dir)]
;;       (is (= {:valid? false
;;               :anomaly-types [:G-single-item :G1b]
;;               :not #{:read-committed}}
;;              (-> (cc/check opts invalid-G1b)
;;                  (select-keys results-of-interest)))))))

;; (deftest Bouajjani
;;   (testing "Bouajjani"
;;     (let [output-dir (str output-dir "/Bouajjani")
;;           opts       (assoc workload/causal-opts :directory output-dir)]
;;       (is (= {:valid? false :anomaly-types [:G-single-item-process]}
;;              (-> (cc/check opts example-a-CM-but-not-CCv)
;;                  (select-keys [:valid? :anomaly-types]))))
;;       (is (= {:valid? false :anomaly-types [:G-single-item-process]}
;;              (-> (cc/check opts example-b-CCv-but-not-CM)
;;                  (select-keys [:valid? :anomaly-types]))))
;;       (is (= {:valid? false :anomaly-types [:G-single-item-process]}
;;              (-> (cc/check opts example-c-CC-but-not-CM-nor-CCv)
;;                  (select-keys [:valid? :anomaly-types]))))

;;       ;; note, we are making the argument that "sequentially consistent"
;;       ;; isn't part of the common definition of Causal Consistency
;;       (is (= {:valid? true}
;;              (-> (cc/check opts example-d-CC-CM-and-CCv-but-not-sequentially-consistent)
;;                  (select-keys [:valid? :anomaly-types]))))

;;       (is (= {:valid? false :anomaly-types [:G-single-item-process :monotonic-reads]}
;;              (-> (cc/check opts example-e-not-CC-nor-CM-nor-CCv-interpretation-1)
;;                  (select-keys [:valid? :anomaly-types]))))
;;       (is (= {:valid? false :anomaly-types [:G-single-item-process :monotonic-reads]}
;;              (-> (cc/check opts example-e-not-CC-nor-CM-nor-CCv-interpretation-2)
;;                  (select-keys [:valid? :anomaly-types])))))))


(comment
  (def last-history
    (->> (store/test -1)
         :history))

  (def last-history-oks
    (->> last-history
         h/oks))

  (def last-history-indexes
    (->> last-history-oks
         adya/indexes))


  (defn version-filter
    "Given a set of versions, #{[k v]}, and a history,
   returns all ok ops that interacted with any of the [k v]."
    [vers history]
    (let [history-oks (->> history
                           h/oks)
          {:keys [write-index read-index]
           :as   _indexes} (adya/indexes history-oks)

        ; writes and reads of vers
          ops (->> vers
                   (reduce (fn [ops kv]
                             (-> ops
                                 (conj      (get write-index kv))
                                 (set/union (get read-index  kv))))
                           #{}))]
    ; output will be sorted by :index    
      (->> ops
           (into (sorted-set-by (fn [op op']
                                  (compare (:index op) (:index op')))))))))
