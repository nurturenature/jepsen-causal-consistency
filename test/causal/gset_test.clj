(ns causal.gset-test
  (:require [clojure.test :refer :all]
            [causal.lww-register :as lww]
            [elle.rw-register :as rw]))

(deftest causal
  (testing "causal"
    (is (:valid? (rw/check lww/causal-opts lww/causal-ok)))

    (is (= (select-keys (rw/check lww/causal-opts lww/causal-2-mops-anomaly) [:valid? :anomaly-types :not])
           {:valid? false :anomaly-types [:G-single-item] :not #{:consistent-view :repeatable-read}}))))

(deftest read-your-writes
  (testing "read-your-writes"
    (is (:valid? (rw/check lww/causal-opts lww/ryw-ok)))

    (is (= (select-keys (rw/check lww/causal-opts lww/ryw-anomaly) [:valid? :anomaly-types :not])
           {:valid? false :anomaly-types [:G-single-item-process :cyclic-versions] :not #{:read-uncommitted}}))))

(deftest monotonic-writes
  (testing "monotonic-writes"
    (is (:valid? (rw/check lww/causal-opts lww/monotonic-writes-ok)))
    (is (:valid? (rw/check lww/causal-opts lww/monotonic-writes-diff-key-ok)))

    (is (= (select-keys (rw/check lww/causal-opts lww/monotonic-writes-anomaly) [:valid? :anomaly-types :not])
           {:valid? false :anomaly-types [:cyclic-versions] :not #{:read-uncommitted}}))
    (is (= (select-keys (rw/check lww/causal-opts lww/monotonic-writes-diff-key-anomaly) [:valid? :anomaly-types :not])
           {:valid? false :anomaly-types [:G-single-item-process] :not #{:strong-session-consistent-view}}))))

(deftest writes-follow-reads
  (testing "writes-follow-reads"
    (is (:valid? (rw/check lww/causal-opts lww/wfr-ok)))

    (is (= (select-keys (rw/check lww/causal-opts lww/wfr-anomaly) [:valid? :anomaly-types :not])
           {:valid? false :anomaly-types [:G-single-item] :not #{:consistent-view :repeatable-read}}))
    (is (= (select-keys (rw/check lww/causal-opts lww/wfr-1-mop-anomaly) [:valid? :anomaly-types :not])
           {:valid? false :anomaly-types [:G-single-item-process] :not #{:strong-session-consistent-view}}))
    (is (= (select-keys (rw/check lww/causal-opts lww/wfr-2-mop-anomaly) [:valid? :anomaly-types :not])
           {:valid? false :anomaly-types [:G-single-item-process] :not #{:strong-session-consistent-view}}))))

(deftest lww
  (testing "lww"
    (is (:valid? (rw/check lww/causal-opts lww/lww-ok)))

    (is (= (select-keys (rw/check lww/causal-opts lww/lww-anomaly) [:valid? :anomaly-types :not])
           {:valid? false :anomaly-types [:cyclic-versions] :not #{:read-uncommitted}}))))

; we ignore lost-update in our causal consistency model
(deftest lost-update
  (testing "lost-update"
    (is (:valid? (rw/check lww/causal-opts lww/lost-update)))))

; false positives we don't understand yet
(deftest false-positive
  (testing "false-positive"
    (is (= (select-keys (rw/check lww/causal-opts lww/not-g-single-item-lost-update) [:valid? :anomaly-types])
           {:valid? true}))))
