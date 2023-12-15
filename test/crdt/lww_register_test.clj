(ns crdt.lww-register-test
  (:require [clojure.test :refer :all]
            [crdt.lww-register :as lww]
            [elle.rw-register :as rw]))

(deftest causal
  (testing "causal"
    (is (:valid? (rw/check lww/causal-opts lww/causal-ok)))

    ; not possible to catch so expect valid
    (is (:valid? (rw/check lww/causal-opts lww/causal-1-mop-anomaly)))
    (is (:valid? (rw/check lww/causal-opts lww/causal-2-mop-anomaly)))

    (is (not (:valid? (rw/check lww/causal-opts lww/causal-2-mops-anomaly))))))

(deftest read-your-writes
  (testing "read-your-writes"
    (is (:valid? (rw/check lww/causal-opts lww/ryw-ok)))
    (is (not (:valid? (rw/check lww/causal-opts lww/ryw-anomaly))))))

(deftest monotonic-writes
  (testing "monotonic-writes"
    (is (:valid? (rw/check lww/causal-opts lww/monotonic-writes-ok)))
    (is (not (:valid? (rw/check lww/causal-opts lww/monotonic-writes-anomaly))))
    (is (:valid? (rw/check lww/causal-opts lww/monotonic-writes-diff-key-ok)))

    ; not possible to catch so expect valid
    (is (:valid? (rw/check lww/causal-opts lww/monotonic-writes-diff-key-anomaly)))))

(deftest writes-follow-reads
  (testing "writes-follow-reads"
    (is (:valid? (rw/check lww/causal-opts lww/wfr-ok)))
    (is (not (:valid? (rw/check lww/causal-opts lww/wfr-anomaly))))

    ; not possible to catch so expect valid
    (is (:valid? (rw/check lww/causal-opts lww/wfr-1-mop-anomaly)))))

(deftest internal
  (testing "internal"
    (is (:valid? (rw/check lww/causal-opts lww/internal-ok)))

    ; show that :internal is needed
    ; not possible to catch so expect valid
    (is (:valid? (rw/check (dissoc lww/causal-opts :anomalies) lww/internal-anomaly)))))

(deftest lww
  (testing "lww"
    (is (:valid? (rw/check lww/causal-opts lww/lww-ok)))
    (is (not (:valid? (rw/check lww/causal-opts lww/lww-anomaly))))))
