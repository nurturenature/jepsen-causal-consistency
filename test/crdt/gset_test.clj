(ns crdt.gset-test
  (:require [clojure.test :refer :all]
            [crdt.gset :refer :all]))

(deftest basic-history
  (testing "basic gset history"
    (is (:valid? (->> (gset-history)
                      causal-check)))))

(deftest read-your-writes
  (testing "read your writes"
    (is (:valid? (->> gset-ryw-history
                      causal-check)))
    (is (not (:valid? (->> gset-ryw-anomaly-history
                           causal-check))))))

(deftest monotonic-writes
  (testing "monotonic writes"
    (is (:valid? (->> gset-mono-w-history
                      causal-check)))
    (is (not (:valid? (->> gset-mono-w-anomaly-history
                           causal-check))))))

(deftest monotonic-reads
  (testing "monotonic reads"
    (is (:valid? (->> gset-mono-r-history
                      causal-check)))
    (is (not (:valid? (->> gset-mono-r-anomaly-history
                           causal-check))))))

(deftest writes-follow-reads
  (testing "writes follow reads"
    (is (:valid? (->> gset-wfr-history
                      causal-check)))
    (is (not (:valid? (->> gset-wfr-anomaly-history
                           causal-check))))))
