(ns crdt.lww-register-test
  (:require [clojure.test :refer :all]
            [crdt.lww-register :as lww]
            [elle.rw-register :as rw]))

(deftest causal
  (testing "causal"
    (is (:valid? (rw/check lww/causal-opts lww/causal-ok)))
    (is (not (:valid? (rw/check lww/causal-opts lww/causal-intra-txn-anomaly))))
    (is (not (:valid? (rw/check lww/causal-opts lww/causal-inter-txn-anomaly))))))
