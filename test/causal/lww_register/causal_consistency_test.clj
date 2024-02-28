(ns causal.lww-register.causal-consistency-test
  (:require [causal.util :as u]
            [causal.lww-register.workload :as lww]
            [clojure.test :refer [deftest is testing]]
            [elle.rw-register :as rw]
            [jepsen.history :as h]))

(def causal-ok
  (->> [[0 "wx0"]
        [1 "rx0wy1"]
        [2 "ry1rx0"]]
       (mapcat #(apply u/op-pair %))
       h/history))

(def causal-2-mops-anomaly
  (->> [[0 "wx0"]
        [1 "rx0wy1"]
        [2 "ry1rx_"]]
       (mapcat #(apply u/op-pair %))
       h/history))

(def ryw-ok
  (->> [[0 "wx0"]
        [0 "rx0"]]
       (mapcat #(apply u/op-pair %))
       h/history))

(def ryw-anomaly
  (->> [[0 "wx0"]
        [0 "rx_"]]
       (mapcat #(apply u/op-pair %))
       h/history))

(def monotonic-writes-ok
  (->> [[0 "wx0"]
        [0 "wx1"]
        [1 "rx0"]
        [1 "rx1"]]
       (mapcat #(apply u/op-pair %))
       h/history))

(def monotonic-writes-anomaly
  (->> [[0 "wx0"]
        [0 "wx1"]
        [1 "rx1"]
        [1 "rx0"]]
       (mapcat #(apply u/op-pair %))
       h/history))

(def monotonic-writes-diff-key-ok
  (->> [[0 "wx0"]
        [0 "wx1"]
        [0 "wy2"]
        [1 "ry2"]
        [1 "rx1"]]
       (mapcat #(apply u/op-pair %))
       h/history))

(def monotonic-writes-diff-key-anomaly
  (->> [[0 "wx0"]
        [0 "wy1"]
        [1 "ry1"]
        [1 "rx_"]]
       (mapcat #(apply u/op-pair %))
       h/history))

(def wfr-ok
  (->> [[0 "wx0"]
        [1 "rx0"]
        [1 "wy1"]
        [2 "ry1rx0"]]
       (mapcat #(apply u/op-pair %))
       h/history))

(def wfr-1-mop-anomaly
  (->> [[0 "wx0"]
        [1 "rx0"]
        [1 "wy1"]
        [2 "ry1"]
        [2 "rx_"]]
       (mapcat #(apply u/op-pair %))
       h/history))

(def wfr-2-mop-anomaly
  (->> [[0 "wx0"]
        [1 "rx0"]
        [1 "wy1"]
        [2 "rx_ry1"]]
       (mapcat #(apply u/op-pair %))
       h/history))

(def wfr-anomaly
  (->> [[0 "wx0"]
        [1 "rx0wy1"]
        [2 "ry1rx_"]]
       (mapcat #(apply u/op-pair %))
       h/history))

(def internal-ok
  (->> [[0 "wx0"]
        [0 "wx1wx2rx2"]]
       (mapcat #(apply u/op-pair %))
       h/history))

(def internal-anomaly
  (->> [[0 "wx0"]
        [0 "wx1wx2rx1"]]
       (mapcat #(apply u/op-pair %))
       h/history))

(def lww-ok
  (let [[p0-wx0 p0-wx0'] (u/op-pair 0 "wx0")
        [p1-wx1 p1-wx1'] (u/op-pair 1 "wx1")
        [p0-rx0 p0-rx0'] (u/op-pair 0 "rx0")
        [p1-rx1 p1-rx1'] (u/op-pair 1 "rx1")]
    (->> [p0-wx0
          p1-wx1
          p0-wx0'
          p1-wx1'
          p0-rx0
          p0-rx0'
          p1-rx1
          p1-rx1']
         h/history)))

(def lww-anomaly
  (let [[p0-wx0 p0-wx0'] (u/op-pair 0 "wx0")
        [p1-wx1 p1-wx1'] (u/op-pair 1 "wx1")
        [p0-rx0 p0-rx0'] (u/op-pair 0 "rx0")
        [p0-rx1 p0-rx1'] (u/op-pair 0 "rx1")
        [p1-rx0 p1-rx0'] (u/op-pair 1 "rx0")
        [p1-rx1 p1-rx1'] (u/op-pair 1 "rx1")]
    (->> [p0-wx0
          p1-wx1
          p0-wx0'
          p1-wx1'
          p0-rx1
          p0-rx1'
          p1-rx0
          p1-rx0']
         h/history)))

(def not-g-single-item-lost-update
  (->> [[0 "wx0"]
        [1 "rx0wx1"]
        [2 "rx0wx2"]
        [3 "rx0"]
        [3 "rx1"]
        [3 "rx2"]]
       (mapcat #(apply u/op-pair %))
       h/history))

(def lost-update
  ; Hlost,update: r1 (x0, 10) r2(x0 , 10) w2(x2 , 15) c2 w1(x1 , 14) c1
  ;  [x0 << x2 << x1 ]
  (->> [{:process 1 :type :invoke :value [[:r :x nil] [:w :x 14]] :f :txn}
        {:process 2 :type :invoke :value [[:r :x nil] [:w :x 15]] :f :txn}
        {:process 2 :type :ok     :value [[:r :x 10]  [:w :x 15]] :f :txn}
        {:process 1 :type :ok     :value [[:r :x 10]  [:w :x 14]] :f :txn}]
       h/history))

(def g-monotonic-anomaly
  "Adya Weak Consistency 4.2.2
   Hnon2L : w1 (x 1) w1 (y 1) c1 w2 (y 2) w2 (x 2) w2 (z 2) r3 (x 2) w3 (z 3) r3 (y 1) c2 c3
   [x 1 << x 2 , y 1 << y 2 , z 2 << z 3]"
  (->> [{:process 1 :type :invoke :value [[:w :x 1] [:w :y 1]] :f :txn}
        {:process 1 :type :ok     :value [[:w :x 1] [:w :y 1]] :f :txn}
        {:process 2 :type :invoke :value [[:w :y 2] [:w :x 2] [:w :z 2]] :f :txn}
        {:process 3 :type :invoke :value [[:r :x nil] [:w :z 3] [:r :y nil]] :f :txn}
        {:process 2 :type :ok     :value [[:w :y 2] [:w :x 2] [:w :z 2]] :f :txn}
        {:process 3 :type :ok     :value [[:r :x 2] [:w :z 3] [:r :y 1]] :f :txn}]
       h/history))

(def g-monotonic-list-append-anomaly
  (->> [{:process 1 :type :invoke :value [[:append :x 1] [:append :y 1]] :f :txn}
        {:process 1 :type :ok     :value [[:append :x 1] [:append :y 1]] :f :txn}
        {:process 2 :type :invoke :value [[:append :y 2] [:append :x 2] [:append :z 2]] :f :txn}
        {:process 3 :type :invoke :value [[:r :x nil] [:append :z 3] [:r :y nil]] :f :txn}
        {:process 2 :type :ok     :value [[:append :y 2] [:append :x 2] [:append :z 2]] :f :txn}
        {:process 3 :type :ok     :value [[:r :x [1,2]] [:append :z 3] [:r :y [1]]] :f :txn}]
       h/history))

(deftest causal
  (testing "causal"
    (is (:valid? (rw/check lww/causal-opts causal-ok)))

    (is (= (select-keys (rw/check lww/causal-opts causal-2-mops-anomaly) [:valid? :anomaly-types :not])
           {:valid? false :anomaly-types [:G-single-item] :not #{:consistent-view :repeatable-read}}))))

(deftest read-your-writes
  (testing "read-your-writes"
    (is (:valid? (rw/check lww/causal-opts ryw-ok)))

    (is (= (select-keys (rw/check lww/causal-opts ryw-anomaly) [:valid? :anomaly-types :not])
           {:valid? false :anomaly-types [:G-single-item-process :cyclic-versions] :not #{:read-uncommitted}}))))

(deftest monotonic-writes
  (testing "monotonic-writes"
    (is (:valid? (rw/check lww/causal-opts monotonic-writes-ok)))
    (is (:valid? (rw/check lww/causal-opts monotonic-writes-diff-key-ok)))

    (is (= (select-keys (rw/check lww/causal-opts monotonic-writes-anomaly) [:valid? :anomaly-types :not])
           {:valid? false :anomaly-types [:cyclic-versions] :not #{:read-uncommitted}}))
    (is (= (select-keys (rw/check lww/causal-opts monotonic-writes-diff-key-anomaly) [:valid? :anomaly-types :not])
           {:valid? false :anomaly-types [:G-single-item-process] :not #{:strong-session-consistent-view}}))))

(deftest writes-follow-reads
  (testing "writes-follow-reads"
    (is (:valid? (rw/check lww/causal-opts wfr-ok)))

    (is (= (select-keys (rw/check lww/causal-opts wfr-anomaly) [:valid? :anomaly-types :not])
           {:valid? false :anomaly-types [:G-single-item] :not #{:consistent-view :repeatable-read}}))
    (is (= (select-keys (rw/check lww/causal-opts wfr-1-mop-anomaly) [:valid? :anomaly-types :not])
           {:valid? false :anomaly-types [:G-single-item-process] :not #{:strong-session-consistent-view}}))
    (is (= (select-keys (rw/check lww/causal-opts wfr-2-mop-anomaly) [:valid? :anomaly-types :not])
           {:valid? false :anomaly-types [:G-single-item-process] :not #{:strong-session-consistent-view}}))))

(deftest lww
  (testing "lww"
    (is (:valid? (rw/check lww/causal-opts lww-ok)))

    (is (= (select-keys (rw/check lww/causal-opts lww-anomaly) [:valid? :anomaly-types :not])
           {:valid? false :anomaly-types [:cyclic-versions] :not #{:read-uncommitted}}))))

; we ignore lost-update in our causal consistency model
(deftest lost-updates
  (testing "lost-updates"
    (is (:valid? (rw/check lww/causal-opts lost-update)))))

; false positives we don't understand yet
(deftest false-positive
  (testing "false-positive"
    (is (= {:valid? false,
            :anomaly-types '(:G-single-item)}
           (select-keys (rw/check lww/causal-opts not-g-single-item-lost-update) [:valid? :anomaly-types])))))