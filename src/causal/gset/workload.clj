(ns causal.gset.workload
  "A test which looks for cycles in write/read transactions.
   Writes are assumed to be unique, but this is the only constraint.
   See jepsen.tests.cycle.wr and elle.rw-register for docs."
  (:require [causal.gset.client :as client]
            [jepsen
             [history :as h]
             [generator :as gen]]
            [jepsen.tests.cycle.wr :as wr]))

(def causal-opts
  "Opts to configure Elle for causal consistency."
  ; rw_register provides:
  ;   - initial nil -> all versions for all keys
  ;   - w->r
  ;   - ww and rw dependencies, as derived from a version order
  {:consistency-models [:strong-session-consistent-view] ; Elle's strong-session with Adya's formalism for causal consistency
   :anomalies-ignored [:lost-update]                     ; `lost-update`s are causally Ok, they are PL-2+, Adya 4.1.3
   :sequential-keys? true                                ; infer version order from elle/process-graph
   ;:linearizable-keys? true                             ; TODO: should be LWW?
   :wfr-keys? true                                       ; wfr-version-graph when <rw within txns
   ;:wfr-process? true TODO: valid? explainer?           ; wfr-process-graph used to infer version order
   ;:additional-graphs [rw/wfr-ww-transaction-graph] TODO: valid? explainer?
   })

(defn workload
  "Gset workload."
  [{:keys [rate] :as opts}]
  (let [opts (merge
              {:directory      "."
               :max-plot-bytes 1048576
               :plot-timeout   10000}
              opts)
        gen       (wr/gen opts)
        final-gen (gen/phases
                   (gen/log "Quiesce...")
                   (gen/sleep 5)
                   (gen/log "Final reads...")
                   (->> (range 100)
                        (map (fn [k]
                               {:type :invoke :f :r-final :value [[:r k nil]] :final-read? true}))
                        (gen/each-thread)
                        (gen/clients)
                        (gen/stagger (/ rate))))]
    {:client          (client/->GSetClient nil)
     :generator       gen
     :final-generator final-gen}))

(defn workload-homogeneous-txns
  "A workload with a generator that emits transactions that are all read or write ops,
   E.g. for the ElectricSQL TypeScript client.
   Generator must only generate txns consisting exclusively of reads or writes
   to accommodate the API."
  [{:keys [min-txn-length max-txn-length] :as opts}]
  (let [min-txn-length (* 2 (or min-txn-length 1))
        max-txn-length (* 2 (or max-txn-length 4))
        opts           (assoc opts
                              :min-txn-length     min-txn-length
                              :max-txn-length     max-txn-length
                              :key-dist           :uniform
                              :key-count          100
                              :max-writes-per-key 1000)
        workload (workload opts)
        generator (->> (:generator workload)
                       (mapcat (fn [{:keys [value] :as op}]
                                 (let [[rs ws] (->> value
                                                    (reduce (fn [[rs ws] [f _k _v :as mop]]
                                                              (case f
                                                                :r (if (some #(= % mop) rs)
                                                                     [rs ws]
                                                                     [(conj rs mop) ws])
                                                                :w [rs (conj ws mop)]))
                                                            [[] []]))]
                                   (cond (and (seq rs)
                                              (seq ws))
                                         (let [r-op (assoc op :value rs :f :r-txn)
                                               w-op (assoc op :value ws :f :w-txn)]
                                           (->> [r-op w-op] shuffle vec))

                                         (seq rs)
                                         [(assoc op :value rs :f :r-txn)]

                                         (seq ws)
                                         [(assoc op :value ws :f :w-txn)])))))]
    (assoc workload
           :generator generator)))

(defn workload-single-writes
  "The default workload with a generator that emits transactions consisting of a single write."
  [opts]
  (let [opts      (merge opts
                         {:min-txn-length     1
                          :max-txn-length     1
                          :key-dist           :uniform
                          :key-count          100
                          :max-writes-per-key 1000})
        workload  (workload opts)
        generator (->> (:generator workload)
                       (filter #(->> %
                                     :value
                                     first
                                     first
                                     (= :w)))
                       (map #(assoc % :f :w-txn)))]
    (assoc workload
           :generator generator)))

(defn cyclic-versions-helper
  "Given a cyclic-versions result map and a history, filter history for involved transactions."
  [{:keys [key scc] :as _cyclic-versions} history]
  (->> history
       h/client-ops
       h/oks
       (h/filter (fn [{:keys [value] :as _op}]
                   (->> value
                        (reduce (fn [_acc [_f k v]]
                                  (if (and (= key k)
                                           (contains? scc v))
                                    (reduced true)
                                    false))
                                false))))
       (map (fn [op]
              (select-keys op [:index :process :value])))))

(defn op
  "Generates an operation from a string language like so:

  wx1       set x = 1
  ry1       read y = 1
  wx1wx2    set x=1, x=2"
  ([string]
   (let [[txn mop] (reduce (fn [[txn [f k _v :as mop]] c]
                             (case c
                               \w [(conj txn mop) [:w]]
                               \r [(conj txn mop) [:r]]
                               \x [txn (conj mop :x)]
                               \y [txn (conj mop :y)]
                               \z [txn (conj mop :z)]
                               (let [e (if (= \_ c)
                                         nil
                                         (Long/parseLong (str c)))]
                                 [txn [f k e]])))
                           [[] nil]
                           string)
         txn (-> txn
                 (subvec 1)
                 (conj mop))]
     {:process 0, :type :ok, :f :txn :value txn}))
  ([process type string]
   (assoc (op string) :process process :type type)))

(defn fail
  "Fails an op."
  [op]
  (assoc op :type :fail))

(defn invoke
  "Takes a completed op and returns an invocation."
  [completion]
  (-> completion
      (assoc :type :invoke)
      (update :value (partial map (fn [[f k _v :as mop]]
                                    (if (= :r f)
                                      [f k nil]
                                      mop))))))

(defn op-pair
  ([txn] (op-pair 0 txn))
  ([p txn]
   (let [op     (op p :ok txn)
         invoke (invoke op)]
     [invoke op])))

(def causal-ok
  (->> [[0 "wx0"]
        [1 "rx0wy1"]
        [2 "ry1rx0"]]
       (mapcat #(apply op-pair %))
       h/history))

(def causal-2-mops-anomaly
  (->> [[0 "wx0"]
        [1 "rx0wy1"]
        [2 "ry1rx_"]]
       (mapcat #(apply op-pair %))
       h/history))

(def ryw-ok
  (->> [[0 "wx0"]
        [0 "rx0"]]
       (mapcat #(apply op-pair %))
       h/history))

(def ryw-anomaly
  (->> [[0 "wx0"]
        [0 "rx_"]]
       (mapcat #(apply op-pair %))
       h/history))

(def monotonic-writes-ok
  (->> [[0 "wx0"]
        [0 "wx1"]
        [1 "rx0"]
        [1 "rx1"]]
       (mapcat #(apply op-pair %))
       h/history))

(def monotonic-writes-anomaly
  (->> [[0 "wx0"]
        [0 "wx1"]
        [1 "rx1"]
        [1 "rx0"]]
       (mapcat #(apply op-pair %))
       h/history))

(def monotonic-writes-diff-key-ok
  (->> [[0 "wx0"]
        [0 "wx1"]
        [0 "wy2"]
        [1 "ry2"]
        [1 "rx1"]]
       (mapcat #(apply op-pair %))
       h/history))

(def monotonic-writes-diff-key-anomaly
  (->> [[0 "wx0"]
        [0 "wy1"]
        [1 "ry1"]
        [1 "rx_"]]
       (mapcat #(apply op-pair %))
       h/history))

(def wfr-ok
  (->> [[0 "wx0"]
        [1 "rx0"]
        [1 "wy1"]
        [2 "ry1rx0"]]
       (mapcat #(apply op-pair %))
       h/history))

(def wfr-1-mop-anomaly
  (->> [[0 "wx0"]
        [1 "rx0"]
        [1 "wy1"]
        [2 "ry1"]
        [2 "rx_"]]
       (mapcat #(apply op-pair %))
       h/history))

(def wfr-2-mop-anomaly
  (->> [[0 "wx0"]
        [1 "rx0"]
        [1 "wy1"]
        [2 "rx_ry1"]]
       (mapcat #(apply op-pair %))
       h/history))

(def wfr-anomaly
  (->> [[0 "wx0"]
        [1 "rx0wy1"]
        [2 "ry1rx_"]]
       (mapcat #(apply op-pair %))
       h/history))

(def internal-ok
  (->> [[0 "wx0"]
        [0 "wx1wx2rx2"]]
       (mapcat #(apply op-pair %))
       h/history))

(def internal-anomaly
  (->> [[0 "wx0"]
        [0 "wx1wx2rx1"]]
       (mapcat #(apply op-pair %))
       h/history))

(def lww-ok
  (let [[p0-wx0 p0-wx0'] (op-pair 0 "wx0")
        [p1-wx1 p1-wx1'] (op-pair 1 "wx1")
        [p0-rx0 p0-rx0'] (op-pair 0 "rx0")
        [p1-rx1 p1-rx1'] (op-pair 1 "rx1")]
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
  (let [[p0-wx0 p0-wx0'] (op-pair 0 "wx0")
        [p1-wx1 p1-wx1'] (op-pair 1 "wx1")
        [p0-rx0 p0-rx0'] (op-pair 0 "rx0")
        [p0-rx1 p0-rx1'] (op-pair 0 "rx1")
        [p1-rx0 p1-rx0'] (op-pair 1 "rx0")
        [p1-rx1 p1-rx1'] (op-pair 1 "rx1")]
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
       (mapcat #(apply op-pair %))
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
