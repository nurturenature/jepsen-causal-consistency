(ns causal.util
  (:require [jepsen.generator :as gen]
            [jepsen.tests.cycle.wr :as wr]))

(defn generator
  "wr/gen with common options."
  [opts]
  (let [opts (merge
              {:directory      "."
               :max-plot-bytes 1048576
               :plot-timeout   10000}
              opts)
        gen (wr/gen opts)]
    gen))

(defn final-generator
  "final-generator for generator."
  [_opts]
  (gen/phases
   (gen/log "Quiesce...")
   (gen/sleep 3)
   (gen/log "Final reads...")
   (->> (range 100)
        (map (fn [k]
               {:type :invoke :f :r-final :value [[:r k nil]] :final-read? true}))
        (gen/each-thread)
        (gen/clients))))

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
