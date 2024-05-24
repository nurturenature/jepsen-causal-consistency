(ns causal.util
  (:require [jepsen.control :as c]
            [jepsen.generator :as gen]
            [jepsen.tests.cycle.wr :as wr]))

(defn reduce-nested
  "Convenience for nested maps, {k {k' v}}.
   Reduces with (fn acc k k' v) for all k and k'."
  [reduce-fn init-state coll]
  (->> coll
       (reduce-kv (fn [acc k inner-map]
                    (->> inner-map
                         (reduce-kv (fn [acc k' v]
                                      (reduce-fn acc k k' v))
                                    acc)))
                  init-state)))

(def causal-opts
  "Opts to configure Elle for causal consistency."
  {;;  :consistency-models [:strong-session-consistent-view] ; Elle's strong-session with Adya's Consistent View(PL-2+)
  ;;  :anomalies          [:internal                        ; basic hygiene to read your writes in a transaction
  ;;                       :garbage-versions                ; lww list append only
  ;;                       :cyclic-transactions             ; lww list append only
  ;;                       :cac                             ; lww list append only
  ;;                       ]
  ;;  :anomalies-ignored  [:lost-update]                    ; `lost-update`s are causally Ok, but they are PL-2+, Adya 4.1.3 ?!?

   ;; TODO: either PR elle or revert
   :consistency-models [:strong-session-PL-2]
   :anomalies [:G-cursor :G-monotonic :G-single :G-single-item :G-single-item-process :G-single-process :G1-process
               :internal                        ; basic hygiene to read your writes in a transaction
               :garbage-versions                ; lww list append only
               :cyclic-transactions             ; lww list append only
               :cac                             ; lww list append only
               ]

   ; where to store anomaly explanations, graphs
   :directory "causal"

   ; causal graph analysis and plotting can be resource intensive
   :cycle-search-timeout 10000
   :max-plot-bytes       1000000
   :plot-timeout         10000})

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

(defn git-clean-pull
  "git cleans and pulls in the CWD."
  []
  (c/exec :git :reset :--hard :HEAD)
  (c/exec :git :clean :-f)
  (c/exec :git :pull))

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
