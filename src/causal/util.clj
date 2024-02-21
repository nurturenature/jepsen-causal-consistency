(ns causal.util
  (:require [jepsen.generator :as gen]
            [jepsen.tests.cycle.wr :as wr]))

(defn generator
  "wr/get with common options."
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
  [{:keys [rate] :as _opts}]
  (gen/phases
   (gen/log "Quiesce...")
   (gen/sleep 3)
   (gen/log "Final reads...")
   (->> (range 100)
        (map (fn [k]
               {:type :invoke :f :r-final :value [[:r k nil]] :final-read? true}))
        (gen/each-thread)
        (gen/clients)
        (gen/stagger (/ rate)))))

