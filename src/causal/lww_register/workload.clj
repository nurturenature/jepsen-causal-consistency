(ns causal.lww-register.workload
  "A test which looks for cycles in write/read transactions.
   Writes are assumed to be unique, but this is the only constraint.
   See jepsen.tests.cycle.wr and elle.rw-register for docs."
  (:require [causal.util :as util]
            [causal.lww-register
             [client :as client]
             [strong-convergence :as strong-convergence]]
            [jepsen
             [checker :as checker]]
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
  [opts]
  (let [opts (merge {:consistency-models [:monotonic-atomic-view] ; atomic transactions
                     :anomalies-ignored [:cyclic-versions]        ; too many false positives
                     :sequential-keys? true                       ; infer version order from elle/process-graph
                     :wfr-keys? true                              ; wfr-version-graph when <rw within txns
                     }
                    opts)]
    {:client          (client/->LWWRegisterClient nil)
     :generator       (util/generator opts)
     :final-generator (util/final-generator opts)
     :checker         (checker/compose
                       {:strong-convergence (strong-convergence/final-reads)
                        :elle               (wr/checker opts)})}))
