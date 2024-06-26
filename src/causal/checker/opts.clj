(ns causal.checker.opts)

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
               ; TODO: implement garbage reads as an independent step in checker
               ;       remove detection from graph building
               ; :garbage-versions
               :cyclic-transactions]

   ; where to store anomaly explanations, graphs
   :directory "causal"

   ; causal graph analysis and plotting can be resource intensive
   :cycle-search-timeout 10000
   :max-plot-bytes       1000000
   :plot-timeout         10000})

