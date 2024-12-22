(ns causal.repl
  "A namespace designed to be an entry into the REPL.
   Requires most commonly needed namespaces."
  (:require [bifurcan-clj
             [graph :as bg]]
            [causal.checker
             [adya :as adya]
             [cyclic-versions :as cyclic-versions]
             [graph :as graph]
             [lww :as lww]
             [opts :as causal-opts]
             [strong-convergence :as strong-convergence]]
            [clojure.set :as set]
            [elle
             [core :as e-core]
             [graph :as e-graph]
             [list-append :as e-list-append]
             [txn :as e-txn]]
            [jepsen
             [history :as h]
             [store :as store]]))

(def sample-op
  {:type :invoke
   :f    :txn
   :value [[:r 0 nil]
           [:append 0 0]
           [:r 0 nil]
           [:append 0 1]
           [:r 0 nil]]})
