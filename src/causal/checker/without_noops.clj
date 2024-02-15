(ns causal.checker.without-noops
  (:require [clojure.set :as set]
            [jepsen
             [checker :as checker]
             [history :as h]]))

(defn without-noops
  "A checker that filters:
     - noop? true from history"
  [checker]
  (reify checker/Checker
    (check [_this test history opts]
      (let [history      (->> history
                              (h/remove (fn [{:keys [noop?] :as op}]
                                          (if (h/invoke? op)
                                            (:noop? (h/completion history op))
                                            noop?))))
            _ (doseq [_op history])]  ; TODO: why is it necessary to touch history before use?
                                      ;       otherwise checker/check hangs indefinitely
        (checker/check checker test history opts)))))
