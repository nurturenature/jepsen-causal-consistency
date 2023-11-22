(ns crdt.util
  (:require [jepsen.history :as h]))

(defn gset-history
  "Generate a sample history of gset ops.
   Optional opts:
   ```
   {:num-processes integer
    :num-elements  integer}
   ```"
  ([] (gset-history {}))
  ([opts]
   (let [num-processes (:num-processes opts 5) 
         num-elements  (:num-elements  opts 1000)]
     (->> num-elements
          (range)
          (mapcat (fn [v]
                 [{:process (rand-int num-processes)
                   :type :ok
                   :f :a
                   :value v}
                  {:process (rand-int num-processes)
                   :type :ok
                   :f :r
                   :value (set (range (+ 1 v)))}]))
          (h/history)))))
