(ns causal.gset.checker.strong-convergence
  (:require [clojure.set :as set]
            [elle.rw-register :as rw]
            [jepsen
             [checker :as checker]
             [history :as h]
             [txn :as txn]]))

(defn r-mop->kv-set
  "Given a [:r k #{v v'...}] mop,
   return #{[k v] [k v']...}"
  [[f k v :as mop]]
  (assert (= f :r) (str "mop is not a read: " mop))
  (->> v
       (map (fn [v'] [k v']))
       (into (sorted-set))))

(defn kv-set->map
  [kv-set]
  (->> kv-set
       (reduce (fn [acc [k v]]
                 (update acc k (fn [old]
                                 (if (nil? old)
                                   (sorted-set v)
                                   (conj old v)))))
               (sorted-map))))

(defn final-reads
  "Do `:final-read? true` reads strongly converge?
   Check:
     - final read from all nodes
     - final read contains all ok writes and all info writes that were read
     - all read values were written"
  []
  (reify checker/Checker
    (check [_this {:keys [nodes] :as _test} history _opts]
      (let [nodes           (->> nodes (into (sorted-set)))
            history         (->> history
                                 h/client-ops)
            ext-write-index (rw/ext-index txn/ext-writes history)
            history'        (->> history
                                 h/oks
                                 (h/filter :final-read?))
            node->final     (->> history'
                                 (reduce (fn [acc {:keys [node value] :as _op}]
                                           (->> value
                                                (reduce (fn [acc mop]
                                                          (update acc node (fn [old]
                                                                             (if (nil? old)
                                                                               ; make initial value a sorted-set
                                                                               (r-mop->kv-set mop)
                                                                               (set/union old (r-mop->kv-set mop))))))
                                                        acc)))
                                         (sorted-map)))
            {:keys [ok-r
                    ok-w
                    info-w]} (->> history
                                  (txn/reduce-mops
                                   (fn [state {:keys [type] :as _op} [f k v :as mop]]
                                     (case [type f]
                                       ([:invoke :r]
                                        [:invoke :w]
                                        [:info :r]
                                        [:fail :r]
                                        [:fail :w])
                                       state

                                       [:ok :r]
                                       (update state :ok-r set/union (r-mop->kv-set mop))

                                       [:ok :w]
                                       (update state :ok-w conj [k v])

                                       [:info :w]
                                       (update state :info-w conj [k v])))
                                   {:ok-r   (sorted-set)
                                    :ok-w   (sorted-set)
                                    :info-w (sorted-set)}))
            expected-final (set/union ok-w (set/intersection info-w ok-r))]
        (merge
         {:valid? true
          :expected-read-count (count expected-final)}

         ; final read from all nodes?
         (let [nodes-with-final-reads    (set (keys node->final))
               nodes-missing-final-reads (set/difference nodes nodes-with-final-reads)]
           (when (seq nodes-missing-final-reads)
             {:valid? false
              :nodes nodes
              :nodes-with-final-reads nodes-with-final-reads
              :nodes-missing-final-reads nodes-missing-final-reads}))

         ; final read has all expected values?
         (let [incomplete-final-reads (->> node->final
                                           (map (fn [[node read]]
                                                  (let [missing  (set/difference expected-final read)
                                                        missing' (->> missing
                                                                      kv-set->map
                                                                      (map (fn [[k vs]]
                                                                             (let [vs (->> vs
                                                                                           (reduce (fn [acc v]
                                                                                                     (assoc acc v
                                                                                                            (->> (get-in ext-write-index [k v])
                                                                                                                 first
                                                                                                                 :node)))
                                                                                                   (sorted-map)))]
                                                                               [k vs])))
                                                                      (into (sorted-map)))]
                                                    (when (seq missing)
                                                      [node {:missing-count (count missing)
                                                             :missing       missing'}]))))
                                           (into (sorted-map)))]
           (when (seq incomplete-final-reads)
             {:valid? false
              :incomplete-final-reads incomplete-final-reads}))

         ; final read has unexpected values?
         (let [unexpected-final-reads (->> node->final
                                           (map (fn [[node read]]
                                                  (let [unexpected (set/difference read expected-final)]
                                                    (when (seq unexpected)
                                                      [node {:unexpected-count (count unexpected)
                                                             :unexpected       (kv-set->map unexpected)}]))))
                                           (into (sorted-map)))]
           (when (seq unexpected-final-reads)
             {:valid? false
              :unexpected-final-reads unexpected-final-reads})))))))
