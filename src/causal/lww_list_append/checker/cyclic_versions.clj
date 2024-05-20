(ns causal.lww-list-append.checker.cyclic-versions
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as str]
            [hiccup.core :as hiccup]
            [jepsen.history :as h]))

(defn filter-by-k
  "Given a set of keys, `#{k}`, and a history,
   returns a history filtered to ops that interacted with the keys, 
   with each transaction filtered to mops that interacted with the keys."
  [filter-keys history-oks]
  (->> history-oks
       (keep (fn [{:keys [value] :as op}]
               (let [interactions (->> value
                                       (reduce (fn [interactions [_f k _v :as mop]]
                                                 (if (contains? filter-keys k)
                                                   (conj interactions mop)
                                                   interactions))
                                               []))]
                 (when (seq interactions)
                   (assoc op :value interactions)))))))

(defn filter-by-kv
  "Given a set of versions, `#{[kv]}` and a history,
   returns the transactions that interacted with a [k v] in the cyclic versions."
  [versions history-oks]
  (let [ops (->> history-oks
                 (keep (fn [{:keys [value] :as op}]
                         (let [interactions (->> value
                                                 (reduce (fn [interactions [f k v :as mop]]
                                                           (case f
                                                             :append
                                                             (if (contains? versions [k v])
                                                               (conj interactions mop)
                                                               interactions)

                                                             :r
                                                             (let [cv-vs (->> versions
                                                                              (keep (fn [[cv-k cv-v]]
                                                                                      (when (= k cv-k)
                                                                                        cv-v)))
                                                                              (into #{}))]
                                                               (if (seq cv-vs)
                                                                 (let [r-vs (into #{} v)]
                                                                   (if (seq (set/intersection r-vs cv-vs))
                                                                     (conj interactions mop)
                                                                     interactions))
                                                                 interactions))))
                                                         []))]
                           (when (seq interactions)
                             (assoc op :value interactions))))))]
    ops))

(defn complete-history
  "Given a history of completions, e.g. :ok's, and a full history,
   returns a history that includes :invoke's in :index order"
  [completions history-full]
  (->> completions
       (mapcat (fn [op]
                 [(h/invocation history-full op) op]))
       (sort-by :index)
       h/history))

(defn concurrent-history-filter
  "Given a seq of ops,
   return a sequence of ops where concurrent ops have invocation and completion,
   and the reset just completions."
  [ops]
  (let [filtered
        (loop [ops      ops
               nesting  0
               filtered nil]
          (let [op      (first ops)
                next-op (fnext ops)]
            ; done?
            (if (nil? op)
              filtered
              (cond
                ; last op
                (nil? next-op)
                (conj filtered op)

                ; not an invoke
                (not= :invoke (:type op))
                (recur (next ops) (dec nesting) (conj filtered op))

                ; an invoke whose completion is next op
                (= (:process op)
                   (:process next-op))
                (recur (next ops) (inc nesting) (if (= 0 nesting)
                                                  filtered
                                                  (conj filtered op)))

                ; concurrent invoke
                :else
                (recur (next ops) (inc nesting) (conj filtered op))))))]
    (reverse filtered)))

(defn hiccup-structure
  "Given the table head and footer and a history returns a Hiccup structure."
  [thead tfoot history-filtered]
  [:html
   [:head
    [:style (->> ["table { border-collapse: collapse; border: 1px solid black; }"
                  "th { text-align: center; }"
                  "th, td { padding: 10px; }"
                  "thead, th, tbody, tr, td { border-collapse: collapse; border: 1px solid black; }"
                  ".centered { text-align: center; }"]
                 (str/join "\n"))]]
   [:body
    [:table
     [:thead
      [:tr
       [:th {:colspan 4} thead]]
      [:tr
       [:th "Index"]
       [:th "Process"]
       [:th "Type"]
       [:th "Mops"]]]
     [:tbody
      (->> history-filtered
           (map (fn [{:keys [index process type value] :as _op}]
                  [:tr
                   [:td {:class "centered"} index]
                   [:td {:class "centered"} process]
                   [:td {:class "centered"} type]
                   [:td {:class "centered"} (->> value
                                                 (mapcat (fn [mop]
                                                           [(str mop) [:br]]))
                                                 butlast)]])))]
     [:tfoot
      [:tr
       [:th {:colspan 4} tfoot]]]]]])

(defn viz-cycles
  "Given a sequence of cyclic versions, `{:sources #{:source} :sccs (#{[kv]})}`, an output directory, and a full history,
   outputs an HTML document for each cycle with the transactions that interacted with a [k v] that was in the cycle"
  [cyclic-versions output-dir history-full]
  (doseq [{:keys [sources sccs] :as _cyclic-version} cyclic-versions]
    (doseq [scc sccs]
      (let [scc              (into (sorted-set) scc)
            sources          (into (sorted-set) sources)
            description      (str scc)
            description      (if (< (.length description) 32)
                               description
                               (str (subs description 0 32) "..."))
            ops              (->> history-full
                                  h/oks
                                  (filter-by-kv scc))
            ops              (->> (complete-history ops history-full)
                                  concurrent-history-filter)
            hiccup-structure (hiccup-structure (str scc) (str "Sources: " sources) ops)
            path             (io/file output-dir
                                      (str description ".html"))]

        (io/make-parents path)
        (spit path
              (hiccup/html hiccup-structure))))))

(defn viz-keys
  "Given a set of keys, `#{k}`, an output directory, and a full history,
   creates a separate HTML document for each individual key with its full history."
  [viz-keys output-dir history-full]
  (doseq [viz-key viz-keys]
    (let [thead            (str "Key: " viz-key)
          tfoot            (str viz-key)
          ops              (->> history-full
                                h/oks
                                (filter-by-k #{viz-key}))
          ops              (->> (complete-history ops history-full)
                                concurrent-history-filter)
          hiccup-structure (hiccup-structure thead tfoot ops)
          path             (io/file output-dir
                                    (str (pr-str viz-key)
                                         ".html"))]

      (io/make-parents path)
      (spit path
            (hiccup/html hiccup-structure)))))

(defn viz
  "Given a sequence of cyclic versions, `{:sources #{:source} :sccs (#{[kv]})}`, an output directory, and a full history,
   outputs an HTML document for each cycle with the transactions that interacted with a [k v] that was in the cycle.
   Creates a separate HTML document for each individual key with its full history."
  [cyclic-versions output-dir history-full]
  (let [all-ks (->> cyclic-versions
                    (mapcat :sccs)
                    (apply set/union)
                    (map (fn [[k _v]] k))
                    (into #{}))]
    (viz-cycles cyclic-versions output-dir history-full)
    (viz-keys   all-ks          output-dir history-full)))
