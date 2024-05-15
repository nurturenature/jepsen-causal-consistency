(ns causal.lww-list-append.checker.cyclic-versions
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [hiccup.core :as hiccup]
            [clojure.string :as str]))

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

(defn hiccup-structure
  "Given the table head and footer and a history returns a Hiccup structure."
  [thead tfoot history-filtered]
  [:html
   [:head
    [:style (->> ["table { border-collapse: collapse; border: 1px solid black; }"
                  "th { text-align: center; }"
                  "th, td { padding: 10px; }"
                  "thead, th, tbody, tr, td { border-collapse: collapse; border: 1px solid black; }"]
                 (str/join "\n"))]]
   [:body
    [:table
     [:thead
      [:tr
       [:th {:colspan 3} thead]]
      [:tr
       [:th "Index"]
       [:th "Node"]
       [:th "Mops"]]]
     [:tbody
      (->> history-filtered
           (map (fn [{:keys [index node value] :as _op}]
                  [:tr
                   [:td index]
                   [:td node]
                   [:td (->> value
                             (map (fn [mop] [:p (str mop)])))]])))]
     [:tfoot
      [:tr
       [:th {:colspan 3} tfoot]]]]]])

(defn viz-cycles
  "Given a sequence of cyclic versions, `{:sources #{:source} :sccs (#{[kv]})}`, an output directory, and a history,
   outputs an HTML document for each cycle with the transactions that interacted with a [k v] that was in the cycle"
  [cyclic-versions output-dir history-oks]
  (doseq [{:keys [sources sccs] :as _cyclic-version} cyclic-versions]
    (doseq [scc sccs]
      (let [scc              (into (sorted-set) scc)
            sources          (into (sorted-set) sources)
            description      (str scc)
            description      (if (< (.length description) 32)
                               description
                               (str (subs description 0 32) "..."))
            ops              (->> history-oks
                                  (filter-by-kv scc))
            hiccup-structure (hiccup-structure (str scc) (str "Sources: " sources) ops)
            path             (io/file output-dir
                                      (str description ".html"))]

        (io/make-parents path)
        (spit path
              (hiccup/html hiccup-structure))))))

(defn viz-keys
  "Given a set of keys, `#{k}`, an output directory, and a history,
   creates a separate HTML document for each individual key with its full history."
  [viz-keys output-dir history-oks]
  (doseq [viz-key viz-keys]
    (let [thead            (str "Key: " viz-key)
          tfoot            (str viz-key)
          ops              (->> history-oks
                                (filter-by-k #{viz-key}))
          hiccup-structure (hiccup-structure thead tfoot ops)
          path             (io/file output-dir
                                    (str (pr-str viz-key)
                                         ".html"))]

      (io/make-parents path)
      (spit path
            (hiccup/html hiccup-structure)))))

(defn viz
  "Given a sequence of cyclic versions, `{:sources #{:source} :sccs (#{[kv]})}`, an output directory, and a history,
   outputs an HTML document for each cycle with the transactions that interacted with a [k v] that was in the cycle.
   Creates a separate HTML document for each individual key with its full history."
  [cyclic-versions output-dir history-oks]
  (let [all-ks (->> cyclic-versions
                    (mapcat :sccs)
                    (apply set/union)
                    (map (fn [[k _v]] k))
                    (into #{}))]
    (viz-cycles cyclic-versions output-dir history-oks)
    (viz-keys   all-ks          output-dir history-oks)))
