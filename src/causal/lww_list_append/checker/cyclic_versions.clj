(ns causal.lww-list-append.checker.cyclic-versions
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [hiccup.core :as hiccup]
            [clojure.string :as str]))

(defn filter-history
  "Given a set of cyclic versions, `#{[kv]}` and a history,
   returns the first 20 ops that interacted with a [k v] in the cyclic versions."
  [cyclic-versions history-oks]
  (let [ops (->> history-oks
                 (keep (fn [{:keys [value] :as op}]
                         (let [interactions (->> value
                                                 (reduce (fn [interactions [f k v :as mop]]
                                                           (case f
                                                             :append
                                                             (if (contains? cyclic-versions [k v])
                                                               (conj interactions mop)
                                                               interactions)

                                                             :r
                                                             (let [cv-vs (->> cyclic-versions
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
                             (assoc op :value interactions)))))
                 (take 20))]
    ops))

(defn hiccup-structure
  "Given a set of cyclic versions
   and a history pre-filtered and mapped to only contain ops/mops that interacted with the cyclic-versions,
   returns a Hiccup structure."
  [cyclic-versions history-filtered]
  (let [cyclic-versions (into (sorted-set) cyclic-versions)]
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
         [:th {:colspan 3} (str cyclic-versions)]]
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
                               (map (fn [mop] [:p (str mop)])))]])))]]]]))

(defn viz
  "Given a sequence of cyclic versions, `#{[kv]}`, an output directory, and a history,
   outputs an HTML document for each cycle with the first 20 transactions that interacted with a [k v] that was in the cycle"
  [cyclic-versions output-dir history-oks]
  (doseq [versions cyclic-versions]
    (let [versions         (into (sorted-set) versions)
          ops              (->> history-oks
                                (filter-history versions))
          hiccup-structure (hiccup-structure versions ops)
          path             (io/file output-dir
                                    (str (pr-str versions)
                                         ".html"))]

      (io/make-parents path)
      (spit path
            (hiccup/html hiccup-structure)))))
