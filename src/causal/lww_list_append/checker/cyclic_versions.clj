(ns causal.lww-list-append.checker.cyclic-versions
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [hiccup.core :as hiccup]
            [clojure.string :as str]))

(defn filter-history
  "Given a set of versions, `#{[kv]}` and a history,
   returns the first 20 ops that interacted with a [k v] in the cyclic versions."
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
                             (assoc op :value interactions)))))
                 (take 20))]
    ops))

(defn hiccup-structure
  "Given sources `#{source}`, the scc `#{[k v]}}`,
   and a history pre-filtered and mapped to only contain ops/mops that interacted with the cyclic-versions,
   returns a Hiccup structure."
  [sources scc history-filtered]
  (let [sources (into (sorted-set) sources)
        scc     (into (sorted-set) scc)]
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
         [:th {:colspan 3} (str scc)]]
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
                               (map (fn [mop] [:p (str mop)])))]])))]]
      [:hr]
      [:p (str "Sources: " sources)]]]))

(defn viz
  "Given a sequence of cyclic versions, `{:sources #{:source} :sccs (#{[kv]})}`, an output directory, and a history,
   outputs an HTML document for each cycle with the first 20 transactions that interacted with a [k v] that was in the cycle"
  [cyclic-versions output-dir history-oks]
  (doseq [{:keys [sources sccs] :as _cyclic-version} cyclic-versions]
    (doseq [scc sccs]
      (let [scc              (into (sorted-set) scc)
            ops              (->> history-oks
                                  (filter-history scc))
            hiccup-structure (hiccup-structure sources scc ops)
            path             (io/file output-dir
                                      (str (pr-str scc)
                                           ".html"))]

        (io/make-parents path)
        (spit path
              (hiccup/html hiccup-structure))))))
