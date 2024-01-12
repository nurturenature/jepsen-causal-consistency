(ns causal.lww-register
  "A test which looks for cycles in write/read transactions.
   Writes are assumed to be unique, but this is the only constraint.
   See jepsen.tests.cycle.wr and elle.rw-register for docs."
  (:refer-clojure :exclude [test])
  (:require [bifurcan-clj
             [core :as b]
             [graph :as bg]
             [set :as bs]]
            [causal.sqlite3 :as sqlite3]
            [cheshire.core :as json]
            [clojure
             [pprint :refer [pprint]]
             [set :as set]
             [string :as str]]
            [clojure.tools.logging :refer [info]]
            [elle
             [consistency-model :as cm]
             [core :as ec]
             [graph :as g]
             [list-append :as la]
             [rels :as rels]
             [rw-register :as rw]
             [txn :as et]
             [util :as eu]]
            [jepsen
             [client :as client]
             [control :as c]
             [history :as h]
             [generator :as gen]
             [txn :as txn]]
            [jepsen.tests.cycle.wr :as wr]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn txn->sql
  "Given a txn of mops, builds an SQL transaction."
  [txn]
  (let [stmts (->> txn
                   (map (fn [[f k v]]
                          (case f
                            :r (str "SELECT k,v FROM lww_registers WHERE k = " k ";")
                            :w (str "INSERT INTO lww_registers(k,v)"
                                    " VALUES(" k "," v ")"
                                    " ON CONFLICT(k) DO UPDATE SET v=" v
                                    " RETURNING k as wk, v as wv;"))))
                   (str/join " "))]
    (str "BEGIN; " stmts " END;")))

(defn mops+sql-result
  "Merges the result of an SQL statement back into the original mops,"
  [mops result]
  (let [[mops' rslts]
        (->> mops
             (reduce (fn [[mops' rslts] [f k v :as mop]]
                       (let [first' (first rslts)
                             rest'  (rest  rslts)
                             k'     (:k first')
                             v'     (:v first')
                             wk'    (:wk first')
                             wv'    (:wv first')
                             type'  (cond
                                      (and wk' wv') :write
                                      (and k' v')   :read
                                      :else         :nil)]
                         (cond
                           (#{:w} f)
                           (do
                             (when (or (not= type' :write)
                                       (not= k wk')
                                       (not= v wv'))
                               (throw+ {:type :sql-result-parse-error :mops mops :result result}))
                             [(conj mops' mop) rest'])

                           (and (#{:r} f)
                                (= type' :write))
                           [(conj mops' [:r k nil]) rslts]

                           (and (#{:r} f)
                                (= type' :read)
                                (= k k'))
                           [(conj mops' [:r k v']) rest']

                           (and (#{:r} f)
                                (= type' :read)
                                (not= k k'))
                           [(conj mops' [:r k nil]) rslts]

                           (and (#{:r} f)
                                (= type' :nil))
                           [(conj mops' [:r k nil]) rslts]

                           :else (throw+ {:type :sql-result-parse-error :mops mops :result result}))))
                     [[] result]))]
    (when (or (not= 0 (count rslts))
              (not= (count mops) (count mops')))
      (throw+ {:type :sql-result-parse-error :mops mops :result result :rslts rslts}))
    mops'))

(defrecord LWWClient [conn]
  client/Client
  (open!
    [this _test node]
    (assoc this
           :node node
           :url  (str "http://" node)))

  (setup!
    [_this _test])

  (invoke!
    [{:keys [node] :as _this} test {:keys [f value] :as op}]
    (let [op (assoc op :node node)]
      (assert (= f :txn) "Ops must be txns.")
      (try+ (let [sql-stmt (txn->sql value)
                  result   (get (c/on-nodes test [node]
                                            (fn [_test _node]
                                              (c/exec :echo sql-stmt :| :sqlite3 :-json sqlite3/database-file)))
                                node)
                  result   (->> result
                                (str/split-lines)
                                (mapcat #(json/parse-string % true))
                                vec)
                  mops'    (mops+sql-result value result)]
              (assoc op
                     :type  :ok
                     :value mops'))
            (catch [:type :jepsen.control/nonzero-exit
                    :exit 1
                    :err "Runtime error near line 1: database is locked (5)\n"] {}
              (assoc op
                     :type  :fail
                     :error :database-locked)))))

  (teardown!
    [_this _test])

  (close!
    [this _test]
    (dissoc this
            :node
            :url)))

(def causal-opts
  "Opts to configure Elle for causal consistency."
  ; rw_register provides:
  ;   - initial nil -> all versions for all keys
  ;   - w->r
  ;   - ww and rw dependencies, as derived from a version order
  {:consistency-models [:strong-session-consistent-view] ; Elle's strong-session with Adya's formalism for causal consistency
   :anomalies [:internal]                                ; basic hygiene
   :anomalies-ignored [:lost-update]                     ; `lost-update`s are causally Ok, they are PL-2+, Adya 4.1.3
   :sequential-keys? true                                ; infer version order from elle/process-graph
   :wfr-keys? true                                       ; wfr-version-graph when <rw within txns
   :wfr-txns? true                                       ; wfr-txn-graph used to infer version order
   })

(defn workload
  "Last write wins register workload.
   `opts` are merged with `causal-opts` to configure `checker`."
  [{:keys [rate] :as opts}]
  (let [opts (merge
              {:directory      "."
               :max-plot-bytes 1048576
               :plot-timeout   10000}
              causal-opts
              opts)
        wr-test (wr/test opts)
        final-r (->> (range 100)
                     (reduce (fn [mops k] (conj mops [:r k nil])) []))
        final-gen (gen/phases
                   (gen/log "Quiesce...")
                   (gen/sleep 5)
                   (gen/log "Final reads...")
                   (->> (gen/once {:type :invoke :f :txn :value final-r :final-read? true})
                        (gen/each-thread)
                        (gen/clients)
                        (gen/stagger (/ rate))))]
    (merge
     wr-test
     {:client (LWWClient. nil)
      :final-generator final-gen})))

(defn op
  "Generates an operation from a string language like so:

  wx1       set x = 1
  ry1       read y = 1
  wx1wx2    set x=1, x=2"
  ([string]
   (let [[txn mop] (reduce (fn [[txn [f k _v :as mop]] c]
                             (case c
                               \w [(conj txn mop) [:w]]
                               \r [(conj txn mop) [:r]]
                               \x [txn (conj mop :x)]
                               \y [txn (conj mop :y)]
                               \z [txn (conj mop :z)]
                               (let [e (if (= \_ c)
                                         nil
                                         (Long/parseLong (str c)))]
                                 [txn [f k e]])))
                           [[] nil]
                           string)
         txn (-> txn
                 (subvec 1)
                 (conj mop))]
     {:process 0, :type :ok, :f :txn :value txn}))
  ([process type string]
   (assoc (op string) :process process :type type)))

(defn fail
  "Fails an op."
  [op]
  (assoc op :type :fail))

(defn invoke
  "Takes a completed op and returns an invocation."
  [completion]
  (-> completion
      (assoc :type :invoke)
      (update :value (partial map (fn [[f k _v :as mop]]
                                    (if (= :r f)
                                      [f k nil]
                                      mop))))))

(defn op-pair
  ([txn] (op-pair 0 txn))
  ([p txn]
   (let [op     (op p :ok txn)
         invoke (invoke op)]
     [invoke op])))

(def causal-ok
  (->> [[0 "wx0"]
        [1 "rx0wy1"]
        [2 "ry1rx0"]]
       (mapcat #(apply op-pair %))
       h/history))

(def causal-2-mops-anomaly
  (->> [[0 "wx0"]
        [1 "rx0wy1"]
        [2 "ry1rx_"]]
       (mapcat #(apply op-pair %))
       h/history))

(def ryw-ok
  (->> [[0 "wx0"]
        [0 "rx0"]]
       (mapcat #(apply op-pair %))
       h/history))

(def ryw-anomaly
  (->> [[0 "wx0"]
        [0 "rx_"]]
       (mapcat #(apply op-pair %))
       h/history))

(def monotonic-writes-ok
  (->> [[0 "wx0"]
        [0 "wx1"]
        [1 "rx0"]
        [1 "rx1"]]
       (mapcat #(apply op-pair %))
       h/history))

(def monotonic-writes-anomaly
  (->> [[0 "wx0"]
        [0 "wx1"]
        [1 "rx1"]
        [1 "rx0"]]
       (mapcat #(apply op-pair %))
       h/history))

(def monotonic-writes-diff-key-ok
  (->> [[0 "wx0"]
        [0 "wx1"]
        [0 "wy2"]
        [1 "ry2"]
        [1 "rx1"]]
       (mapcat #(apply op-pair %))
       h/history))

(def monotonic-writes-diff-key-anomaly
  (->> [[0 "wx0"]
        [0 "wy1"]
        [1 "ry1"]
        [1 "rx_"]]
       (mapcat #(apply op-pair %))
       h/history))

(def wfr-ok
  (->> [[0 "wx0"]
        [1 "rx0"]
        [1 "wy1"]
        [2 "ry1rx0"]]
       (mapcat #(apply op-pair %))
       h/history))

(def wfr-1-mop-anomaly
  (->> [[0 "wx0"]
        [1 "rx0"]
        [1 "wy1"]
        [2 "ry1"]
        [2 "rx_"]]
       (mapcat #(apply op-pair %))
       h/history))

(def wfr-2-mop-anomaly
  (->> [[0 "wx0"]
        [1 "rx0"]
        [1 "wy1"]
        [2 "rx_ry1"]]
       (mapcat #(apply op-pair %))
       h/history))

(def wfr-anomaly
  (->> [[0 "wx0"]
        [1 "rx0wy1"]
        [2 "ry1rx_"]]
       (mapcat #(apply op-pair %))
       h/history))

(def internal-ok
  (->> [[0 "wx0"]
        [0 "wx1wx2rx2"]]
       (mapcat #(apply op-pair %))
       h/history))

(def internal-anomaly
  (->> [[0 "wx0"]
        [0 "wx1wx2rx1"]]
       (mapcat #(apply op-pair %))
       h/history))

(def lww-ok
  (let [[p0-wx0 p0-wx0'] (op-pair 0 "wx0")
        [p1-wx1 p1-wx1'] (op-pair 1 "wx1")
        [p0-rx0 p0-rx0'] (op-pair 0 "rx0")
        [p1-rx1 p1-rx1'] (op-pair 1 "rx1")]
    (->> [p0-wx0
          p1-wx1
          p0-wx0'
          p1-wx1'
          p0-rx0
          p0-rx0'
          p1-rx1
          p1-rx1']
         h/history)))

(def lww-anomaly
  (let [[p0-wx0 p0-wx0'] (op-pair 0 "wx0")
        [p1-wx1 p1-wx1'] (op-pair 1 "wx1")
        [p0-rx0 p0-rx0'] (op-pair 0 "rx0")
        [p0-rx1 p0-rx1'] (op-pair 0 "rx1")
        [p1-rx0 p1-rx0'] (op-pair 1 "rx0")
        [p1-rx1 p1-rx1'] (op-pair 1 "rx1")]
    (->> [p0-wx0
          p1-wx1
          p0-wx0'
          p1-wx1'
          p0-rx1
          p0-rx1'
          p1-rx0
          p1-rx0']
         h/history)))

(def not-cycle-lost-update
  (->> [[0 "wx0"]
        [1 "rx0wx1"]
        [2 "rx0wx2"]
        [3 "rx0"]
        [3 "rx1"]
        [3 "rx2"]]
       (mapcat #(apply op-pair %))
       h/history))

(def g-monotonic-anomaly
  "Adya Weak Consistency 4.2.2
   Hnon2L : w1 (x 1) w1 (y 1) c1 w2 (y 2) w2 (x 2) w2 (z 2) r3 (x 2) w3 (z 3) r3 (y 1) c2 c3
   [x 1 << x 2 , y 1 << y 2 , z 2 << z 3]"
  (->> [{:process 1 :type :invoke :value [[:w :x 1] [:w :y 1]] :f :txn}
        {:process 1 :type :ok     :value [[:w :x 1] [:w :y 1]] :f :txn}
        {:process 2 :type :invoke :value [[:w :y 2] [:w :x 2] [:w :z 2]] :f :txn}
        {:process 3 :type :invoke :value [[:r :x nil] [:w :z 3] [:r :y nil]] :f :txn}
        {:process 2 :type :ok     :value [[:w :y 2] [:w :x 2] [:w :z 2]] :f :txn}
        {:process 3 :type :ok     :value [[:r :x 2] [:w :z 3] [:r :y 1]] :f :txn}]
       h/history))

(def g-monotonic-list-append-anomaly
  (->> [{:process 1 :type :invoke :value [[:append :x 1] [:append :y 1]] :f :txn}
        {:process 1 :type :ok     :value [[:append :x 1] [:append :y 1]] :f :txn}
        {:process 2 :type :invoke :value [[:append :y 2] [:append :x 2] [:append :z 2]] :f :txn}
        {:process 3 :type :invoke :value [[:r :x nil] [:append :z 3] [:r :y nil]] :f :txn}
        {:process 2 :type :ok     :value [[:append :y 2] [:append :x 2] [:append :z 2]] :f :txn}
        {:process 3 :type :ok     :value [[:r :x [1,2]] [:append :z 3] [:r :y [1]]] :f :txn}]
       h/history))
