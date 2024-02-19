(ns causal.gset
  "A test which looks for cycles in write/read transactions.
   Writes are assumed to be unique, but this is the only constraint.
   See jepsen.tests.cycle.wr and elle.rw-register for docs."
  (:refer-clojure :exclude [test])
  (:require [bifurcan-clj
             [core :as b]
             [graph :as bg]
             [set :as bs]]
            [causal.checker
             [strong-convergence :as sc]]
            [causal.db
             [sqlite3 :as sqlite3]]
            [cheshire.core :as json]
            [clj-http.client :as http]
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
             [checker :as checker]
             [client :as client]
             [control :as c]
             [history :as h]
             [generator :as gen]
             [txn :as txn]
             [util :as u]]
            [jepsen.tests.cycle.wr :as wr]
            [next.jdbc :as jdbc]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn txn->electric-findMany
  "Given a transaction, returns the JSON necessary to use ElectricSQL findMany.
   Transaction is assumed to be all read mops."
  [txn]
  (let [ks (->> txn
                (mapv (fn [[f k v :as mop]]
                        (assert (= :r f)  (str "findMany is read only: " mop))
                        (assert (= nil v) (str "malformed read: " mop))
                        k)))]
    (->> {:where   {:k {:in ks}}
          :select  {:k true
                    :v true}
          :orderBy [{:k :asc}
                    {:v :asc}]}
         json/generate-string)))

(defn electric-findMany->txn
  "Given the original transaction and the result of an ElectricSQL findMany,
   returns the transaction with the result merged."
  [txn result]
  (let [result (->> (json/parse-string result true)
                    (map (fn [{:keys [k v]}]
                           [k v]))
                    (into #{})
                    sc/kv-set->map)]
    (->> txn
         (mapv (fn [[f k v :as mop]]
                 (assert (= :r f)  (str "findMany is read only: " mop))
                 (assert (= nil v) (str "malformed read: " mop))
                 [:r k (get result k)])))))

(defn txn->electric-createMany
  "Given a transaction, returns the JSON necessary to use ElectricSQL createMany.
   Transaction is assumed to be all writes."
  [txn]
  (let [records (->> txn
                     (mapv (fn [[f k v :as _mop]]
                             (assert (= :w f) (str "mop is not a write in " txn))
                             {:id (->> k (* 10000) (+ v))
                              :k  k
                              :v  v})))]

    (->> {:data records}
         json/generate-string)))

(defn electric-createMany->txn
  "Given the original transaction and the ElectricSQL createMany result,
   return the transaction with the results merged in."
  [txn result]
  (let [result (json/parse-string result true)]
    (assert (= (:count result) (count txn))
            (str "result count mismatch: " result " for " txn)))
  txn)

(defrecord ElectricSQLClient [conn]
  client/Client
  (open!
    [this _test node]
    (assoc this
           :node node
           :url  (str "http://" node ":8089")))

  (setup!
    [_this _test])

  (invoke!
    [{:keys [node url] :as _this} _test {:keys [f value] :as op}]
    (let [op (assoc op :node node)]
      (try+ (let [[r-or-w _ _] (first value)
                  [url body]   (case r-or-w
                                 :r [(str url "/electric-findMany")
                                     (txn->electric-findMany value)]
                                 :w [(str url "/electric-createMany")
                                     (txn->electric-createMany value)])
                  result (http/post url
                                    {:body               body
                                     :content-type       :json
                                     :socket-timeout     1000
                                     :connection-timeout 1000
                                     :accept             :json})
                  result (:body result)
                  result (case r-or-w
                           :r (electric-findMany->txn value result)
                           :w (electric-createMany->txn   value result))]
              (assoc op
                     :type  :ok
                     :value result))
            (catch (and (instance? java.net.ConnectException %)
                        (re-find #"Connection refused" (.getMessage %)))
                   {}
              (assoc op
                     :type  :fail
                     :error :connection-refused))
            (catch (or (instance? java.net.SocketException %)
                       (instance? org.apache.http.NoHttpResponseException %))
                   {:keys [cause]}
              (assoc op
                     :type  :info
                     :error cause)))))

  (teardown!
    [_this _test])

  (close!
    [this _test]
    (dissoc this
            :node
            :url)))

(defn op->better-sqlite3
  "Given an op, return the JSON for a better-sqlite3 transaction."
  [{:keys [value] :as _op}]
  (let [value (->> value
                   (map (fn [[f k v]]
                          (case f
                            :r {"f" f "k" k "v" v}
                            :w {"f" f "k" k "v" v "id" (+ (* k 10000)
                                                          v)})))
                   (into []))
        op     {:type  :invoke
                :value value}]
    (->> op
         json/generate-string)))

(defn better-sqlite3->op
  "Given the original op and a better-sqlite3 JSON result,
   return the op updated with better-sqlite3 results."
  [{:keys [value] :as op} {:keys [status body] :as rslt}]
  (let [_                     (assert (= status 200))
        rslt                  (json/parse-string body true)
        [type' value' error'] [(keyword (:type rslt)) (:value rslt) (:error rslt)]
        _                     (assert (= (count value)
                                         (count value')))
        value' (->> value'
                    (map (fn [[f k v] mop]
                           (let [[f' k' v'] [(keyword (:f mop)) (:k mop) (:v mop)]]
                             (assert (and (= f f')
                                          (= k k'))
                                     (str "Original op: " op ", result: " rslt ", mismatch"))
                             (case f
                               :r (if (nil? v')
                                    [:r k nil]
                                    (let [v' (->> v'
                                                  (map (fn [{:keys [k v]}]
                                                         (assert (= k k')
                                                                 (str ":r for k contain non-k results: " op ", " rslt))
                                                         v))
                                                  (into (sorted-set)))]
                                      [:r k v']))
                               :w (do
                                    (assert (= v v')
                                            (str "Munged write value in result, expected " v ", actual " v'))
                                    [f k v]))))
                         value)
                    (into []))]
    (case type'
      :ok
      (assoc op
             :type  :ok
             :value value')

      :fail
      (assoc op
             :type  :fail
             :error error')

      :info
      (assoc op
             :type  :info
             :error error'))))

(defrecord BetterSQLite3Client [conn]
  client/Client
  (open!
    [this _test node]
    (assoc this
           :node node
           :url  (str "http://" node ":8089/better-sqlite3")))

  (setup!
    [_this _test])

  (invoke!
    [{:keys [node url] :as _this} test {:keys [f value] :as op}]
    (let [op   (assoc op :node node)]
      (try+ (let [body (op->better-sqlite3 op)
                  rslt (http/post url
                                  {:body               body
                                   :content-type       :json
                                   :socket-timeout     1000
                                   :connection-timeout 1000
                                   :accept             :json})]
              (better-sqlite3->op op rslt))
            (catch (and (instance? java.net.ConnectException %)
                        (re-find #"Connection refused" (.getMessage %)))
                   {}
              (assoc op
                     :type  :fail
                     :error :connection-refused))
            (catch (or (instance? java.net.SocketException %)
                       (instance? org.apache.http.NoHttpResponseException %))
                   {:keys [cause]}
              (assoc op
                     :type  :info
                     :error cause)))))

  (teardown!
    [_this _test])

  (close!
    [this _test]
    (dissoc this
            :node
            :url)))

(defn txn->sqlite3-cli
  "Given a txn of mops, builds an SQLite3 transaction."
  [txn]
  (let [stmts (->> txn
                   (map (fn [[f k v]]
                          (case f
                            :r (str "SELECT k,v FROM gset WHERE k = " k ";")
                            :w (str "INSERT INTO gset (id,k,v)"
                                    " VALUES(" (->> k (* 10000) (+ v)) "," k "," v ")"
                                    " RETURNING k as wk, v as wv;"))))
                   (str/join " "))]
    (str "BEGIN; " stmts " END;")))

(defn sqlite3-cli->txn
  "Merges the result of an SQLite3 statement back into the original txn."
  [txn result]
  (let [result (-> (str "[" result "]")
                   (str/replace #"\]\n\[" "],\n[")
                   (json/parse-string true))
        [txn' result']
        (->> txn
             (reduce (fn [[txn' result'] [f k v :as mop]]
                       (let [stmt-rslt (first result')
                             rest-rslt (rest  result')
                             {rk :k rv :v wk :wk wv :wv} (first stmt-rslt)]
                         (cond
                           ; mop is :w, statement result is the 1 record written
                           (and (#{:w} f)
                                (= 1 (count stmt-rslt))
                                (= k wk)
                                (= v wv))
                           [(conj txn' mop) rest-rslt]

                           ; mop is :r, no more statement results, null read
                           (and (#{:r} f)
                                (nil? result'))
                           [(conj txn' [:r k nil]), nil]

                           ; mop is :r, statement result is a :w result [{:wk :wv}], null read
                           (and (#{:r} f)
                                (= 1 (count stmt-rslt))
                                wk wv)
                           [(conj txn' [:r k nil]), result']

                           ; mop is :r, statement result is a :r result [{:k :v} ...] for a different key, null read
                           (and (#{:r} f)
                                (not= k rk))
                           [(conj txn' [:r k nil]), result']

                           ; mop is :r, statement result is a :r result [{:k :v} ...] for the same key
                           (and (#{:r} f)
                                (= k rk))
                           (let [vs (->> stmt-rslt
                                         (map :v)
                                         (into (sorted-set)))]
                             [(conj txn' [:r k vs]) rest-rslt])

                           :else
                           (throw+ {:type :sql-result-parse-error
                                    :mops txn :result result
                                    :mop mop :stmt-rslt stmt-rslt
                                    :k k :v v :rk rk :rv rv :wk wk :wv wv}))))
                     [[] result]))]
    (when (or (not= 0 (count result'))
              (not= (count txn) (count txn')))
      (throw+ {:type :sql-result-parse-error :mops txn :result result :rslts result'}))
    txn'))

(defrecord SQLite3CliClient [conn]
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
      (try+ (let [sql-stmt (txn->sqlite3-cli value)
                  result   (get (c/on-nodes test [node]
                                            (fn [_test _node]
                                              (c/exec :echo sql-stmt :| :sqlite3 :-json sqlite3/database-file)))
                                node)
                  mops'    (sqlite3-cli->txn value result)]
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

(defn get-jdbc-connection
  "Tries to get a `jdbc` connection for a total of ms, default 5000, every 1000ms.
   Throws if no client can be gotten."
  ([db-spec] (get-jdbc-connection db-spec 5000))
  ([db-spec ms]
   (let [conn (u/timeout ms nil (u/retry 1 (->> db-spec
                                                jdbc/get-datasource
                                                jdbc/get-connection)))]
     (when (nil? conn)
       (throw+ {:connection-failure db-spec}))
     conn)))

(defrecord PostgreSQLJDBCClient [db-spec]
  client/Client
  (open!
    [this _test node]
    (assoc this
           :node node
           :conn (get-jdbc-connection db-spec)))

  (setup!
    [_this _test])

  (invoke!
    [{:keys [conn node] :as _this} _test {:keys [value] :as op}]
    (let [op (assoc op
                    :node node)]
      (try+
       (let [mops' (jdbc/with-transaction
                     [tx conn]
                     (->> value
                          (map (fn [[f k v :as mop]]
                                 (case f
                                   :r [:r k (->> (jdbc/execute! tx [(str "SELECT k,v FROM gset WHERE k = " k)])
                                                 (map :gset/v)
                                                 (into (sorted-set)))]
                                   :w (do
                                        (assert (= 1
                                                   (let [id (->> k (* 10000) (+ v))]
                                                     (->> (jdbc/execute! tx [(str "INSERT INTO gset (id,k,v) VALUES (" id "," k "," v ")")])
                                                          first
                                                          :next.jdbc/update-count))))
                                        mop))))
                          (into [])))]
         (assoc op
                :type  :ok
                :value mops'))
       (catch (fn [e]
                (if (and (instance? org.postgresql.util.PSQLException e)
                         (re-find #"ERROR\: deadlock detected\n.*" (.getMessage e)))
                  true
                  false)) {}
         (assoc op
                :type  :fail
                :error :deadlock)))))

  (teardown!
    [_this _test])

  (close!
    [{:keys [conn] :as _client} _test]
    (.close conn)))

(def db-specs
  "Map of node names to db-specs."
  {"postgresql"  {:dbtype   "postgresql"
                  :host     "postgresql"
                  :user     "postgres"
                  :password "postgres"}
   "electricsql" {:dbtype   "postgresql"
                  :host     "electricsql"
                  :port     65432
                  :user     "postgres"
                  :password "postgres"
                  :dbname   "postgres"}})

;; TODO: why isn't electricsql allowing a connection using jdbc?
;;       just connecting to postgresql for now
(defn node->client
  "Maps a node name to its `client` protocol.
   BetterSQLite3Client is default."
  [{:keys [better-sqlite3-nodes electricsql-nodes postgresql-nodes sqlite3-cli-nodes] :as _test} node]
  (cond
    (contains? electricsql-nodes node)
    (ElectricSQLClient. nil)

    (contains? better-sqlite3-nodes node)
    (BetterSQLite3Client. nil)

    (contains? sqlite3-cli-nodes node)
    (SQLite3CliClient. nil)

    (contains? postgresql-nodes node)
    (PostgreSQLJDBCClient. (get db-specs "postgresql"))

    (= "electricsql" node)
    ; TODO: electricsql pg proxy doesn't play well with jdbc
    (PostgreSQLJDBCClient. (get db-specs "postgresql"))

    :else
    (BetterSQLite3Client. nil)))

(defrecord GSetClient [conn]
  client/Client
  (open!
    [_this test node]
    (client/open! (node->client test node) test node)))

(def causal-opts
  "Opts to configure Elle for causal consistency."
  ; rw_register provides:
  ;   - initial nil -> all versions for all keys
  ;   - w->r
  ;   - ww and rw dependencies, as derived from a version order
  {:consistency-models [:strong-session-consistent-view] ; Elle's strong-session with Adya's formalism for causal consistency
   :anomalies-ignored [:lost-update]                     ; `lost-update`s are causally Ok, they are PL-2+, Adya 4.1.3
   :sequential-keys? true                                ; infer version order from elle/process-graph
   ;:linearizable-keys? true                             ; TODO: should be LWW?
   :wfr-keys? true                                       ; wfr-version-graph when <rw within txns
   ;:wfr-process? true TODO: valid? explainer?           ; wfr-process-graph used to infer version order
   ;:additional-graphs [rw/wfr-ww-transaction-graph] TODO: valid? explainer?
   })

(defn workload
  "Gset workload."
  [{:keys [rate] :as opts}]
  (let [opts (merge
              {:directory      "."
               :max-plot-bytes 1048576
               :plot-timeout   10000}
              opts)
        gen       (wr/gen opts)
        final-gen (gen/phases
                   (gen/log "Quiesce...")
                   (gen/sleep 5)
                   (gen/log "Final reads...")
                   (->> (range 100)
                        (map (fn [k]
                               {:type :invoke :f :r-final :value [[:r k nil]] :final-read? true}))
                        (gen/each-thread)
                        (gen/clients)
                        (gen/stagger (/ rate))))]
    {:client          (GSetClient. nil)
     :generator       gen
     :final-generator final-gen}))

(defn workload-homogeneous-txns
  "A workload with a generator that emits transactions that are all read or write ops,
   E.g. for the ElectricSQL TypeScript client.
   Generator must only generate txns consisting exclusively of reads or writes
   to accommodate the API."
  [{:keys [min-txn-length max-txn-length] :as opts}]
  (let [min-txn-length (* 2 (or min-txn-length 1))
        max-txn-length (* 2 (or max-txn-length 4))
        opts           (assoc opts
                              :min-txn-length     min-txn-length
                              :max-txn-length     max-txn-length
                              :key-dist           :uniform
                              :key-count          100
                              :max-writes-per-key 1000)
        workload (workload opts)
        generator (->> (:generator workload)
                       (mapcat (fn [{:keys [value] :as op}]
                                 (let [[rs ws] (->> value
                                                    (reduce (fn [[rs ws] [f _k _v :as mop]]
                                                              (case f
                                                                :r (if (some #(= % mop) rs)
                                                                     [rs ws]
                                                                     [(conj rs mop) ws])
                                                                :w [rs (conj ws mop)]))
                                                            [[] []]))]
                                   (cond (and (seq rs)
                                              (seq ws))
                                         (let [r-op (assoc op :value rs :f :r-txn)
                                               w-op (assoc op :value ws :f :w-txn)]
                                           (->> [r-op w-op] shuffle vec))

                                         (seq rs)
                                         [(assoc op :value rs :f :r-txn)]

                                         (seq ws)
                                         [(assoc op :value ws :f :w-txn)])))))]
    (assoc workload
           :generator generator)))

(defn workload-single-writes
  "The default workload with a generator that emits transactions consisting of a single write."
  [opts]
  (let [opts      (merge opts
                         {:min-txn-length     1
                          :max-txn-length     1
                          :key-dist           :uniform
                          :key-count          100
                          :max-writes-per-key 1000})
        workload  (workload opts)
        generator (->> (:generator workload)
                       (filter #(->> %
                                     :value
                                     first
                                     first
                                     (= :w)))
                       (map #(assoc % :f :w-txn)))]
    (assoc workload
           :generator generator)))

(defn cyclic-versions-helper
  "Given a cyclic-versions result map and a history, filter history for involved transactions."
  [{:keys [key scc] :as _cyclic-versions} history]
  (->> history
       h/client-ops
       h/oks
       (h/filter (fn [{:keys [value] :as _op}]
                   (->> value
                        (reduce (fn [_acc [_f k v]]
                                  (if (and (= key k)
                                           (contains? scc v))
                                    (reduced true)
                                    false))
                                false))))
       (map (fn [op]
              (select-keys op [:index :process :value])))))

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

(def not-g-single-item-lost-update
  (->> [[0 "wx0"]
        [1 "rx0wx1"]
        [2 "rx0wx2"]
        [3 "rx0"]
        [3 "rx1"]
        [3 "rx2"]]
       (mapcat #(apply op-pair %))
       h/history))

(def lost-update
  ; Hlost,update: r1 (x0, 10) r2(x0 , 10) w2(x2 , 15) c2 w1(x1 , 14) c1
  ;  [x0 << x2 << x1 ]
  (->> [{:process 1 :type :invoke :value [[:r :x nil] [:w :x 14]] :f :txn}
        {:process 2 :type :invoke :value [[:r :x nil] [:w :x 15]] :f :txn}
        {:process 2 :type :ok     :value [[:r :x 10]  [:w :x 15]] :f :txn}
        {:process 1 :type :ok     :value [[:r :x 10]  [:w :x 14]] :f :txn}]
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
