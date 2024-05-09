(ns causal.gset.client
  (:require [causal.gset.checker.strong-convergence :as sc]
            [causal.sqlite3 :as sqlite3]
            [cheshire.core :as json]
            [clj-http.client :as http]
            [clojure
             [string :as str]]
            [jepsen
             [client :as client]
             [control :as c]
             [util :as u]]
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
    [{:keys [node url] :as _this} _test {:keys [value] :as op}]
    (let [op (assoc op :node node)]
      (try+ (let [[r-or-w _ _] (first value)
                  [url body]   (case r-or-w
                                 :r [(str url "/gset/electric-findMany")
                                     (txn->electric-findMany value)]
                                 :w [(str url "/gset/electric-createMany")
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
                       (instance? java.net.SocketTimeoutException %)
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
  [{:keys [value] :as op} {:keys [status body] :as _rslt}]
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
           :url  (str "http://" node ":8089/gset/better-sqlite3")))

  (setup!
    [_this _test])

  (invoke!
    [{:keys [node url] :as _this} _test op]
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
                       (instance? java.net.SocketTimeoutException %)
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
    [{:keys [node] :as _this} test {:keys [value] :as op}]
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
                     :error :database-locked))
            (catch [:type :jepsen.control/nonzero-exit
                    :exit 1
                    :err "Parse error near line 1: database is locked (5)\n"] {}
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
    [this test node]
    (let [table (get test :postgresql-table "gset")]
      (assoc this
             :node      node
             :conn      (get-jdbc-connection db-spec)
             :table     table
             :result-kw (keyword table "v"))))

  (setup!
    [_this _test])

  (invoke!
    [{:keys [conn node table result-kw] :as _this} _test {:keys [value] :as op}]
    (let [op (assoc op
                    :node node)]
      (try+
       (let [mops' (jdbc/with-transaction
                     [tx conn {:isolation :repeatable-read}]
                     (->> value
                          (map (fn [[f k v :as mop]]
                                 (case f
                                   :r [:r k (->> (jdbc/execute! tx [(str "SELECT k,v FROM " table " WHERE k = " k)])
                                                 (map result-kw)
                                                 (into (sorted-set)))]
                                   :w (do
                                        (assert (= 1
                                                   (let [id (->> k (* 10000) (+ v))]
                                                     (->> (jdbc/execute! tx [(str "INSERT INTO " table " (id,k,v) VALUES (" id "," k "," v ")")])
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
