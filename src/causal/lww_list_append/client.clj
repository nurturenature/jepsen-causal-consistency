(ns causal.lww-list-append.client
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
            [clojure.string :as str]
            [jepsen
             [client :as client]
             [util :as u]]
            [next.jdbc :as jdbc]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn op->better-sqlite3
  "Given an op, return the JSON for a better-sqlite3 transaction."
  [{:keys [value] :as _op}]
  (let [value (->> value
                   (mapv (fn [[f k v]]
                           (case f
                             :r      {"f" f "k" k "v" nil}
                             :append {"f" f "k" k "v" (str v)}))))
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

        type'  (if (->> error' :code (contains? #{"SQLITE_BUSY" "SQLITE_BUSY_SNAPSHOT"}))
                 :fail
                 type')

        value' (->> value'
                    (map (fn [[f k v] mop]
                           (let [[f' k' v'] [(keyword (:f mop)) (:k mop) (:v mop)]]
                             (assert (and (= f f')
                                          (= k k'))
                                     (str "Original op: " op ", result: " rslt ", mismatch"))
                             (case f
                               :r
                               (if (nil? v')
                                 [:r k nil]
                                 (let [v' (->> (str/split v' #"\s+")
                                               (mapv parse-long))]
                                   [:r k v']))

                               :append
                               (let [v' (parse-long v')]
                                 (assert (= v v')
                                         (str "Munged write value in result, expected " v ", actual " v'))
                                 [f k v]))))
                         value)
                    (into []))]
    (case type'
      :ok
      (do
        (assert (= (count value)
                   (count value'))
                {:op     op
                 :rslt   rslt
                 :value  value
                 :type'  type'
                 :value' value'
                 :error' error'})
        (assoc op
               :type  :ok
               :value value'))

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
           :url  (str "http://" node ":8089/lww/better-sqlite3")))

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

(defn txn->electric-findUnique
  "Given a transaction, returns the JSON necessary to use ElectricSQL findUnique.
   Transaction is assumed to be a read mop."
  [txn]
  (assert (= 1 (count txn))
          (str "findUnique is single mop only: " txn))
  (let [[f k v :as mop] (first txn)]
    (assert (= :r f)  (str "findUnique is read only: " mop))
    (assert (= nil v) (str "malformed read: " mop))
    (->> {:where {:k k}}
         json/generate-string)))

(defn electric-findUnique->txn
  "Given the original transaction and the result of an ElectricSQL findUnique,
   returns the transaction with the result merged."
  [txn result]
  (assert (= 1 (count txn))
          (str "findUnique is single mop only: " txn))
  (let [[f k v :as mop] (first txn)
        {:keys [_k' v']} (json/parse-string result true)]
    (assert (= :r f)  (str "findUnique is read only: " mop))
    (assert (= nil v) (str "malformed read: " mop))
    [[:r k v']]))

(defn txn->electric-upsert
  "Given a transaction, returns the JSON necessary to use ElectricSQL upsert.
   Transaction is assumed to be a write."
  [txn]
  (assert (= 1 (count txn))
          (str "upsert is single mop only: " txn))
  (let [[f k v :as mop] (first txn)]
    (assert (= :append f)  (str "upsert is append only: " mop))
    (assert (not (nil? v)) (str "nil v in append: " mop))
    (->> {:create {:k k
                   :v v}
          :update {:v v}
          :where  {:k k}}
         json/generate-string)))

(defn electric-upsert->txn
  "Given the original transaction and the ElectricSQL upsert result,
   return the transaction with the results merged in."
  [txn result]
  (assert (= 1 (count txn))
          (str "upsert is single mop only: " txn))
  (let [[f k v :as mop] (first txn)
        {:keys [k' v'] :as result} (json/parse-string result true)]
    (assert (= :append f) (str "upsert is append only: " mop))
    (assert (= k k')      (str "different keys: " mop ", " result))
    (assert (= v v')      (str "different values: " mop ", " result))
    [[:append k v]]))

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
    (assert (= 1 (count value))
            (str "ElectricSQLClient is single mop only: " value))
    (let [op (assoc op :node node)]
      (try+ (let [[f _k _v]  (first value)
                  [url body] (case f
                               :r
                               [(str url "/lww/electric-findUnique")
                                (txn->electric-findUnique value)]

                               :append
                               [(str url "/lww/electric-upsert")
                                (txn->electric-upsert value)])
                  result (http/post url
                                    {:body               body
                                     :content-type       :json
                                     :socket-timeout     1000
                                     :connection-timeout 1000
                                     :accept             :json})
                  result (:body result)
                  result (case f
                           :r      (electric-findUnique->txn value result)
                           :append (electric-upsert->txn     value result))]
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
    (let [table (get test :postgresql-table "lww")]
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
                                   :r
                                   (let [v (->> (jdbc/execute! tx [(str "SELECT k,v FROM " table " WHERE k = " k)])
                                                (map result-kw)
                                                first)
                                         v (when v
                                             (->> (str/split v #"\s+")
                                                  (mapv parse-long)))]
                                     [:r k v])
                                   :append
                                   (do
                                     (assert (= 1
                                                (->> (jdbc/execute! tx [(str "INSERT INTO " table " (k,v) VALUES (" k ",'" v "')"
                                                                             "ON CONFLICT (k) DO UPDATE SET v = lww.v || ' ' || '" v "'")])
                                                     first
                                                     :next.jdbc/update-count)))
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
                :error :deadlock))
       (catch (fn [e]
                (if (and (instance? org.postgresql.util.PSQLException e)
                         (re-find #"ERROR: could not serialize access due to concurrent update\n.*" (.getMessage e)))
                  true
                  false)) {}
         (assoc op
                :type  :fail
                :error :concurrent-update)))))

  (teardown!
    [_this _test])

  (close!
    [{:keys [conn] :as _client} _test]
    (.close conn)))

(defn db-specs
  "Map node names to db-specs."
  [{:keys [electric-host postgres-host] :as _opts}]
  {"postgresql"  {:dbtype   "postgresql"
                  :host     postgres-host
                  :user     "postgres"
                  :password "db_password"
                  :dbname   "electric-sqlite3-client"}
   "electricsql" {:dbtype   "postgresql"
                  :host     electric-host
                  :port     65432
                  :user     "postgres"
                  :password "proxy_password"
                  :dbname   "electric-sqlite3-client"}})

;; TODO: why isn't electricsql allowing a connection using jdbc?
;;       just connecting to postgresql for now
(defn node->client
  "Maps a node name to its `client` protocol.
   BetterSQLite3Client is default."
  [{:keys [better-sqlite3-nodes electricsql-nodes postgresql-nodes local-sqlite3?] :as test} node]
  (cond
    ; overrides all others
    local-sqlite3?
    (BetterSQLite3Client. nil)

    (contains? better-sqlite3-nodes node)
    (BetterSQLite3Client. nil)

    (contains? electricsql-nodes node)
    (ElectricSQLClient. nil)

    (contains? postgresql-nodes node)
    (PostgreSQLJDBCClient. (get (db-specs test) "postgresql"))

    :else
    (BetterSQLite3Client. nil)))

(defrecord LWWListAppendClient [conn]
  client/Client
  (open!
    [_this test node]
    (client/open! (node->client test node) test node)))
