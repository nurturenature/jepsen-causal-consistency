(ns causal.lww-list-append.client
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
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
(defn db-specs
  "Map node names to db-specs."
  [{:keys [electric-host postgres-host] :as _opts}]
  {"postgresql"  {:dbtype   "postgresql"
                  :host     postgres-host
                  :user     "postgres"
                  :password "db_password"
                  :dbname   "electric"}
   "electricsql" {:dbtype   "postgresql"
                  :host     electric-host
                  :port     65432
                  :user     "postgres"
                  :password "proxy_password"
                  :dbname   "electric"}})

(defn get-jdbc-connection
  "Tries to get a `jdbc` connection for a total of ms, default 5000, every 1000ms.
   Throws if no client can be gotten."
  [db-spec]
  (let [conn (->> db-spec
                  jdbc/get-datasource
                  jdbc/get-connection)]
    (when (nil? conn)
      (throw+ {:connection-failure db-spec}))
    conn))

(defrecord PostgreSQLJDBCClient [db-spec]
  client/Client
  (open!
    [this test node]
    (let [table (get test :postgresql-table "lww")]
      (info "PostgreSQL opening: " db-spec)
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
                                                (->> (jdbc/execute! tx [(str "INSERT INTO " table " (k,v,bucket) VALUES (" k ",'" v "',0)"
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

(defn parse-update
  "Given the original txn and the result of an update,
   parse and merge the results returning a txn of mops."
  [txn result]
  (let [appends (->> (get-in txn [:data :lww :update])
                     (mapv (fn [{:keys [data where]}]
                             (let [v (when-let [v (:v data)]
                                       (parse-long v))]
                               [:append (:k where) v]))))
        reads   (->> (get result :lww)
                     (mapv (fn [{:keys [k v]}]
                             (let [v (when v (->> (str/split v #"\s+")
                                                  (mapv parse-long)))]
                               [:r k v]))))]
    (into appends reads)))

(defn parse-updateMany
  "Given the original txn and the result of an updateMany,
   parse and merge the results returning a txn of mops."
  [txn result]
  (let [v  (->> (get-in txn [:data :v])
                parse-long)
        ks (get-in txn [:where :k :in])]
    (assert (= (:count result) (count ks)) (str "Update count for value: " txn ", result: " result))
    (->> ks
         (mapv (fn [k]
                 [:append k v])))))

(defn parse-findMany
  "Given the original txn and the result of an findMany,
   parse and merge the results returning a txn of mops."
  [txn result]
  (let [kvs (->> result
                 (map (fn [{:keys [k v]}]
                        (let [v (when v (->> (str/split v #"\s+")
                                             (mapv parse-long)))]
                          [k v])))
                 (into {}))]
    (->> (get-in txn [:where :k :in])
         (mapv (fn [k]
                 [:r k (get kvs k)])))))

(defrecord ElectricSQLiteClient [conn]
  client/Client
  (open!
    [this {:keys [workload] :as test} node]
    (if (and (= :active-active workload)
             (= "n1" node))
      (client/open! (PostgreSQLJDBCClient. (get (db-specs test) "postgresql")) test node)
      (assoc this
             :node node
             :url  (str "http://" node ":8089"))))

  (setup!
    [_this _test])

  (invoke!
    [{:keys [node url] :as _this} _test {:keys [f value] :as op}]
    (let [op   (assoc op :node node)
          url  (str url "/lww/" (name f))
          body (json/generate-string value)]
      (try+ (let [result (http/post url
                                    {:body               body
                                     :content-type       :json
                                     :socket-timeout     1000
                                     :connection-timeout 1000
                                     :accept             :json})
                  result (-> result :body (json/parse-string true))
                  result (case f
                           :update
                           (parse-update value result)

                           :updateMany
                           (parse-updateMany value result)

                           :findMany
                           (parse-findMany value result))]
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


(defrecord ElectricPGliteClient [conn]
  client/Client
  (open!
    [this {:keys [workload] :as test} node]
    (if (and (= :active-active workload)
             (= "n1" node))
      (client/open! (PostgreSQLJDBCClient. (get (db-specs test) "postgresql")) test node)
      (assoc this
             :node node
             :url  (str "http://" node ":8089"))))

  (setup!
    [_this _test])

  (invoke!
    [{:keys [node url] :as _this} _test {:keys [f value] :as op}]
    (let [op   (assoc op :node node)
          url  (str url "/lww/" (name f))
          body (json/generate-string value)]
      (try+ (let [result (http/post url
                                    {:body               body
                                     :content-type       :json
                                     :socket-timeout     1000
                                     :connection-timeout 1000
                                     :accept             :json})
                  result (-> result :body (json/parse-string true))
                  result (case f
                           :update
                           (parse-update value result)

                           :updateMany
                           (parse-updateMany value result)

                           :findMany
                           (parse-findMany value result))]
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

(defn op->pgexec-pglite
  "Given an op, return the JSON for a pgexec-pglite transaction."
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

(defn pgexec-pglite->op
  "Given the original op and a pgexec-pglite JSON result,
   return the op updated with pgexec-pglite results."
  [{:keys [value] :as op} {:keys [status body] :as _rslt}]
  (let [_                     (assert (= status 200))
        rslt                  (json/parse-string body true)
        [type' value' error'] [(keyword (:type rslt)) (:value rslt) (:error rslt)]

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
                                 (let [v' (->> v'
                                               first
                                               :v)
                                       v' (->> (str/split v' #"\s+")
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

;; TODO: PGlite throws parse error on INSERT but not SELECT?
(defrecord PGExecPGliteClient [conn]
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
      (try+ (let [url    (str url "/lww/pglite-exec")
                  body   (op->pgexec-pglite op)
                  result (http/post url
                                    {:body               body
                                     :content-type       :json
                                     :socket-timeout     1000
                                     :connection-timeout 1000
                                     :accept             :json})]
              (pgexec-pglite->op op result))
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

