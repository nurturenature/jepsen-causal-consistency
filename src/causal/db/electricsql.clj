(ns causal.db.electricsql
  "Install and configure ElectricSQL sync service on node `electricsql`."
  (:require [clojure.tools.logging :refer [info]]
            [causal.db
             [postgresql :as postgresql]
             [promises :as promises]]
            [jepsen
             [control :as c]
             [db :as db]
             [util :as u]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as deb]
            [slingshot.slingshot :refer [try+]]))

(def host
  "Name of host machine for ElectricSQL."
  "electricsql")

(def pg-proxy-port
  "PostgreSQL proxy port."
  "65432")

(def pg-proxy-password
  "PostgreSQL proxy password."
  "postgres")

(def connection-url
  "ElectricSQL connection URI."
  (str "postgresql://postgres:" pg-proxy-password "@" host ":" pg-proxy-port))

(def install-dir
  "Directory to install ElectricSQL to."
  "/root/electricsql")

(def build-dir
  "ElectricSQL build dir."
  (str install-dir "/components/electric/_build"))

(def bin
  "ElectricSQL binary."
  (str build-dir "/prod/rel/electric/bin/electric"))

(def bin-env
  "Environment vars to start `bin` with."
  {:DATABASE_URL              postgresql/connection-url
   :ELECTRIC_WRITE_TO_PG_MODE :direct_writes
   :LOGICAL_PUBLISHER_HOST    host           ; ignored with direct_writes
   :LOGICAL_PUBLISHER_PORT    5433           ; ignored with direct_writes
   :PG_PROXY_PORT             pg-proxy-port
   :PG_PROXY_PASSWORD         pg-proxy-password
   :AUTH_MODE                 :insecure
   :ELECTRIC_USE_IPV6         :false
   ;:LOG_LEVEL                 :debug
   })

(def pid-file (str install-dir "/electricsql.pid"))

(def log-file-short "electricsql.log")
(def log-file       (str install-dir "/" log-file-short))


(defn insure-psql
  "Insures psql is installed.
   Assumes `c/on c/su`."
  []
  (when-not (try+
             (c/exec :psql :--version)
             true
             (catch [] _
               false))
    (postgresql/insure-repo)
    (deb/install [:postgresql-client])))

(defn drop-table
  "Attempts to drop table.
   TODO; Dropping a table isn't fully supported by ElectricSQL."
  []
  (insure-psql)
  ; do our best, just try to delete rows, un-electrify, drop table
  (u/meh
   (c/exec :psql :-d connection-url
           :-c "DELETE FROM public.lww_registers;"))
  ;; (u/meh
  ;;  (c/exec :psql :-d connection-url
  ;;          :-c "ALTER TABLE public.lww_registers DISABLE ELECTRIC;"))
  ;; (u/meh
  ;;  (c/exec :psql :-d connection-url
  ;;          :-c "DROP TABLE public.lww_registers;"))
  (info "ElectricSQL table public.lww_registers:")
  (u/meh
   (info (c/exec :psql :-d connection-url
                 :-c "TABLE public.lww_registers;"))))

(defn insure-table-exists-empty
  "Insures the lww_register table exists, is electrified, and empty."
  []
  (insure-psql)
  ; may already exist, try and let fail to guard electrification
  (u/meh
   (c/exec :psql :-d connection-url
           :-c "CREATE TABLE public.lww_registers (k integer PRIMARY KEY, v integer);")
   (c/exec :psql :-d connection-url
           :-c "ALTER TABLE public.lww_registers ENABLE ELECTRIC;"))
  ; insure empty
  (c/exec :psql :-d connection-url
          :-c "DELETE FROM public.lww_registers;")
  (info "ElectricSQL table public.lww_registers:")
  (info (c/exec :psql :-d connection-url
                :-c "TABLE public.lww_registers;")))

(defn db
  "ElectricSQL SQLite database."
  []
  (reify db/DB
    (setup!
      [this test node]
      (info "Setting up ElectricSQL")

      (c/su
       ; dependencies, only install if not present
       ; erlang
       (when-not (try+
                  (c/exec :erl :-eval "erlang:display (erlang:system_info (otp_release)), halt () ." :-noshell)
                  true
                  (catch [] _
                    false))
         (deb/install [:build-essential :autoconf :m4 :libncurses-dev :libwxgtk3.2-dev :libwxgtk-webview3.2-dev :libgl1-mesa-dev :libglu1-mesa-dev :libpng-dev :libssh-dev :unixodbc-dev :xsltproc :fop :libxml2-utils
                       :wget])

         (cu/install-archive! "https://github.com/erlang/otp/releases/download/OTP-25.3.2.8/otp_src_25.3.2.8.tar.gz" "/root/erlang")
         (c/cd "/root/erlang"
               (c/exec "ERL_TOP=/root/erlang" "./configure")
               (c/exec "ERL_TOP=/root/erlang" :make)
               (c/exec "ERL_TOP=/root/erlang" :make :install)))
       (info "Erlang version: "
             (c/exec :erl :-eval "erlang:display (erlang:system_info (otp_release)), halt () ." :-noshell))

       ; elixir
       (when-not (try+
                  (c/exec :elixir :-v)
                  true
                  (catch [] _
                    false))

         (cu/install-archive! "https://github.com/elixir-lang/elixir/archive/refs/tags/v1.15.7.tar.gz" "/root/elixir")
         (c/cd "/root/elixir"
               (c/exec :make))
         (c/exec :ln :-s "/root/elixir/bin/elixir"  "/usr/local/bin/elixir")
         (c/exec :ln :-s "/root/elixir/bin/elixirc" "/usr/local/bin/elixirc")
         (c/exec :ln :-s "/root/elixir/bin/iex"     "/usr/local/bin/iex")
         (c/exec :ln :-s "/root/elixir/bin/mix"     "/usr/local/bin/mix"))
       (info "Elixir version: "
             (c/exec :elixir :-v))

       ; ElectricSQL
       (when (not (cu/exists? install-dir))
         (cu/install-archive! "https://github.com/electric-sql/electric/archive/refs/tags/electric-sql@0.9.0.tar.gz" install-dir)
         (c/cd (str install-dir "/components/electric")
               (c/exec :mix :deps.get)
               (c/exec :mix :compile)
               (c/exec (c/env {:MIX_ENV :prod
                               :ELECTRIC_VERSION :0.9.0})
                       :mix :release :--overwrite)))

       (assert (deref promises/postgresql-available? 300000 false)
               "PostgreSQL not available")

       (db/start! this test node)

       ;; TODO: http://electricsql:?/api/status
       (u/sleep 5000)

       (insure-table-exists-empty))
      (deliver promises/electricsql-available? true))

    (teardown!
      [this test node]
      (doseq [[n p] @promises/sqlite3-teardown?s]
        (assert (deref p 10000 false)
                (str "SQLite3 node failed to teardown?: " n)))

      (info "Tearing down ElectricSQL")
      (drop-table)

      ; stop service
      (db/kill! this test node)

      (c/su
       (c/exec :rm :-rf log-file))

      (deliver promises/electricsql-teardown? true))

    ; ElectricSQL doesn't have `primaries`.
    ; db/Primary

    db/LogFiles
    (log-files
      [_db _test _node]
      {log-file log-file-short})

    db/Kill
    (start!
      [_this _test _node]
      (if (cu/daemon-running? pid-file)
        :already-running
        (c/su
         (cu/start-daemon!
          {:chdir install-dir
           :env bin-env
           :pidfile pid-file
           :logfile log-file}
          bin
          :start)
         :started)))

    (kill!
      [_this _test _node]
      (c/su
       (cu/stop-daemon! pid-file)
       (cu/grepkill! "beam.smp")
       (cu/grepkill! "epmd"))
      :killed)

    db/Pause
    (pause!
      [_this _test _node]
      (c/su
       (cu/grepkill! :stop "beam.smp"))
      :paused)

    (resume!
      [_this _test _node]
      (c/su
       (cu/grepkill! :cont "beam.smp"))
      :resumed)))
