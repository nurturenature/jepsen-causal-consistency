(ns causal.electricsql
  "Install and configure ElectricSQL sync service on node `electricsql`."
  (:require [clojure.tools.logging :refer [info]]
            [causal
             [postgresql :as postgresql]]
            [jepsen
             [control :as c]
             [util :as u]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [slingshot.slingshot :refer [try+]]))

(def host
  "Name of host machine for ElectricSQL."
  "electricsql")

(def pg-proxy-port
  "PostgreSQL proxy port."
  :65432)

(def pg-proxy-password
  "PostgreSQL proxy password."
  postgresql/electric-password)

(def connection-url
  "ElectricSQL connection URI."
  (str "postgresql://" postgresql/electric-user ":" pg-proxy-password "@" host ":" pg-proxy-port "/electric"))

(def install-dir
  "Directory to install ElectricSQL to."
  "/root/electricsql")

(def bin
  "ElectricSQL binary."
  (str install-dir "/components/electric/_build/prod/rel/electric/bin/electric"))

(def bin-env
  "Environment vars to start `bin` with."
  (c/env {:DATABASE_URL           postgresql/connection-url
          :LOGICAL_PUBLISHER_HOST host
          :PG_PROXY_PORT          pg-proxy-port
          :PG_PROXY_PASSWORD      pg-proxy-password
          :AUTH_MODE              :insecure}))

(def run-elixir
  "Helper script to run elixir apps."
  "/root/elixir/run-elixir.sh")

(defn run-elixir-sh
  "Creates /root/elixir/run-elixir.sh helper script.
   Assumes `c/on c/su`"
  []
  (c/exec :rm :-f run-elixir)
  (c/exec :echo "export PATH=\"/root/elixir/bin:$PATH\""
          :>> run-elixir)
  (c/exec :echo "\"$@\""
          :>> run-elixir)
  (c/exec :chmod :u+x run-elixir))

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
    (debian/install [:postgresql-client])))

(defn teardown
  "Teardown ElectricSQL."
  [_opts]
  (info "Tearing down ElectricSQL")
  (c/on host
        (c/su
         ; tests may have stopped/killed service
         (when (try+
                (c/exec bin-env run-elixir bin :restart)
                true
                (catch [] _
                  false))
           ; un-electrify, drop table
           (u/meh
            (c/exec :psql :-d connection-url
                    :-c "ALTER TABLE public.lww_registers DISABLE ELECTRIC;"))
           (u/meh
            (c/exec :psql :-d connection-url
                    :-c "DROP TABLE public.lww_registers;"))
           (info "ElectricSQL tables: "
                 (c/exec :psql :-d connection-url
                         :-c "\\dt"))

           ; stop service 
           (c/exec bin-env run-elixir bin :stop)))))

(defn delete
  "Delete ElectricSQL."
  [_opts]
  (info "Deleting ElectricSQL")
  (c/on host
        (c/su
         (u/meh
          (c/exec bin-env bin :stop))
         (u/meh
          (c/exec :rm :-rf install-dir)))))

(defn setup
  "Sets up, installing if necessary, starts, configures ElectricSQL"
  [_opts]
  (info "Setting up ElectricSQL on " host)
  (c/on host
        (c/su
         ; dependencies, only install if not present
         ; erlang
         (when-not (try+
                    (c/exec :erl :-eval "erlang:display (erlang:system_info (otp_release)), halt () ." :-noshell)
                    true
                    (catch [] _
                      false))
           (debian/install [:build-essential :autoconf :m4 :libncurses-dev :libwxgtk3.2-dev :libwxgtk-webview3.2-dev :libgl1-mesa-dev :libglu1-mesa-dev :libpng-dev :libssh-dev :unixodbc-dev :xsltproc :fop :libxml2-utils
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
                    (c/exec run-elixir :elixir :-v)
                    true
                    (catch [] _
                      false))

           (cu/install-archive! "https://github.com/elixir-lang/elixir/archive/refs/tags/v1.15.7.tar.gz" "/root/elixir")
           (c/cd "/root/elixir"
                 (c/exec :make))
           (run-elixir-sh))
         (info "Elixir version: "
               (c/exec run-elixir :elixir :-v))

         ; ElectricSQL
         (when (not (try+
                     (c/exec bin-env run-elixir bin :restart)
                     true
                     (catch [] _
                       false)))
           (debian/install [:git])
           (c/exec :rm :-rf install-dir)
           (c/exec :git :clone "https://github.com/electric-sql/electric.git" install-dir)
           (c/cd (str install-dir "/components/electric")
                 (c/exec run-elixir :mix :deps.get)
                 (c/exec run-elixir :mix :compile)
                 (c/exec (c/env {:MIX_ENV :prod})
                         run-elixir :mix :release)
                 (c/exec bin-env run-elixir bin :daemon)
                 (u/sleep 1000)))
         (info "ElectricSQL pid: "
               (c/exec bin-env run-elixir bin :pid))

         ; create and electrify table
         (insure-psql)
         (c/exec :psql :-d connection-url
                 :-c "CREATE TABLE public.lww_registers (key integer PRIMARY KEY, value integer);")
         (c/exec :psql :-d connection-url
                 :-c "ALTER TABLE public.lww_registers ENABLE ELECTRIC;")
         (info "ElectricSQL tables: "
               (c/exec :psql :-d connection-url
                       :-c "\\dt")))))

(defn start?
  "Attempts to restart, then start if restart fails, ElectricSQL.
   Returns true if server was able to be (re)started and is running."
  [_opts]
  (info "Starting ElectricSQL")
  (c/on host
        (c/su
         (if (try+
              (c/exec bin-env run-elixir bin :restart)
              true
              (catch [] _
                false))
           true
           (try+
            (c/exec bin-env run-elixir bin :start)
            true
            (catch [] _
              false))))))

(def opt-spec
  "Specifies CLI options."
  [[nil "--nodes NODE_LIST" (str "Must be " host)
    :default [host]]
   [nil "--teardown" "If set, tears down ElectricSQL."]
   [nil "--delete"   "If set, deletes ElectricSQL."]
   [nil "--setup"    "If set, sets up, installing if necessary, and starts ElectricSQL."]
   [nil "--start"    "If set, starts ElectricSQL using current configs, state."]])

(defn opt-fn
  "Transforms CLI options before execution."
  [parsed]
  (let [nodes (u/coll (get-in parsed [:options :nodes]))]
    (assert (= nodes [host]) (str ":nodes must be [" host "], not " nodes)))
  parsed)

(defn run-fn
  "Body of command."
  [{:keys [options]}]
  (when (:teardown options)
    (teardown options))

  (when (:delete options)
    (delete options))

  (when (:setup options)
    (setup options))

  (when (:start options)
    (start? options)))

(def command
  {"electricsql"
   {:opt-spec opt-spec
    :opt-fn opt-fn
    :usage (str "Installs ElectricSQL on " host ".")
    :run run-fn}})