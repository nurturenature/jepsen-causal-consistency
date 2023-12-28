(ns causal.electricsql
  "Install and configure ElectricSQL sync service on node `electricsql`."
  (:require [clojure.tools.logging :refer [info]]
            [causal
             [postgresql :as postgresql]]
            [jepsen
             [control :as c]
             [util :as u]]
            [slingshot.slingshot :refer [try+]]
            [jepsen.os.debian :as deb]))

(def host
  "Name of host machine for ElectricSQL."
  "electricsql")

(def pg-proxy-port
  "PostgreSQL proxy port."
  :65432)

(def pg-proxy-password
  "PostgreSQL proxy password."
  postgresql/electric-password)

(def install-dir
  "Directory to install ElectricSQL to."
  "/root/electricsql")

(def opt-spec
  "Specifies CLI options."
  [[nil "--nodes NODE_LIST" (str "Must be " host)
    :default [host]]])

(defn opt-fn
  "Transforms CLI options before execution."
  [parsed]
  (let [nodes (u/coll (get-in parsed [:options :nodes]))]
    (assert (= nodes [host]) (str ":nodes must be [" host "], not " nodes)))
  parsed)

(defn run-fn
  "Body of command."
  [opts]
  (info (pr-str opts))
  (info "Installing ElectricSQL on " host)
  (c/on host
        (c/su
         ; stop and cleanup any existing
         (u/meh
          (c/exec (str install-dir "/components/electric/_build/prod/rel/electric/bin/electric") :stop)
          (c/exec :rm :-rf install-dir))

         ; dependencies, only install if not present
         ; use `asdf` shims, it's quick & dirty
         (when-not (try+
                    (c/exec :asdf :info)
                    true
                    (catch [] _
                      false))
           (deb/install [:curl :git])
           (c/exec :git :clone "https://github.com/asdf-vm/asdf.git" "~/.asdf" :--branch "v0.13.1")
           (c/exec :chmod :u+x "~/.asdf/asdf.sh" "~/.asdf/completions/asdf.bash")
           (c/exec :echo ". \"$HOME/.asdf/asdf.sh\""               :>> "~/.bashrc")
           (c/exec :echo ". \"$HOME/.asdf/completions/asdf.bash\"" :>> "~/.bashrc")
           (info
            (c/exec :asdf :info)))

         ; erlang
         (when-not (try+
                    (c/exec "erl -eval 'erlang:display (erlang:system_info (otp_release)), halt () .'  -noshell")
                    true
                    (catch [] _
                      false))
           (deb/install [:build-essential :autoconf :m4 :libncurses-dev :libwxgtk3.2-dev :libwxgtk-webview3.2-dev :libgl1-mesa-dev :libglu1-mesa-dev :libpng-dev :libssh-dev :unixodbc-dev :xsltproc :fop :libxml2-utils])
           (c/exec :asdf :plugin :add :erlang "https://github.com/asdf-vm/asdf-erlang.git")
           (c/exec :asdf :install :erlang :25.3.2.8)
           (c/exec :asdf :global  :erlang :25.3.2.8)
           (info
            (c/exec "erl -eval 'erlang:display (erlang:system_info (otp_release)), halt () .'  -noshell")))

         ; elixir
         (when-not (try+
                    (c/exec :elixir :-v)
                    true
                    (catch [] _
                      false))
           (deb/install [:unzip])
           (c/exec :asdf :plugin :add :elixir "https://github.com/asdf-vm/asdf-elixir.git")
           (c/exec :asdf :install :elixir :1.15.7-otp-25)
           (c/exec :asdf :global  :elixir :1.15.7-otp-25)
           (info
            (c/exec :elixir :-v)))

         ; always (re)install ElectricSQL
         (c/exec :git :clone "https://github.com/electric-sql/electric.git" install-dir)
         (c/cd (str install-dir "/components/electric")
               (c/exec :asdf :local :erlang :25.3.2.8)
               (c/exec :asdf :local :elixir :1.15.7-otp-25)
               (c/exec :mix :deps.get)
               (c/exec :mix :compile)
               (c/exec (c/env {:MIX_ENV :prod})
                       :mix :release)
               (c/exec (c/env {:DATABASE_URL           postgresql/database-url
                               :LOGICAL_PUBLISHER_HOST host
                               :PG_PROXY_PORT          pg-proxy-port
                               :PG_PROXY_PASSWORD      pg-proxy-password
                               :AUTH_MODE              :insecure})
                       (str install-dir "/components/electric/_build/prod/rel/electric/bin/electric") :daemon)
               (info "ElectricSQL pid: "
                     (c/exec (str install-dir "/components/electric/_build/prod/rel/electric/bin/electric") :pid))))
        (info "Installed")))

;; TODO
;; interact through proxy to create table, etc
;; CREATE TABLE public.lww_registers (key integer PRIMARY KEY, value integer);
;; ALTER TABLE lww_registers ENABLE ELECTRIC;

(def command
  {"electricsql"
   {:opt-spec opt-spec
    :opt-fn opt-fn
    :usage (str "Installs ElectricSQL on " host ".")
    :run run-fn}})