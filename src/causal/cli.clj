(ns causal.cli
  "Command-line entry point for ElectricSQL tests."
  (:require [causal.gset.workload :as gset]
            [causal.lww-register.workload :as lww-register]
            [causal
             [sqlite3 :as sqlite3]
             [nemesis :as nemesis]]
            [clojure
             [set :as set]
             [string :as str]]
            [clojure.tools.logging :refer [info warn]]
            [elle
             [consistency-model :as cm]
             [graph :as g]
             [rw-register :as rw]
             [txn :as txn]]
            [jepsen
             [checker :as checker]
             [cli :as cli]
             [control :as c]
             [generator :as gen]
             [os :as os]
             [store :as store]
             [tests :as tests]
             [util :as u]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.os.debian :as debian]))

(def workloads
  "A map of workload names to functions that take CLI options and return
  workload maps."
  {:gset               gset/workload
   :gset-homogeneous   gset/workload-homogeneous-txns
   :gset-single-writes gset/workload-single-writes
   :lww-register       lww-register/workload
   :none               (fn [_] tests/noop-test)})

(def all-workloads
  "A collection of workloads we run by default."
  [:gset])

(def all-nemeses
  "Combinations of nemeses for tests"
  [[]
   [:pause]
   [:partition]
   [:pause :partition]
   [:kill]])

(def special-nemeses
  "A map of special nemesis names to collections of faults"
  {:none []
   :all  [:pause :partition :kill]})

(defn parse-nemesis-spec
  "Takes a comma-separated nemesis string and returns a collection of keyword
  faults."
  [spec]
  (->> (str/split spec #",")
       (map keyword)
       (mapcat #(get special-nemeses % [%]))))

(defn parse-nodes-spec
  "Takes a comma-separated nodes string and returns a set of node names."
  [spec]
  (->> (str/split spec #",")
       (map str/trim)
       (into #{})))

(def short-consistency-name
  "A map of consistency model names to a short name."
  {:strong-session-consistent-view "ss-consistent-view"})

(defn causal-test
  "Given options from the CLI, constructs a test map."
  [{:keys [nodes] :as opts}]
  (let [workload-name (:workload opts)
        workload ((workloads workload-name) opts)
        db       (sqlite3/db)
        nemesis  (nemesis/nemesis-package
                  {:db         db
                   :nodes      (:nodes opts)
                   :faults     (:nemesis opts)
                   :partition  {:targets [:one :minority-third :majority]}
                   :pause      {:targets [:one :minority :majority :all]}
                   :kill       {:targets (->> (repeatedly 10 (fn []
                                                               (->> nodes
                                                                    shuffle
                                                                    (take 2)
                                                                    sort
                                                                    (into []))))
                                              (into []))}
                   :packet     {:targets   [:one :minority :majority :all]
                                :behaviors [{:delay {}}]}
                   :clock      {:targets (->> (repeatedly 3 (fn []
                                                              (->> nodes
                                                                   shuffle
                                                                   (take 3)
                                                                   sort
                                                                   (into []))))
                                              (into []))}
                   :stop-start {:targets [:minority-third]}
                   :reset-db   {:targets [:minority-third]}
                   :interval   (:nemesis-interval opts)})]
    (merge tests/noop-test
           opts
           {:name (str "Electric"
                       " " (name workload-name)
                       " " (str/join "," (->> (:consistency-models opts)
                                              (map #(short-consistency-name % (name %)))))
                       " " (str/join "," (map name (:nemesis opts))))
            :os debian/os
            :db db
            :checker (checker/compose
                      {:perf               (checker/perf
                                            {:nemeses (:perf nemesis)})
                       :timeline           (timeline/html)
                       :stats              (checker/stats)
                       :exceptions         (checker/unhandled-exceptions)
                       :clock              (checker/clock-plot)
                       :logs-client        (checker/log-file-pattern #"SatelliteError" sqlite3/log-file-short)
                       :workload           (:checker workload)})
            :client    (:client workload)
            :nemesis   (:nemesis nemesis)
            :generator (gen/phases
                        (gen/log "Workload with nemesis")
                        (->> (:generator workload)
                             (gen/stagger    (/ (:rate opts)))
                             (gen/nemesis    (:generator nemesis))
                             (gen/time-limit (:time-limit opts)))

                        (gen/log "Final nemesis")
                        (gen/nemesis (:final-generator nemesis))

                        (gen/log "Final workload")
                        (:final-generator workload))})))

(def cli-opts
  "Command line options"
  [[nil "--better-sqlite3-nodes NODES" "A comma-separated list of nodes that should get better-sqlite3 clients"
    :parse-fn parse-nodes-spec]

   [nil "--consistency-models MODELS" "What consistency models to check for."
    :parse-fn parse-nemesis-spec
    :validate [(partial every? cm/all-models)
               (str "Must be one or more of " cm/all-models)]]

   [nil "--electricsql-nodes NODES" "A comma-separated list of nodes that should get ElectricSQL clients"
    :parse-fn parse-nodes-spec]

   [nil "--key-count NUM" "Number of keys in active rotation."
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--key-dist DISTRIBUTION" "Exponential or uniform."
    :parse-fn keyword
    :validate [#{:exponential :uniform} "Must be exponential or uniform."]]

   [nil "--max-txn-length NUM" "Maximum number of operations in a transaction."
    :default  4
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--max-writes-per-key NUM" "Maximum number of writes to any given key."
    :default  256
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]

   [nil "--min-txn-length NUM" "Minimum number of operations in a transaction."
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--nemesis FAULTS" "A comma-separated list of nemesis faults to enable"
    :parse-fn parse-nemesis-spec
    :validate [(partial every? #{:pause :partition :kill :clock :stop-start :reset-db})
               "Faults must be partition, pause, kill, clock, stop-start, or reset-db, or the special faults all or none."]]

   [nil "--nemesis-interval SECS" "Roughly how long between nemesis operations."
    :default 5
    :parse-fn read-string
    :validate [pos? "Must be a positive number."]]

   [nil "--postgresql-nodes NODES" "A comma-separated list of nodes that should get PostgreSQL clients"
    :parse-fn parse-nodes-spec]

   ["-r" "--rate HZ" "Approximate request rate, in hz"
    :default 100
    :parse-fn read-string
    :validate [pos? "Must be a positive number."]]

   [nil "--sqlite3-cli-nodes NODES" "A comma-separated list of nodes that should get SQLite3 CLI clients"
    :parse-fn parse-nodes-spec]

   ["-w" "--workload NAME" "What workload should we run?"
    :default  :gset
    :parse-fn keyword
    :missing  (str "Must specify a workload: " (cli/one-of workloads))
    :validate [workloads (cli/one-of workloads)]]])

(defn all-tests
  "Turns CLI options into a sequence of tests."
  [opts]
  (let [nemeses   (if-let [n (:nemesis opts)] [n] all-nemeses)
        workloads (if-let [w (:workload opts)] [w] all-workloads)]
    (for [n nemeses, w workloads, _i (range (:test-count opts))]
      (causal-test (assoc opts :nemesis n :workload w)))))

(defn opt-fn
  "Transforms CLI options before execution."
  [parsed]
  parsed)

(defn -main
  "CLI.
   
   `lein run` to list commands."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  causal-test
                                         :opt-spec cli-opts
                                         :opt-fn   opt-fn})
                   (cli/test-all-cmd {:tests-fn all-tests
                                      :opt-spec cli-opts
                                      :opt-fn   opt-fn})
                   (cli/serve-cmd))
            args))
