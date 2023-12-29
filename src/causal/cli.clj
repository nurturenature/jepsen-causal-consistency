(ns causal.cli
  "Command-line entry point for ElectricSQL tests."
  (:require [causal
             [electricsql :as electricsql]
             [lww-register :as lww]
             [postgresql :as postgresql]
             [sqlite3 :as sqlite3]]
            [clojure [string :as str]]
            [clojure.tools.logging :refer [info warn]]
            [elle.consistency-model :as cm]
            [jepsen
             [checker :as checker]
             [client :as client]
             [cli :as cli]
             [control :as c]
             [generator :as gen]
             [nemesis :as nemesis]
             [os :as os]
             [tests :as tests]
             [util :as u]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.nemesis.combined :as nc]
            [jepsen.os.debian :as debian]))

(def workloads
  "A map of workload names to functions that take CLI options and return
  workload maps."
  {:lww-register lww/workload
   :none         (fn [_] tests/noop-test)})

(def all-workloads
  "A collection of workloads we run by default."
  [])

(def all-nemeses
  "Combinations of nemeses for tests"
  [[]])

(def special-nemeses
  "A map of special nemesis names to collections of faults"
  {:none []
   :all  [:pause :kill :partition :clock]})

(defn parse-nemesis-spec
  "Takes a comma-separated nemesis string and returns a collection of keyword
  faults."
  [spec]
  (->> (str/split spec #",")
       (map keyword)
       (mapcat #(get special-nemeses % [%]))))

(defn causal-test
  "Given options from the CLI, constructs a test map."
  [opts]
  (let [workload-name (:workload opts)
        workload ((workloads workload-name) opts)
        db       (sqlite3/db)
        nemesis  (nc/nemesis-package
                  {:db db
                   :nodes (:nodes opts)
                   :faults (:nemesis opts)
                   :partition {:targets [:one :majority]}
                   :pause {:targets [:one]}
                   :kill  {:targets [:one :all]}
                   :interval (:nemesis-interval opts)})]
    (merge tests/noop-test
           opts
           {:name (str "ElectricSQL"
                       " " (name workload-name)
                       " " (str/join "," (map name (:consistency-models opts)))
                       (str/join "," (map name (:nemesis opts))))
            :os debian/os
            :db db
            :checker (checker/compose
                      {:perf (checker/perf
                              {:nemeses (:perf nemesis)})
                       :clock (checker/clock-plot)
                       :stats (checker/stats)
                       :exceptions (checker/unhandled-exceptions)
                       :timeline (timeline/html)
                       :workload (:checker workload)})
            :client    (:client workload)
            :nemesis   (:nemesis nemesis)
            :generator (->> (:generator workload)
                            (gen/stagger    (/ (:rate opts)))
                            (gen/nemesis    (:generator nemesis))
                            (gen/time-limit (:time-limit opts)))})))

(def cli-opts
  "Command line options"
  [[nil "--consistency-models MODELS" "What consistency models to check for."
    :default [:strong-session-consistent-view]
    :parse-fn parse-nemesis-spec
    :validate [(partial every? cm/all-models)
               (str "Must be one or more of " cm/all-models)]]

   [nil "--key-count NUM" "Number of keys in active rotation."
    :default  10
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--nemesis FAULTS" "A comma-separated list of nemesis faults to enable"
    :parse-fn parse-nemesis-spec
    :validate [(partial every? #{:pause :kill :partition :clock})
               "Faults must be pause, kill, partition, or clock, or the special faults all or none."]]

   [nil "--max-txn-length NUM" "Maximum number of operations in a transaction."
    :default  4
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--max-writes-per-key NUM" "Maximum number of writes to any given key."
    :default  256
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]

   [nil "--nemesis-interval SECS" "Roughly how long between nemesis operations."
    :default 5
    :parse-fn read-string
    :validate [pos? "Must be a positive number."]]

   ["-r" "--rate HZ" "Approximate request rate, in hz"
    :default 100
    :parse-fn read-string
    :validate [pos? "Must be a positive number."]]

   ["-w" "--workload NAME" "What workload should we run?"
    :default  :lww-register
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
                   (cli/serve-cmd)
                   postgresql/command
                   electricsql/command)
            args))
