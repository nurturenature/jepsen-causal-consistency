(defproject causal "0.1.0-SNAPSHOT"
  :description "Causal Consistency Tests for Jepsen."
  :url "https://github.com/nurturenature/jepsen-causal-consistency"
  :license {:name "Apache License Version 2.0, January 2004"
            :url "http://www.apache.org/licenses/"}
  :dependencies [[org.clojure/clojure "1.12.0"]
                 [jepsen "0.3.7"]
                 ;; TODO: PR elle or revert
                 ;; [elle "0.2.2-SNAPSHOT"]
                 ;; TODO: PR history.sim or revert
                 ;; [io.jepsen/history.sim "0.1.1-SNAPSHOT"]
                 [io.jepsen/history.sim "0.1.0"]]
  :jvm-opts ["-Xmx8g"
             "-Djava.awt.headless=true"
             "-server"]
  :main causal.cli
  :repl-options {:init-ns causal.repl}
  :plugins [[lein-codox "0.10.8"]
            [lein-localrepo "0.5.4"]]
  :codox {:output-path "target/doc/"
          :source-uri "../../{filepath}#L{line}"
          :metadata {:doc/format :markdown}})
