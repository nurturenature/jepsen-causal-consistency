(defproject causal "0.1.0-SNAPSHOT"
  :description "Causal Consistency Tests for Jepsen."
  :url "https://github.com/nurturenature/jepsen-causal-consistency"
  :license {:name "Apache License Version 2.0, January 2004"
            :url "http://www.apache.org/licenses/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.3.5-SNAPSHOT"]
                 ; [elle "0.2.0"]
                 [elle "0.2.1-SNAPSHOT"]
                 [spootnik/unilog "0.7.31"]]
  :jvm-opts ["-Djava.awt.headless=true"]
  :main causal.cli
  :repl-options {:init-ns causal.cli}
  :plugins [[lein-codox "0.10.8"]
            [lein-localrepo "0.5.4"]]
  :codox {:output-path "target/doc/"
          :source-uri "../../{filepath}#L{line}"
          :metadata {:doc/format :markdown}})
