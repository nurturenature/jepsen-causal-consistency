(defproject causal "0.1.0-SNAPSHOT"
  :description "Causal Consistency Tests for Jepsen."
  :url "https://github.com/nurturenature/jepsen-causal-consistency"
  :license {:name "Apache License Version 2.0, January 2004"
            :url "http://www.apache.org/licenses/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.3.6-SNAPSHOT"]
                 [elle "0.2.2-SNAPSHOT"]
                 [cheshire "5.12.0"]
                 [clj-http "3.12.3"]
                 [com.github.seancorfield/next.jdbc "1.3.909"]
                 [org.postgresql/postgresql "42.7.1"]]
  :jvm-opts ["-Djava.awt.headless=true"]
  :main causal.cli
  :repl-options {:init-ns causal.lww-list-append.checker.causal-consistency}
  :plugins [[lein-codox "0.10.8"]
            [lein-localrepo "0.5.4"]]
  :codox {:output-path "target/doc/"
          :source-uri "../../{filepath}#L{line}"
          :metadata {:doc/format :markdown}})
