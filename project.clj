(defproject crdt "0.1.0-SNAPSHOT"
  :description "CRDT Tests for Jepsen."
  :url "https://github.com/nurturenature/crdt-jepsen"
  :license {:name "Apache License Version 2.0, January 2004"
            :url "http://www.apache.org/licenses/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [elle "0.2.0"]
                 ; [elle "0.2.1-SNAPSHOT"]
                 [spootnik/unilog "0.7.31"]]
  :repl-options {:init-ns crdt.lww-register}
  :plugins [[lein-codox "0.10.8"]
            [lein-localrepo "0.5.4"]]
  :codox {:output-path "target/doc/"
          :source-uri "../../{filepath}#L{line}"
          :metadata {:doc/format :markdown}})
