(defproject crdt "0.1.0-SNAPSHOT"
  :description "CRDT Tests for Jepsen."
  :url "https://github.com/nurturenature/crdt-jepsen"
  :license {:name "Apache License Version 2.0, January 2004"
            :url "http://www.apache.org/licenses/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.3.4"]]
  :repl-options {:init-ns crdt.util})
