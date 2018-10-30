(defproject fyte-models "0.1.0-SNAPSHOT"
  :description "Fyte ml models"
  :url "https://github.com/nfcharles/clj-fyte-models.git"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/data.json "0.2.6"]
		 [org.clojure/core.async "0.2.374"]
                 [http-kit "2.1.18"]
                 [enlive "1.1.6"]
                 #_[net.mikera/core.matrix "0.20.0"]
                 #_[clatrix "0.3.0"]
                 [incanter "1.9.2"]
                 [gorillalabs/sparkling "2.1.3"]
                 [org.apache.spark/spark-core_2.11 "2.1.2"]
                 [org.apache.spark/spark-sql_2.11 "2.1.2"]
                 [org.apache.hadoop/hadoop-aws "2.8.3"]
                 [com.fasterxml.jackson.core/jackson-core "2.6.5"]
                 [com.fasterxml.jackson.core/jackson-annotations "2.6.5"]
                 [com.taoensso/timbre "4.10.0"]
                 [clj-time "0.13.0"]]
  :main ^:skip-aot fyte-models.scraper.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})


; 0.61.0 latest
; 0.5.0  latest
; 1.5.7  latest
