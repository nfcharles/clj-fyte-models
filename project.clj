(defproject fyte-models "0.1.0-SNAPSHOT"
  :description "Fyte ml models"
  :url "https://github.com/nfcharles/clj-fyte-models.git"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/data.json "0.2.6"]
		 [org.clojure/core.async "0.2.374"]
                 [http-kit "2.1.18"]
                 [enlive "1.1.6"]]
  :main ^:skip-aot fyte-models.scraper.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
