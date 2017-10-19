(ns fyte-models.scraper.core
  (:require [fyte-models.scraper.dataset :as fyte-dataset])
  (:gen-class))


(def endpoint "http://www.fightmetric.com/statistics/fighters")

(defn -main
 [& args]
 (try
   (let [concurrency (read-string (nth args 0))
         prefix (nth args 1)
         n (read-string (nth args 2))]
     (println (format "CONCURRENCY=%d" concurrency))
     (println (format "PREFIX=%s" prefix))
     (doseq [xs (take n (partition 2 (range 97 123)))]
       (let [[a b] xs
             filename (format "%s_%s-%s.json" prefix (char a) (char b))]
         (fyte-dataset/gen-dataset filename concurrency endpoint xs))))
   (finally
     (shutdown-agents))))
