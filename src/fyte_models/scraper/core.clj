(ns fyte-models.scraper.core
  (:require [fyte-models.scraper.dataset :as fyte-dataset])
  (:gen-class))


(def endpoint "http://www.fightmetric.com/statistics/fighters")

(defn parse-str [index args default]
  (if (> (count args) index)
    (nth args index)
    default))

(defn parse-num [index args default]
  (if (> (count args) index)
    (read-string (nth args index))
    default))


(defn -main
 [& args]
 (try
   (let [concurrency (parse-num 0 args 2)
         prefix      (parse-str 1 args "/tmp/profiles")
         n           (parse-num 2 args 13)
         offset      (parse-num 3 args 0)]
     (println (format "CONCURRENCY=%d" concurrency))
     (println (format "PREFIX=%s" prefix))
     (doseq [xs (take n (drop offset (partition 2 (range 97 123))))]
       (let [[a b] xs
             filename (format "%s_%s-%s.json" prefix (char a) (char b))]
	 (println (format "c=%d p=%s n=%d o=%d (%s %s)" concurrency prefix n offset (char a) (char b)))
	 (fyte-dataset/gen-dataset filename concurrency endpoint xs))))
   (finally
     (shutdown-agents))))
