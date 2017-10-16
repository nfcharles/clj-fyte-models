(ns fyte-models.scraper.dataset
  (:require [clojure.pprint :as pprint]
            [clojure.java.shell :as shell]
            [clojure.data.json :as json]
            [clojure.core.async :as async]
            [org.httpkit.client :as http]
            [fyte-models.scraper.details :as fyte-details]
            [fyte-models.scraper.core :as fyte-core]	    
            [net.cgrand.enlive-html :as html])
  (import [java.net URL]
          [java.util.concurrent Executors ExecutorCompletionService])
  (:gen-class))


(defmacro with-pool [bindings & body]
  "(with-pool [pool (...)]
      (let [completion-service (... pool)]
        ))"
  (assert (vector? bindings) "vector required for bindings")
  (assert (== (count bindings) 2) "one binding pair expected")
  (assert (symbol? (bindings 0)) "only symbol allowed in binding")
  `(let ~(subvec bindings 0 2)
     (try
       ~@body
       (finally
         (.shutdown ~(bindings 0))))))

(def endpoint "http://www.fightmetric.com/statistics/fighters")

(defn gen-url [base suffix]
  (let [url (format "%s?char=%s&page=all" base suffix)]
    (println (format "Generating %s" url))
    url))

(defn links [dom]
  (doall (map #(first (html/attr-values % :href))
              (html/select dom [:tr.b-statistics__table-row html/first-child :a]))))

(defn elements [endpoint c]
  (let [dom (fyte-core/fetch (gen-url endpoint c))]
    ;(println dom)
    (map vector (links dom)
                (partition 11 (html/select dom [:tr.b-statistics__table-row :td.b-statistics__table-col])))))

(defn task [row]
  (try
    (let [[link elems] row
          dom (fyte-core/fetch link)
          names (fyte-core/names (take 3 elems))
          basic (fyte-core/basic-info (take-last 8 elems))
          matches (fyte-details/matches dom)]
      (when (or (= matches []) (nil? matches))
        (println (format ">>>>> LINK <<<<< %s" link))
        (pprint/pprint names))
      (merge names basic {"matches" matches}))
    (catch Exception e
      (println (format "TASK EXCEPTION: %s" e)))))

(defn assert-noerr [_]
  (if (nil? _) (System/exit 99) _))

(defn dequeue [csvc n]
  (loop [i 0
         acc []]
    (if (< i n)
      (->> (.take csvc)
           (.get)
           (assert-noerr)
           (conj acc)
           (recur (inc i)))
      acc)))

(defn fetch-details [concurrency in-ch]
  (let [out-ch (async/chan)]
    (async/thread
      (println "Staring fetch-details thread")
      (with-pool [pool (Executors/newFixedThreadPool concurrency)]
        (let [csvc (ExecutorCompletionService. pool)]
          (loop []
            (if-let [rows (async/<!! in-ch)]
	      (let [n (count rows)]
                ;; Submit tasks for completion
                (dotimes [i n]
                  (.submit csvc #(task (nth rows i))))
                (async/>!! out-ch (dequeue csvc n))
	        (recur))
	      (do
	        (async/close! out-ch)))))))
    out-ch))

(defn fetch-basic [base char-range]
  (let [out-ch (async/chan 27)]
    (async/thread
      (println "Starting fetch-basic thread")
      (loop [cr char-range]
        (if-let [cint (first cr)]
	  (do
	    (async/>!! out-ch (elements base (char cint)))
	    (recur (rest cr)))
	  (async/close! out-ch))))
    out-ch))
        
(defn write [name payload]
  (let [f (format "/tmp/%s" name)]
    (println (format "Writing %s" f))
    (spit f (json/write-str payload))))

(defn gen-dataset [filename concurrency base]
  (println "Starting main thread")
  (let [out-ch (->> (fetch-basic base (range 97 100))
                    (fetch-details concurrency))]
    (loop [total 0
           acc []]
      (if-let [res (async/<!! out-ch)]
        (let [n (count res)]
	  (println (format "Received %d records" n))
          (recur (+ total n) (conj acc res)))
	(do
	  (println (format "Processed %d total records" total))
	  (write filename (flatten acc)))))))


;;; TEST

(defn -main
 [& args]
 (try
   (let [concurrency (read-string (nth args 0))
         filename (nth args 1)]
     (println (format "CONCURRENCY=%d" concurrency))
     (println (format "FILENAME=%s" filename))
     (gen-dataset filename concurrency endpoint))
   (finally
     (shutdown-agents))))

	  
