(ns fyte-models.scraper.dataset
  (:require [clojure.pprint :as pprint]
            [clojure.java.shell :as shell]
            [clojure.data.json :as json]
            [clojure.core.async :as async]
            [org.httpkit.client :as http]
            [fyte-models.scraper.details :as fyte-details]
            [fyte-models.scraper.util :as fyte-util]
            [fyte-models.macro :refer [with-pool]]
            [net.cgrand.enlive-html :as html])
  (import [java.net URL]
          [java.util.concurrent Executors ExecutorCompletionService])
  (:gen-class))


(defn names [id dom]
  (loop [source (map html/text (html/select dom [:a]))
         labels ["first" "last" "alias"]
         acc {"id" id}]
    (if-let [src (first source)]
      (recur (rest source) (rest labels) (assoc acc (first labels) src))
      acc)))

(defn basic-info [dom]
  (loop [source (map #(fyte-util/deser (html/text %)) dom)
         labels ["ht" "wt" "reach" "stance" "w" "l" "d" "belt"]
         acc {}]
    (if-let [src (first source)]
      (recur (rest source) (rest labels) (assoc acc (first labels) src))
      acc)))

(defn gen-url [base suffix]
  (let [url (format "%s?char=%s&page=all" base suffix)]
    (println (format "Generating %s" url))
    url))

(defn links [dom]
  (doall (map #(first (html/attr-values % :href))
              (html/select dom [:tr.b-statistics__table-row html/first-child :a]))))

(defn elements [endpoint c]
  (let [dom (fyte-util/fetch (gen-url endpoint c))]
    (map vector (links dom)
                (partition 11 (html/select dom [:tr.b-statistics__table-row :td.b-statistics__table-col])))))

(defn task [row]
  (try
    (let [[link elems] row
          id  (last (re-find #"^.*/(\w+)$" link))
          dom (fyte-util/fetch link)
          names (names id (take 3 elems))
          basic (basic-info (take-last 8 elems))
          matches (fyte-details/matches dom)]
      (when (or (= matches []) (nil? matches))
        (println (format "LINK[%s]" link)))
      (merge names basic {"matches" matches}))
    (catch Exception e
      (println (format "TASK EXCEPTION: %s" e)))))

(defn task [row]
  (let [[link elems] row]
    (try
      (let [id  (last (re-find #"^.*/(\w+)$" link))
           dom (fyte-util/fetch link)
           names (names id (take 3 elems))
           basic (basic-info (take-last 8 elems))
           matches (fyte-details/matches dom)]
        (when (or (= matches []) (nil? matches))
          (println (format "LINK[%s]" link)))
        (merge names basic {"matches" matches}))
      (catch Exception e
        (println (format "Error processing link[%s]: %s" link e))))))

(defn assert-noerr [_]
  (if (nil? _) (System/exit 99) _))

(def assert-noerr identity)

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
        
(defn write [path payload]
  (println (format "Writing %s" path))
  (spit path (json/write-str payload)))

(defn gen-dataset [filename concurrency base qargs]
  (println "Starting main thread")
  (let [out-ch (->> (fetch-basic base qargs)
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

;;; RUNNER

(def endpoint "http://www.fightmetric.com/statistics/fighters")

(defn parse-qargs [in]
  (let [[a b] (doall (map read-string (clojure.string/split in #",")))]
    (range a (inc b))))

(defn -main
 [& args]
 (try
   (let [concurrency (read-string (nth args 0))
         filename (nth args 1)
         qargs (parse-qargs (nth args 2))]
     (println (format "CONCURRENCY=%d" concurrency))
     (println (format "FILENAME=%s" filename))
     (println (format "QARGS=%s" qargs))
     (gen-dataset filename concurrency endpoint qargs))
   (finally
     (shutdown-agents))))
