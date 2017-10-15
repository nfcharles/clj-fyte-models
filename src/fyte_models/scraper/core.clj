(ns fyte-models.scraper.core
  (:require [clojure.pprint :as pprint]
            [clojure.java.shell :as shell]
	    [clojure.data.json :as json]
	    [clojure.core.async :as async]
            [org.httpkit.client :as http]
	    [fyte-models.scraper.details :as fyte-details]
            [net.cgrand.enlive-html :as html])
  (import [java.net URL])
  (:gen-class))


(defn deser [s]
  (->> (clojure.string/replace s #"\n" "")
       (clojure.string/trim)))
       
(defn fetch [url]
  (html/html-resource (URL. url)))

(defn fetch [url]
  (html/html-snippet (:body @(http/get url {:insecure? true}))))

(defn parse-result []
  (let [ok  "< Status: 200 OK"
        tmp "< HTTP/1.1 ([\d]{3}) (.*)"]
    


(defn fetch [url]
  (let [ret (shell/sh "curl" "-v" url)]
    (println (format "EXIT: %s" (ret :exit)))
    (println (format "ERROR: %s" (ret :err)))
    ;; Search For 404 Not Found and warn
    (html/html-snippet (:out ret))))

(defn names [dom]
  (loop [source (map html/text (html/select dom [:a]))
         labels ["first" "last" "alias"]
         acc {}]
    (if-let [src (first source)]
      (recur (rest source) (rest labels) (assoc acc (first labels) src))
      acc)))

(defn basic-info [dom]
  (loop [source (map #(deser (html/text %)) dom)
         labels ["ht" "wt" "reach" "stance" "w" "l" "d" "belt"]
	 acc {}]
    (if-let [src (first source)]
      (recur (rest source) (rest labels) (assoc acc (first labels) src))
      acc)))

(defn details-link [dom]
  (first (html/attr-values (first (html/select dom [:a])) :href)))

(defn matches [dom]
  {"matches" (fyte-details/matches (fetch (details-link dom)))})

(defn all-profiles [dom]
  (loop [grps (partition 11 (html/select dom [:tr.b-statistics__table-row :td.b-statistics__table-col]))
         i 0
	 acc []]
    (if-let [grp (first grps)]
      (recur (rest grps)
             (inc i)
             (conj acc
             (merge (names (take 3 grp))
                    (basic-info (take-last 8 grp))
                    (matches (first grp)))))
      acc)))

(defn write [name payload]
  (let [f (format "/tmp/%s" name)]
    (println (format "Writing %s" f))
    (spit f (json/write-str payload))))

(defn process [url name]
  (let [dom (fetch url)]
    (write name (all-profiles dom))))

(defn gen-url [base suffix]
  (let [url (format "%s?char=%s&page=all" base suffix)]
    (println (format "Generating %s" url))
    url))

(def endpoint "http://www.fightmetric.com/statistics/fighters")

(defn gen-dataset [base filename concurrency]
  (let [in (async/chan 27)
        out (async/chan)
        source (range 97 100) ;123
	total (atom 0)
        res (atom [])
	active-thrds (atom concurrency)]
    ;; fill queue w/ pages to featch
    (loop [src (map char source)]
      (if-let [c (first src)]
        (do
	  (println (format "Adding %s to input channel" c))
	  (async/>!! in (gen-url base c))
	  (recur (rest src)))
	(async/close! in)))
	  
    (dotimes [i concurrency]
      (async/thread
        (println (format "Starting thread-%d" i))
        (loop [acc []]
          (if-let [url (async/<!! in)]
            (let [ret (all-profiles (fetch url))]
              (println (format "thread-%d: Retrieved %d records" i (count ret)))
              (swap! total + (count ret))
              (recur (conj acc ret)))
	    (do
	      (swap! res conj acc)
	      (swap! active-thrds dec)
	      (when (= @active-thrds 0)
		(let [final (flatten @res)]
		  (write filename final)
		  (async/>!! out @total)
		  (async/close! out))))))))
    out))


(defn -main
  [& args]
  (try
    (let [concurrency (read-string (nth args 0))
          filename (nth args 1)]
      (println (format "CONCURRENT=%d" concurrency))
      (println (format "FILENAME=%s" filename))
      (println (async/<!! (gen-dataset endpoint filename concurrency)))
      (comment (pprint/pprint (fyte-details/matches (fetch sample-url)))))
    (finally
      (shutdown-agents))))
