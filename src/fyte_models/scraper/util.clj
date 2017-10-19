(ns fyte-models.scraper.util
  (:require [clojure.pprint :as pprint]
            [clojure.java.shell :as shell]
	    [clojure.data.json :as json]
            [org.httpkit.client :as http]
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

(defn parse-result [err-out]
  (let [[status msg] (rest (re-find #"< HTTP/1.1 ([0-9][0-9][0-9]) (.*)" err-out))]
    [(read-string status) msg]))

(defn fetch [url]
  (let [ret (shell/sh "curl" "-v" url)
        std-err (ret :err)
        std-out (ret :out)
        exit    (ret :exit)]
    (if (not= exit 0)
      (throw (java.lang.Exception. (format "curl exited with status: %d" exit)))
      (let [[code msg] (parse-result std-err)]
        (if (= code 200)
          (html/html-snippet std-out)
	  (let [err-prefix (int (/ code 100))]
	    (if (or (= err-prefix 4) (= err-prefix 5))
	      (throw (java.lang.Exception. (format "HTTP GET status is %d: %s" code msg)))
	      (do
	        (println (format ("HTTP GET status is %d: %s" code msg)))
		(html/html-snippet std-out)))))))))
