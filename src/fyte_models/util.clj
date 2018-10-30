(ns fyte-models.util
  (require [clojure.string :as string]
           [clj-time.core :as t]
           [clj-time.coerce :as c]
           [clj-time.format :as f]
           [clojure.data.json :as json])
  (:gen-class))


;; -------------
;; -  Readers  -
;; -------------

(defn parse
  "Parse input file to json object"
  [path]
  (println (format "Reading %s..." path))
  (let [xs (json/read-str (slurp path) :key-fn keyword)]
    #_(pp/pprint (first xs))
    xs))

(defn mparse
  "Parse input file to json object"
  [& paths]
  (loop [xs paths
         acc []]
    (if-let [path (first xs)]
      (do
        (println (format "Reading %s..." path))
        (let [ret (json/read-str (slurp path) :key-fn keyword)]
          #_(pp/pprint (first xs))
          (recur (rest xs) (conj acc ret))))
      (flatten acc))))

;; ----------------
;; -  Date Utils  -
;; ----------------

;;;  format: "Apr. 13, 2013"

(def month->int
  (hash-map
    "Jan." "01"
    "Feb." "02"
    "Mar." "03"
    "Apr." "04"
    "May." "05"
    "Jun." "06"
    "Jul." "07"
    "Aug." "08"
    "Sep." "09"
    "Oct." "10"
    "Nov." "11"
    "Dec." "12"))


(defn raw->iso
  "Converts raw date to iso 8601"
  [dt]
  (let [[m d y] (string/split dt #"\s")]
    #_(println (format "M=%s, D=%s, Y=%s" m d y))
    (format "%s-%s-%s" y (month->int m) (string/replace d #"[,]" ""))))

(defn iso->timestamp
  "Converts iso to timestamp"
  [dt]
  (let [frmt (f/formatter "yyyy-MM-dd")]
    (c/to-long (f/parse frmt dt))))

(defn raw->timestamp
  [dt]
  (->> dt
    (raw->iso)
    (iso->timestamp)))

;; -------------------
;; -  Measure Utils  -
;; -------------------

;;; format: "5' 10\""

(defn raw->inch
  "Converts raw measure to inches"
  [s]
  (let [[ft in] (map read-string (rest (re-find #"^(\d)\'\s*(\d+).*$" s)))]
    (+ (* 12 ft) in)))

;; "rnd": 3,
;; "tme": "3:01"


(defn raw->duration
  "Returns duration in seconds"
  [rnd time]
  (let [[m s] (map read-string (rest (re-find #"^(\d*):0?(\d*)$" time)))]
    (+ (* (- rnd 1) 300) (+ (* m 60) s))))

;; ----------
;; -  Test  -
;; ----------

(defn parse-date
  [date]
  (println (format "DATE=%s" date))
  (println (raw->iso date))
  (println (raw->timestamp date)))

(defn parse-measure
  [height]
  (println (format "HEIGHT=%s" height))
  (println (raw->inch height)))

(defn parse-duration
  [rnd minute]
  (println (format "ROUND=%s, MINUTE=%s" rnd minute))
  (println (raw->duration rnd minute)))

(defn -main
  [& args]
  (let [date   (nth args 0)
        height (nth args 1)
        rnd    (read-string (nth args 2))
        minute (nth args 3)]
    (println args)
    (parse-date date)
    (parse-measure height)
    (parse-duration rnd minute)))
