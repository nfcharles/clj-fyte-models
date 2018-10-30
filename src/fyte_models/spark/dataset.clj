(ns fyte-models.spark.dataset
  (:require [clojure.pprint :as pp]
            [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [clojure.data.json :as json]
            [incanter.stats :as i.stats]
            [incanter.core :as i.core]
            [incanter.charts :as i.charts]
            [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [sparkling.sql  :as sql]
            [sparkling.serialization]
            [fyte-models.util :as fyte.util])
  (:gen-class))


;; -----------------
;; -  Match Index  -
;; -----------------

;; Raw -> xform 1
;;
;; idx.0 -> (hash-map)
;;   dt.0 -> (array-map)
;;     - idx
;;     - res
;;     - pass
;;     - str
;;     - sub
;;     - td
;;     - prior
;;     - (next)
;;   dt.1
;;     - ...
;; idx.1 ->
;;   dt.0 ->
;;     - ...
;;
;;
;;

(defn result
  "Encode nc/l/d/w"
  [match]
  (let [x (:res match)]
    (condp = x
      "nc"   0
      "loss" 1
      "draw" 2
      "win"  3
      (throw (java.lang.Exception. (format "Unknown result: %s, %s" x match))))))
      
(defn datetime
  [match]
  (if match
    (fyte.util/raw->iso (:dte match))))

(defn xform
  [match priors & {:keys [n]
                   :or {n 3}}]
  (let [dt  (datetime match)
        end (:end match)
        idx (:id match)
        sec (fyte.util/raw->duration (:rnd match) (:tme match))
        res (result match)
        xs  (if (> (count priors) 0) (take n priors) (list))]
    [dt (merge {"end" end "idx" idx "res" res "sec" sec "priors" xs} (first (:mtx match)))]))

(defn -head
  [pair]
  (first pair))

(defn -tail
  [pair]
  (last pair))

(defn result-seq
  [records]
  (loop [xs records
         acc []]
    (if-let [x (first xs)]
      (recur (rest xs) (conj acc (result x)))
      acc)))

(defn initial-record
  "Determine initial win/loss ratio before recordings"
  [record]
  {:loss (read-string (:l record))
   :draw (read-string (:d record))
   :win  (read-string (:w record))})

(defn update-rec
  [prior-rec current]
  (let [{:keys [loss draw win]} prior-rec]
    (condp = (:res current)
      "loss" {:loss (dec loss) :draw draw :win win}
      "draw" {:loss loss :draw (dec draw) :win win}
      "win"  {:loss loss :draw draw :win (dec win)}
      "nc"   prior-rec
      (throw (java.lang.Exception "Fuck")))))
  
(defn build-inner-index
  [record n]
  (loop [xs (partition 2 1 (list) (:matches record))
         prior-res (result-seq (:matches record))
         prior-rec (initial-record record)
         p0 (list)
         acc {}]
    (if-let [p1 (first xs)]
      (let [[current tail] p1
            head-id (datetime (-head p0))
            tail-id (datetime tail)
            [dt ret] (xform current (rest prior-res) :n 5)]
        (recur (rest xs)
               (rest prior-res)
               (update-rec prior-rec current)
               p1
               (assoc acc dt (merge ret {:head head-id :tail tail-id :prior-rec (update-rec prior-rec current)}))))
      acc)))

(defn build-index
  [records n]
  (loop [xs records
         acc {}]
    (if-let [x (first xs)]
      (let [id  (:id x)
            res (build-inner-index x n)]
        (recur (rest xs) (assoc acc id res)))
      acc)))

(defn run
  [records filename n]
  (let [ret (build-index records n)]
    (println (format "Writing file %s" filename))
    (spit filename (json/write-str ret))))


;; ----------
;; -  Main  -
;; ----------

(defn -main
  [& args]
  (let [filename (nth args 0)
        n (read-string (nth args 1))
        xs (apply fyte.util/mparse (drop 2 args))]
    (run xs filename n)))
