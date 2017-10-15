(ns fyte-models.scraper.details
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
       
(defn w-l [dom]
  (->> (html/select dom [:i.b-flag__text])
       (first)
       (html/text)))

(defn status [dom]
  (->> (html/select dom [:i.b-flag__text])
       (first)
       (html/text)))


(defn ftr [dom]
  (->> (html/select dom [[:p (html/nth-of-type 2)] :a])
       (first)
       (html/text)
       (deser)))
       
(defn -metric [dom]
  (map #(->> %
             (html/text)
	     (deser)
	     (read-string))
       (html/select dom [:p])))

(defn stk [dom]
  (-metric dom))

(defn tdn [dom]
  (-metric dom))

(defn sub [dom]
  (-metric dom))

(defn pss [dom]
  (-metric dom))

(defn evt [dom]
  (->> (html/select dom [:p :a])
       (first)
       (html/text)
       (deser)))

(defn dte [dom]
  (->> (html/select dom [[:p (html/nth-of-type 2)]])
       (first)
       (html/text)
       (deser)))

(defn rz1 [dom]
  (->> (html/select dom [[:p (html/nth-of-type 1)]])
       (first)
       (html/text)
       (deser)))
       
(defn rz2 [dom]
  (->> (html/select dom [[:p (html/nth-of-type 2)]])
       (first)
       (html/text)
       (deser)))

(defn rnd [dom]
  (->> (html/select dom [:p])
       (first)
       (html/text)
       (deser)
       (read-string)))
       
(defn tme [dom]
  (->> (html/select dom [:p])
       (first)
       (html/text)
       (deser)))

(defn foo [coll]
  (hash-map
    "str"  (nth coll 0)
    "td"   (nth coll 1)
    "sub"  (nth coll 2)
    "pass" (nth coll 3)))

(defn metrics [& colls]
  (reduce #(conj %1 (foo %2)) [] colls))
  
(defn matches [dom]
  (loop [elems (html/select dom [:tr.b-fight-details__table-row :td.b-fight-details__table-col])
         acc []]
    (if (not= 0 (count elems))
      (let [stat (status (nth elems 0))]
        (if (= stat "next")
	  ;; upcoming fight rows only have nine <td> elements
	  (recur (drop 9 elems) acc)
          (let [row (take 10 elems)
	        [f1 f2] (map vector (stk (nth row 2))
		                    (tdn (nth row 3))
				    (sub (nth row 4))
				    (pss (nth row 5)))
	        coll {"res" stat
                      "ftr" (ftr (nth row 1))
                      "mtx" (metrics f1 f2)
                      "evt" (evt (nth row 6))
                      "dte" (dte (nth row 6))
                      "end" (rz1 (nth row 7))
                      "how" (rz2 (nth row 7))
                      "rnd" (rnd (nth row 8))
                      "tme" (tme (nth row 9))
	             }]
            (recur (drop 10 elems) (conj acc coll)))))
      acc)))
