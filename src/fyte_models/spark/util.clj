(ns fyte-models.spark.util
  "Spark utility and helper functions."
  (:require [clojure.string :as string]
            [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [sparkling.sql  :as sql]	    
            [sparkling.destructuring :as s-de]
            [sparkling.serialization]
            [sparkling.function :as func]
            [taoensso.timbre :as timbre :refer [infof]])
  (:import [org.apache.spark.sql RowFactory Row]
           [org.apache.spark.sql.types StructType
                                       StructField
                                       DataTypes
                                       DataType])
  (:gen-class))


;; ------------
;; -  Schema  -
;; ------------

(defn ^DataType data-type
  "DataType factory function."
  [-type]
  (condp = -type
    :string    (DataTypes/StringType)
    :short     (DataTypes/ShortType)
    :int       (DataTypes/IntegerType)
    :long      (DataTypes/LongType)
    :float     (DataTypes/FloatType)
    :double    (DataTypes/DoubleType)
    :bool      (DataTypes/BooleanType)
    :byte      (DataTypes/ByteType)
    :date      (DataTypes/DateType)
    :timestamp (DataTypes/TimestampType)
    (throw (java.lang.Exception. (format "Unknown type: %s!" -type)))))

(defn ^StructType struct-type
  "Builds StructType from columns and column types."
  [col-types]
  (loop [xs (seq col-types)
         acc []]
    (if-let [x (first xs)]
      (let [[col dtype] x]
        (infof "Adding <%s>%s to struct" dtype col)
        (recur (rest xs) (conj acc (DataTypes/createStructField col (data-type dtype) true))))
      (DataTypes/createStructType acc))))


;; -------------
;; -   UDFs    -
;; -------------


;; ------------
;; -   RDDs   -
;; ------------


(defn select-cols
  "Select columns"
  [idxs parsers row]
  (loop [xs (seq idxs)
         acc {}]
    (if-let [col_idx (first xs)]
      (let [[col idx] col_idx]
        (recur (rest xs) (assoc acc (keyword col) ((parsers col) (nth row idx)))))
      acc)))

(defn rdd->df
  "Spread RDD to Dataframe"
  [sql-ctx rdd schema]
  (infof "SOURCE_RDD=%s" (type rdd))
  (let [row (fn [xs]
              (RowFactory/create (object-array xs)))]
    (->> rdd
         (spark/map row)
         (sql/rdd->data-frame sql-ctx (struct-type schema)))))
