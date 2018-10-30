(ns fyte-models.spark.configuration
  (:require [clojure.string :as string]
            [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [sparkling.sql  :as sql]	    
            [sparkling.destructuring :as s-de]
            [sparkling.serialization]
            [taoensso.timbre :as timbre :refer [info warn error fatal infof warnf errorf debugf]])
  (:import [org.apache.spark.api.java JavaSparkContext])
  (:gen-class))


(defn get-env [k]
  (System/getenv k))

(defn set-hadoop [ctx k v]
  (infof "setting key[%s]" k)
  (.set (.hadoopConfiguration ctx) k v))

(defn configure-common [ctx]
  (set-hadoop ctx "fs.s3a.access.key" (get-env "AWS_ACCESS_KEY_ID"))
  (set-hadoop ctx "fs.s3a.secret.key" (get-env "AWS_SECRET_ACCESS_KEY")))  

(defn configure-from-role [ctx]
  (configure-common ctx)
  (set-hadoop ctx "fs.s3a.aws.credentials.provider" "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
  (set-hadoop ctx "fs.s3a.session.token" (get-env "AWS_SESSION_TOKEN")))

(defn configure [ctx]
  (configure-common ctx))

(defn set-spark-conf
  "Set spark configuration"
  [^org.apache.spark.SparkConf conf & kv]
  (info "Configuring spark...")
  (doseq [[k v] kv]
    (infof "SET %s=%s" k v)
    (.set conf (str k) (str v)))
  (info "Done")
  conf)

(def cores (.availableProcessors (java.lang.Runtime/getRuntime)))

;; TODO: Allow configuration of spark conf
(defn ^JavaSparkContext spark-context [& {:keys [app-name master]
                                          :or {app-name "fyte-models"
                                               master   "local[*]"}}]
  (infof "SPARK_MASTER=%s" master)
  (infof "SPARK_APP_NAME=%s" app-name)
  (let [ctx (-> (set-spark-conf
                  (conf/spark-conf)
                  ["spark.sql.shuffle.partitions" cores])
                (conf/master master)
                (conf/app-name app-name)
                spark/spark-context)]
    (if (= "role" (get-env "AUTH_TYPE"))
      (configure-from-role ctx)
      (configure ctx))
    ctx))

(defn ^org.apache.spark.sql.SQLContext sql-context [^JavaSparkContext sc]
  (sql/sql-context sc))
