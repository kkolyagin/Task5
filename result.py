from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
conf = SparkConf()
conf.setMaster("local").setAppName('My app')
conf.set("spark.jars","c:\Scripts\spark-3.3.1-bin-hadoop3\jars\postgresql-42.5.1.jar")
sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)
print('Запущен Spark версии', spark.version)

import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.window import Window

import datetime

spark = SparkSession.builder.master("local").\
                    appName("Word Count").\
                    config("spark.driver.bindAddress","localhost").\
                    config("spark.ui.port","4040").\
                    getOrCreate()

#### creds info
url = "jdbc:postgresql://localhost:5432/Test"
db_name = "public"
creds = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

df=spark.read\
  .format("jdbc")\
  .option("url", url)\
  .option("dbtable", "(SELECT date(tpep_pickup_datetime) as date, passenger_count, Total_amount FROM public.yellow_tripdata LIMIT 1000000) AS t")\
  .option("user", "postgres")\
  .option("password", "postgres")\
  .load()   

df\
    .fillna(value=0, subset=["passenger_count"])\
    .withColumn("passenger_type",\
              F.when(F.col("passenger_count") >= F.lit(4),4)\
              .otherwise(F.col("passenger_count")))\
    .withColumn("passenger_sum_byday",F.sum(F.col("passenger_count"))\
                .over(Window.partitionBy("date")))\
    .groupBy("date", "passenger_type","passenger_sum_byday")\
    .agg(F.sum("passenger_count").alias("passenger_sum_bytype"))\
    .withColumn("perc_by_pass", F.round(100*F.col("passenger_sum_bytype")/F.col("passenger_sum_byday"),2))\
    .select("date", "passenger_type", "perc_by_pass")\
    .groupby("date").pivot("passenger_type").sum("perc_by_pass")\
    .show()

df\
    .fillna(value=0, subset=["passenger_count"])\
    .withColumn("passenger_type",\
              F.when(F.col("passenger_count") >= F.lit(4),4)\
              .otherwise(F.col("passenger_count")))\
    .groupBy("date", "passenger_type")\
    .agg(F.count("passenger_count").alias("trip_sum_by_cnt"))\
    .withColumn("trip_sum_by_day",F.sum(F.col("trip_sum_by_cnt"))\
                .over(Window.partitionBy("date")))\
    .withColumn("perc_by_cnt", F.round(100*F.col("trip_sum_by_cnt")/F.col("trip_sum_by_day"),2))\
    .select("date", "passenger_type", "perc_by_cnt")\
    .groupby("date").pivot("passenger_type").sum("perc_by_cnt")\
    .show()