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

df=spark.read\
  .format("jdbc")\
  .option("url", url)\
  .option("dbtable", "(SELECT date(tpep_pickup_datetime) as date, passenger_count, Total_amount FROM public.yellow_tripdata)  AS t")\
  .option("user", "postgres")\
  .option("password", "postgres")\
  .option('partitionColumn','date')\
  .option('numPartitions',50)\
  .option("lowerBound", '2020-01-01')\
  .option("upperBound", '2021-01-01')\
  .load()
df_tmp=df.fillna(value=0, subset=["passenger_count"])    
df_tmp_all=df_tmp\
    .orderBy("date")\
    .groupBy("date")\
    .agg(F.count("passenger_count").alias("passenger_count_by_day"))
df_tmp_zero=df_tmp\
    .where(F.col("passenger_count")==0)\
    .orderBy("date")\
    .groupBy(F.col("date").alias("date0"))\
    .agg(F.count("passenger_count").alias("passenger_count_zero"),\
         F.max("total_amount").alias("max_pay_zero"),\
         F.min("total_amount").alias("min_pay_zero"))   
df_tmp_1p= df_tmp\
    .where(F.col("passenger_count")==1)\
    .orderBy("date")\
    .groupBy(F.col("date").alias("date1"))\
    .agg(F.count("passenger_count").alias("passenger_count_1p"),\
         F.max("total_amount").alias("max_pay_1p"),\
         F.min("total_amount").alias("min_pay_1p"))
df_tmp_2p= df_tmp\
    .where(F.col("passenger_count")==2)\
    .orderBy("date")\
    .groupBy(F.col("date").alias("date2"))\
    .agg(F.count("passenger_count").alias("passenger_count_2p"),\
         F.max("total_amount").alias("max_pay_2p"),\
         F.min("total_amount").alias("min_pay_2p"))
df_tmp_3p= df_tmp\
    .where(F.col("passenger_count")==3)\
    .orderBy("date")\
    .groupBy(F.col("date").alias("date3"))\
    .agg(F.count("passenger_count").alias("passenger_count_3p"),\
         F.max("total_amount").alias("max_pay_3p"),\
         F.min("total_amount").alias("min_pay_3p"))
df_tmp_4p_plus= df_tmp\
    .where(F.col("passenger_count")>=4)\
    .orderBy("date")\
    .groupBy(F.col("date").alias("date4"))\
    .agg(F.count("passenger_count").alias("passenger_count_4p_plus"),\
         F.max("total_amount").alias("max_pay_4p_plus"),\
         F.min("total_amount").alias("min_pay_4p_plus"))
df_tmp1 = df_tmp_all\
    .join(df_tmp_zero, df_tmp_all["date"] == df_tmp_zero["date0"],"left")\
    .join(df_tmp_1p, df_tmp_all["date"] == df_tmp_1p["date1"],"left")\
    .join(df_tmp_2p, df_tmp_all["date"] == df_tmp_2p["date2"],"left")\
    .join(df_tmp_3p, df_tmp_all["date"] == df_tmp_3p["date3"],"left")\
    .join(df_tmp_4p_plus, df_tmp_all["date"] == df_tmp_4p_plus["date4"],"left")\
    .withColumn("percentage_zero",  F.round(100*F.col("passenger_count_zero")/F.col("passenger_count_by_day"),2))\
    .withColumn("percentage_1p",  F.round(100*F.col("passenger_count_1p")/F.col("passenger_count_by_day"),2))\
    .withColumn("percentage_2p",  F.round(100*F.col("passenger_count_2p")/F.col("passenger_count_by_day"),2))\
    .withColumn("percentage_3p",  F.round(100*F.col("passenger_count_3p")/F.col("passenger_count_by_day"),2))\
    .withColumn("percentage_4p_plus",  F.round(100*F.col("passenger_count_4p_plus")/F.col("passenger_count_by_day"),2))\
    .select("date","percentage_zero","percentage_1p","percentage_2p","percentage_3p","percentage_4p_plus"\
           ,"max_pay_zero","max_pay_1p","max_pay_2p","max_pay_3p","max_pay_4p_plus"\
           ,"min_pay_zero","min_pay_1p","min_pay_2p","min_pay_3p","min_pay_4p_plus")\
    .orderBy("date")
df_tmp1.show()
df_tmp1.write\
  .format("jdbc")\
  .mode("overwrite")\
  .option("url", url).option("user", "postgres").option("password", "postgres")\
  .option("dbtable", "public.parquet")\
  .save()