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