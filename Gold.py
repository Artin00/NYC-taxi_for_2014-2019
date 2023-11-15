# Databricks notebook source
# MAGIC %pip install geopandas

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

#Running a code to access the data 

spark.conf.set("fs.azure.account.key.datacohortworkspacelabs.dfs.core.windows.net","NUwhjFcHG95EU1Rnu7+Woq3JQP28bXy5kDQhA9yFV68XBz1umr7uqeQgMhrwxHfTwNWxAx/n1K6j+AStGTUMkQ==")

# COMMAND ----------

silver_fac = spark.read.load("abfss://silver-artin@datacohortworkspacelabs.dfs.core.windows.net/DataSet", format = "delta")
silver_fac.show()

silver_fact = silver_fac

# COMMAND ----------

# Creating the logical database for task 2
import geopandas as gpd
from shapely.geometry import Point
import pandas as pd
import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, FloatType
from pyspark.sql.functions import *
import geopandas as gpd
from shapely.geometry import Point
import uuid 
from pyspark.sql.window import Window 

df = taxi_fact_table = StructType([ \
                        StructField("trip_id", IntegerType(), True), \
                        StructField("vendor_id", IntegerType(), True), \
                        StructField("pickup_date_id", IntegerType(), True),\
                        StructField("pickup_time_id", IntegerType(), True),\
                        StructField("dropoff_date_id", IntegerType(), True), \
                        StructField("dropoff_time_id", IntegerType(), True),\
                        StructField("passenger count", IntegerType(), True), \
                        StructField("trip_distance", FloatType(), True),\
                        StructField("pickup_id", IntegerType(), True), \
                        StructField("dropoff_id", IntegerType(), True), \
                        StructField("payment_type_id", IntegerType(), True), \
                        StructField("fare_amount", FloatType(), True), \
                        StructField("extra", FloatType(), True), \
                        StructField("mta_tax", FloatType(), True), \
                        StructField("tip_amount", FloatType(), True), \
                        StructField("toll_amount", FloatType(), True), \
                        StructField("total_amount", FloatType(), True) ])

df2 = time_dimension_table = StructType([ \
                        StructField("time_id", IntegerType(), True), \
                        StructField("time", TimestampType(), True), \
                        StructField("hour", IntegerType(), True), \
                        StructField("minute", IntegerType(), True),\
                        StructField("second", IntegerType(), True),])

df3 = date_dimension_table = StructType([ \
                        StructField("date_id", IntegerType(), True), \
                        StructField("date", DateType(), True), \
                        StructField("year", IntegerType(), True), \
                        StructField("month", IntegerType(), True),\
                        StructField("day", IntegerType(), True),])

df4 = vendor_dimension_table = StructType([ \
                        StructField("vendor_ids", IntegerType(), True), \
                        StructField("vendor_number", IntegerType(), True), \
                        StructField("vendor_name", StringType(), True),])

df5 = payment_type_dimension_table = StructType([ \
                        StructField("payment_types_ids", IntegerType(), True), \
                        StructField("payment_type_text", StringType(), True),])

df6 = location_type_dimension_table = StructType([ \
                        StructField("location_id", IntegerType(), True), \
                        StructField("location_name", StringType(), True),])




# COMMAND ----------

# Showing the schemas that have been created

dfs = spark.createDataFrame([[None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None]], schema=df)
dfs.show()

df2s = spark.createDataFrame([[None, None, None, None, None]], schema=df2)
df2s.show()

df3s = spark.createDataFrame([[None, None, None, None, None]], schema=df3)
df3s.show()

df4s = spark.createDataFrame([[None, None, None]], schema=df4)
df4s.show()

df5s = spark.createDataFrame([[None, None]], schema=df5)
df5s.show()

df6s = spark.createDataFrame([[None, None]], schema=df6)
df6s.show()

# COMMAND ----------

# Create the data for the vendor_id and payment_type_id and allocate a key

generated_uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())

vendor_data = [(1, 1, "CMT"), (2, 2, "VTS"), (3, 4, "Unknown")]

vendor_df = spark.createDataFrame(vendor_data, df4)
vendor_dfr = vendor_df.withColumn("vendor_ids", generated_uuid_udf())
vendor_dfr.display()

payment_type_data = [(1, "CRD"), (2, "CSH"), (3, "DIS"), (4, "UNK"), (5, "NOC")]

payment_type_df = spark.createDataFrame(payment_type_data, df5)
payment_type_dfs = payment_type_df.withColumn("payment_types_id", col("payment_types_ids"))
payment_type_dfse = payment_type_dfs.withColumn("payment_types_id", generated_uuid_udf())
payment_type_dfse.display()

# COMMAND ----------

# Creating the time_dimension_table with the necessary data 

from pyspark.sql.functions import col, concat_ws, format_string, date_format, lit, desc, to_date, month, posexplode, expr , explode, array, from_unixtime, unix_timestamp

time_df = spark.range(0, 24 * 60 * 60, 1).select((col("id")).alias("total_seconds"))

time_df = time_df.withColumn("hour", (col("total_seconds") / 3600).cast("int"))
time_df = time_df.withColumn("minute", ((col("total_seconds") % 3600) / 60).cast("int"))
time_df = time_df.withColumn("second", (col("total_seconds") % 60).cast("int"))

time_df = time_df.withColumn("time", concat_ws(":", col("hour"), col("minute"), col("second")).cast("int"))
time_df = time_df.withColumn("time_id", format_string("%02d%02d%02d", col("hour"), col("minute"), col("second")).cast("int"))
time_df = time_df.withColumn("time", col("total_seconds").cast("timestamp"))
time_df = time_df.withColumn("time", date_format("time", "HH:mm:ss"))
time_df = time_df.withColumn("time_id", format_string("%02d%02d%02d", col("hour"), col("minute"), col("second")))

time_df2s = time_df.select("time_id", "time", "hour", "minute", "second")

new_time_df2s = df2s.union(time_df2s)

generated_uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())

df2ste = new_time_df2s.na.drop()

df2st = df2ste.withColumn("time_id", generated_uuid_udf())


df2st.show()
df2st.count()

# COMMAND ----------

# Create a date_dimension_table and generate the necessary data from 2014 and 2019

df3s_9 = spark.range(0, 365).withColumn("id", col("id").cast("int"))
df3s_9 = df3s_9.withColumn("date", expr("date_add(to_date('2014-01-01'), id)"))
df3s_9 = df3s_9.withColumn("year", expr("year(date)").cast("int"))
df3s_9.show()

df3s_1 = df3s_9.withColumn("month", expr("month(date)").cast("int"))
df3s_1 = df3s_1.withColumn("day", expr("day(date)").cast("int"))

df3s_1 = df3s_1.withColumn("date_id", (col("id")).cast("int"))
df3s_1 = df3s_1.withColumn("date_id", format_string("%04d%02d%02d", col("year"), col("month"), col("day")).cast("int"))


df3s_1.display()

df3s_1.show(40)
df3s_1 = df3s_1.select("date_id", "date", "year", "month", "day")

df3s_1_new = df3s.union(df3s_1)

df3s_12 = df3s_1_new.na.drop()
df3s_12.display()

df3s_0 = spark.range(0, 365).withColumn("id", col("id").cast("int"))
df3s_0 = df3s_0.withColumn("date", expr("date_add(to_date('2019-01-01'), id)"))
df3s_5 = df3s_0.withColumn("year", expr("year(date)").cast("int"))
df3s_5.show()

df3s_5 = df3s_5.withColumn("month", expr("month(date)").cast("int"))
df3s_5 = df3s_5.withColumn("day", expr("day(date)").cast("int"))

df3s_14 = df3s_5.withColumn("date_id", (col("id")).cast("int"))
df3s_14 = df3s_14.withColumn("date_id", format_string("%04d%02d%02d", col("year"), col("month"), col("day")).cast("int"))

df3s_14 = df3s_14.select("date_id", "date", "year", "month", "day")

df3s_14_new = df3s.union(df3s_14)

df3s_new = df3s_14_new.na.drop()
df3s_new.display()

generated_uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())

df30 = df3s_new.union(df3s_12)

df30 = df30.withColumn("date_id", generated_uuid_udf())

df30.display()

# COMMAND ----------

import geopandas as gpd
from shapely.geometry import Point
import pandas as pd
import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, FloatType
from pyspark.sql.functions import *
import geopandas as gpd
from shapely.geometry import Point
import uuid 

zones = gpd.read_file("file:///Workspace/Users/artin.keyhani@qualyfi.co.uk/location_data.geojson")

# COMMAND ----------

# Create the location dimensional table
column_names = list(zones.columns)
print(column_names)

selected_columns = zones[["location_i", "zone"]]
distinct_column = selected_columns.drop_duplicates(subset = "zone")


crazy = distinct_column.reset_index(drop=True)
crazies = spark.createDataFrame(crazy)

crazies_fun = crazies.withColumn("location_id", col("location_i").cast("int")) \
                     .withColumn("location_name", col("zone"))
crazier = crazies_fun.drop("location_i", "zone")
cray = crazier.withColumn("location_ids", col("location_id"))
cram = cray.withColumn("location_ids", generated_uuid_udf())

display(cram)

# COMMAND ----------

# Add the assigned key to the main table and remove pickup/dropoff time 
df2st_new = df2st.drop("hour", "minute", "second")
silver_fac_time = silver_fact.join(df2st_new, silver_fact.pickup_time == df2st_new.time, "inner")
silver_fact_time_do = silver_fac_time.withColumnRenamed("time_id", "pickup_time_id")
silver_fact_time_dos = silver_fact_time_do.drop("pickup_time", "time")
silver_fact_time_dos.show()


# COMMAND ----------


silver_fact_time_done = silver_fact_time_dos.join(df2st_new, silver_fac_time.dropoff_time == df2st_new.time, "inner")
silver_fact_time_new = silver_fact_time_done.withColumnRenamed("time_id", "dropoff_time_id")
silver_fact_done_time = silver_fact_time_new.drop("dropoff_time", "time")
silver_fact_done_time.show()

# COMMAND ----------

# The data in the pickup/dropoff date switched with a assigned key column consisting of the same values
df30_new = df30.drop("year", "month", "day")
silver_fact_da = silver_fact_done_time.join(df30_new, silver_fact_done_time.pickup_date == df30_new.date, "inner")
silver_fact_dat = silver_fact_da.withColumnRenamed("date_id", "pickup_date_id")
silver_fact_date_dos = silver_fact_dat.drop("pickup_date", "date")

silver_fact_do = silver_fact_date_dos.join(df30_new, silver_fact_date_dos.dropoff_date == df30_new.date, "inner")
silver_fact_dop = silver_fact_do.withColumnRenamed("date_id", "dropoff_date_id")
silver_fact_dope = silver_fact_dop.drop("dropoff_date", "date")
silver_fact_dope.show()

# COMMAND ----------

# Data organised into the desired taxt_fact_table order


windows = Window.orderBy("pickup_date_id")
silver_fact_table = silver_fact_dope.withColumn("trip_id", generated_uuid_udf ()) \
                                    .withColumnRenamed("payment_type", "payment_type_id") 
silver_fact_tables_is = silver_fact_table.select("trip_id", "vendor_id", "pickup_date_id", "pickup_time_id", "dropoff_date_id", "dropoff_time_id", "passenger_count", "trip_distance", "pickup_location_id", "dropoff_location_id", "payment_type_id", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "total_amount")

# COMMAND ----------

# Assign key to payment_type_id
payway = payment_type_dfse.drop("payment_type_text")

silver_fact_tables_is_n = silver_fact_tables_is.join(payway, silver_fact_tables_is.payment_type_id == payway.payment_types_ids, "inner")
silver_fact_tables_is_new = silver_fact_tables_is_n.drop("payment_types_ids", "payment_type_id")
silver_fact_tables_is_news = silver_fact_tables_is_new.withColumnRenamed("payment_types_id", "payment_type_id")

# COMMAND ----------

# Assign key to location_id
crane = cram.drop("location_name")

near_gold  =  silver_fact_tables_is_new.join(crane, silver_fact_tables_is_new.pickup_location_id == crane.location_id, "inner")
nearl_gold = near_gold.drop("location_id", "pickup_location_id")
nearly_gold = nearl_gold.withColumnRenamed("location_ids", "pickup_location_id")

near_goldi  =  nearly_gold.join(crane, nearly_gold.dropoff_location_id == crane.location_id, "inner")
nearl_golde = near_goldi.drop("location_id", "dropoff_location_id")
nearly_golden = nearl_golde.withColumnRenamed("location_ids", "dropoff_location_id")

# COMMAND ----------

vend = vendor_dfr.drop("vendor_name")

noway = nearly_golden.join(vend, nearly_golden.vendor_id == vend.vendor_number, "inner")
noaways = noway.drop("vendor_id", "vendor_number")
noaways_now = noaways.withColumnRenamed("vendor_ids", "vendor_id")


# COMMAND ----------

gold_completed = noaways_now.select("trip_id", "vendor_id", "pickup_date_id", "pickup_time_id", "dropoff_date_id", "dropoff_time_id", "passenger_count", "trip_distance", "pickup_location_id", "dropoff_location_id", "payment_types_id", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "total_amount")
gold_completed.printSchema()

# COMMAND ----------

# Creation of delta dimension_tables within databricks for PowerBI use
cram.write.format("delta").saveAsTable("location_dim")
vendor_dfr.write.format("delta").saveAsTable("vendor_dim")
df30.write.format("delta").saveAsTable("date_dim")
df2st.write.format("delta").saveAsTable("time_dim")
payment_type_dfse.write.format("delta").saveAsTable("payment_dim")

# COMMAND ----------

# Creation of delta fact_tables within databricks for PowerBI use
gold_completed.write.format("delta").saveAsTable("taxi_fact")

# COMMAND ----------

payment_type_dfse.write.format("delta").saveAsTable("pay_dim", path = "abfss://gold-artin@datacohortworkspacelabs.dfs.core.windows.net/payment_dim")

# COMMAND ----------

df30.write.format("delta").saveAsTable("date_dim", path = "abfss://gold-artin@datacohortworkspacelabs.dfs.core.windows.net/date_dim")

# COMMAND ----------

cram.write.format("delta").saveAsTable("loca_dim", path = "abfss://gold-artin@datacohortworkspacelabs.dfs.core.windows.net/location_dim")

# COMMAND ----------

vendor_dfr.write.format("delta").saveAsTable("ven_dim", path = "abfss://gold-artin@datacohortworkspacelabs.dfs.core.windows.net/vendor_dim")

# COMMAND ----------

df2st.write.format("delta").saveAsTable("time_dimensi", path = "abfss://gold-artin@datacohortworkspacelabs.dfs.core.windows.net/time_dim")

# COMMAND ----------

gold_completed.write.format("delta").partitionBy("vendor_id").saveAsTable("fact_tab_taxi", path = "abfss://gold-artin@datacohortworkspacelabs.dfs.core.windows.net/taxi_fact")
