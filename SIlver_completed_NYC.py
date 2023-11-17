# Databricks notebook source
#Running a code to access the data 

spark.conf.set("fs.azure.account.key.datacohortworkspacelabs.dfs.core.windows.net","NUwhjFcHG95EU1Rnu7+Woq3JQP28bXy5kDQhA9yFV68XBz1umr7uqeQgMhrwxHfTwNWxAx/n1K6j+AStGTUMkQ==")

# COMMAND ----------

# MAGIC %pip install geopandas

# COMMAND ----------

pip install fsspec

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

bronz_2014 = spark.read.load("abfss://bronze-artin@datacohortworkspacelabs.dfs.core.windows.net/2014", format = "delta")

bronz_2019 = spark.read.load("abfss://bronze-artin@datacohortworkspacelabs.dfs.core.windows.net/2019", format = "delta")

# COMMAND ----------

# Converting the datetime columns into seperate pickup/dropoff date and pickup/dropoff time columns in both dataframes
from pyspark.sql.functions import col, date_format, to_timestamp
from datetime import datetime
from pyspark.sql.types import DayTimeIntervalType

data = bronz_2014.withColumn("pickup_date", date_format("pickup_datetime", "yyyy-MM-dd").cast("date"))
deta = data.withColumn("pickup_time", to_timestamp("pickup_datetime", "HH:mm:ss"))
detail = deta.withColumn("pickup_time", date_format("pickup_time", "HH:mm:ss"))
data_1 = detail.withColumn("dropoff_date", date_format("dropoff_datetime", "yyyy-MM-dd").cast("date"))
bronze_2014_kn = data_1.withColumn("dropoff_time", to_timestamp("dropoff_datetime", "HH:mm:ss"))
bronze_2014_kne = bronze_2014_kn.withColumn("dropoff_time",date_format("dropoff_time", "HH:mm:ss"))
bronze_2014_knew = bronze_2014_kne.drop("pickup_datetime", "dropoff_datetime")


lab = bronz_2019.withColumn("pickup_date", date_format("tpep_pickup_datetime", "yyyy-MM-dd").cast("date"))
labs = lab.withColumn("pickup_time", to_timestamp("tpep_pickup_datetime", "HH:mm:ss"))
lew = labs.withColumn("pickup_time", date_format("pickup_time", "HH:mm:ss"))
labryinth = lew.withColumn("dropoff_date", date_format("tpep_dropoff_datetime", "yyyy-MM-dd").cast("date"))
bronze_2019_kn = labryinth.withColumn("dropoff_time", date_format("tpep_dropoff_datetime", "HH:mm:ss"))
bronze_2019_kne = bronze_2019_kn.withColumn("dropoff_time", date_format("dropoff_time", "HH:mm:ss"))
bronze_2019_knew = bronze_2019_kne.drop("tpep_pickup_datetime", "tpep_dropoff_datetime")

display(bronze_2014_knew)
display(bronze_2019_knew)

# COMMAND ----------

#Using a UDF to input the longitude and latitude values within this JSON formula to obtain the location_id
import geopandas as gpd
from shapely.geometry import Point
import pandas as pd
import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, FloatType
from pyspark.sql.functions import *

zonals = gpd.read_file("file:///Workspace/Users/artin.keyhani@qualyfi.co.uk/location_data.geojson")

dfws = bronze_2014_knew
display(dfws.limit(10))

def find_location_id(row):
    longitude = row[str("pickup_longitude")]
    latitude = row[str("pickup_latitude")]
    point = Point(longitude, latitude)

    for index, zone in zonals.iterrows():
        if zone['geometry'].contains(point):
            return zone["location_i"]
    
    return None

find_location_id_udf = udf(find_location_id, StringType())
dfw_with_location_id = dfws.withColumn("pickup_location_id", find_location_id_udf(struct(dfws.columns)))
display(dfw_with_location_id.limit(10))

# COMMAND ----------

def find_location_id(row):
    longitude = row[str("dropoff_longitude")]
    latitude = row[str("dropoff_latitude")]
    point = Point(longitude, latitude)

    for index, zone in zonals.iterrows():
        if zone['geometry'].contains(point):
            return zone["location_i"]
    
    return None

find_location_id_udf = udf(find_location_id, StringType())
dfw_with_both = dfw_with_location_id.withColumn("dropoff_location_id", find_location_id_udf(struct(dfw_with_location_id.columns)))
display(dfw_with_both.limit(10))

# COMMAND ----------

# Create the conditions mentioned where TotalAmount is notNull and greated that 0
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, FloatType

condition = (col("total_amount").isNotNull()) & (col("total_amount") > 0)
bronze_2014_fil = dfw_with_both.filter(condition) 
display(bronze_2014_fil.limit(10))

condition_19 = (col("total_amount").isNotNull()) & (col("total_amount") > 0)
bronze_2019_fil = bronze_2019_knew.filter(condition_19) 
display(bronze_2019_fil.limit(10))

# COMMAND ----------

# Create the conditions mentioned where TripDistance greater than 0

distance = (col("trip_distance") > 0)
bronze_2014_filtered = bronze_2014_fil.filter(distance)
display(bronze_2014_filtered.limit(10))

distance_19 = (col("trip_distance") > 0)
bronz_2019_filtered = bronze_2019_fil.filter(distance_19)
display(bronz_2019_filtered.limit(10))


# COMMAND ----------

# Organise the schema of the data and the datatype as well as selecting the necessary column order

from pyspark.sql.types import DoubleType

brond_2019 = bronz_2019_filtered.withColumnRenamed("Vendorid", "vendor_id") \
                                .withColumnRenamed("PULocationID", "pickup_location_id") \
                                .withColumnRenamed("DOLocationID", "dropoff_location_id")

bronze_2019_done_fi = brond_2019.withColumn("trip_distance", col("trip_distance").cast(FloatType())) \
                                       .withColumn("fare_amount", col("fare_amount").cast(FloatType())) \
                                       .withColumn("mta_tax", col("mta_tax").cast(FloatType())) \
                                       .withColumn("tip_amount", col("tip_amount").cast(FloatType())) \
                                       .withColumn("tolls_amount", col("tolls_amount").cast(FloatType())) \
                                       .withColumn("total_amount", col("total_amount").cast(FloatType()))
bronze_2019_lols = bronze_2019_done_fi.select("vendor_id", "pickup_date", "pickup_time", "dropoff_date", "dropoff_time", \
                                           "passenger_count", "trip_distance", "pickup_location_id", "dropoff_location_id", "payment_type", \
                                           "fare_amount", "mta_tax", "tip_amount", "tolls_amount", "total_amount", "file_name", "created_on", "extra")

bronze_2019_lols.show()

brond_2014 = bronze_2014_filtered.drop("pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude")
brond_2014 = brond_2014.withColumn("pickup_location_id", col("pickup_location_id").cast("int")) \
                           .withColumn("dropoff_location_id", col("dropoff_location_id").cast("int")) \
                           .withColumn("trip_distance", col("trip_distance").cast(FloatType())) \
                           .withColumn("fare_amount", col("fare_amount").cast(FloatType())) \
                           .withColumn("mta_tax", col("mta_tax").cast(FloatType())) \
                           .withColumn("tip_amount", col("tip_amount").cast(FloatType())) \
                           .withColumn("tolls_amount", col("tolls_amount").cast(FloatType())) \
                           .withColumn("total_amount", col("total_amount").cast(FloatType()))

bronze_2014_done_fi = brond_2014.select("vendor_id", "pickup_date", "pickup_time", "dropoff_date", "dropoff_time", \
                                           "passenger_count", "trip_distance", "pickup_location_id", "dropoff_location_id", "payment_type", \
                                           "fare_amount", "mta_tax", "tip_amount", "tolls_amount", "total_amount", "file_name", "created_on")

display(bronze_2014_done_fi.limit(10))

# COMMAND ----------

#Saving the dataframe to the silver container
silver_2019 = bronze_2019_lols.write.format("delta").partitionBy("vendor_id").save("abfss://silver-artin@datacohortworkspacelabs.dfs.core.windows.net/2019")
