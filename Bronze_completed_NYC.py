# Databricks notebook source
#Running a code to access the data 

spark.conf.set("fs.azure.account.key.datacohortworkspacelabs.dfs.core.windows.net","NUwhjFcHG95EU1Rnu7+Woq3JQP28bXy5kDQhA9yFV68XBz1umr7uqeQgMhrwxHfTwNWxAx/n1K6j+AStGTUMkQ==")

# COMMAND ----------

#Uploading the data and removing the space in front of the column name
from pyspark.sql import functions as F

data_14 = spark.read.option("inferSchema", "true").option("header", "true").csv("abfss://landing-artin@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata_2014*")
data_2014 = data_14.select([F.col(column).alias(column.strip()) for column in data_14.columns])
data_2014.show(5)

data_19 = spark.read.option("inferSchema", "true").option("header", "true").csv("abfss://landing-artin@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata_2019*")
data_2019 = data_19.select([F.col(column).alias(column.strip()) for column in data_19.columns])
data_2019.show(5)

# COMMAND ----------

# Removing the RateCode and Store_and fwd flag, 
date_2014 = data_2014.drop("rate_code","store_and_fwd_flag","surcharge")
date_2014.show(5)

date_2019 = data_2019.drop("RatecodeID", "store_and_fwd_flag", "improvement_surcharge", "congestion_surcharge")
date_2019.show(5)

# COMMAND ----------

# Creating the code to input the desired columns and change datatype to delta as well as partition by Vendor ID
from pyspark.sql.functions import input_file_name, current_timestamp

date_2014_file = date_2014.withColumn("file_name", input_file_name()) 

date_2014_complete = date_2014_file.withColumn("created_on", current_timestamp())
date_2014_complete.show(5, truncate = False)


date_2019_file = date_2019.withColumn("file_name", input_file_name()) 

date_2019_complete = date_2019_file.withColumn("created_on", current_timestamp())
date_2019_complete.show(5, truncate = False)

# COMMAND ----------

# Saving the file in the bronze container
bronze_2014 = date_2014_complete.write.format("delta").partitionBy("vendor_id").save("abfss://bronze-artin@datacohortworkspacelabs.dfs.core.windows.net/2014")
bronze_2019 = date_2019_complete.write.format("delta").partitionBy("VendorID").save("abfss://bronze-artin@datacohortworkspacelabs.dfs.core.windows.net/2019")
