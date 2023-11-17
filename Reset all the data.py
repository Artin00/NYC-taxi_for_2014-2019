# Databricks notebook source
#Running a code to access the data 

spark.conf.set("fs.azure.account.key.datacohortworkspacelabs.dfs.core.windows.net","NUwhjFcHG95EU1Rnu7+Woq3JQP28bXy5kDQhA9yFV68XBz1umr7uqeQgMhrwxHfTwNWxAx/n1K6j+AStGTUMkQ==")

# COMMAND ----------

spark.sql("DROP TABLE date_dimi")
spark.sql("DROP TABLE pay_dimi")
spark.sql("DROP TABLE loca_dimi")
spark.sql("DROP TABLE ven_dimi")
spark.sql("DROP TABLE time_dimi")
spark.sql("DROP TABLE fa_ab_taxi")

# COMMAND ----------

spark.sql("DROP TABLE da_dimension")
spark.sql("DROP TABLE ti_dimension")
spark.sql("DROP TABLE loca_dimension")
spark.sql("DROP TABLE vend_dimension")
spark.sql("DROP TABLE paym_dimension")
spark.sql("DROP TABLE taxi_fact_tab_new")

# COMMAND ----------

dbutils.fs.rm("abfss://silver-artin@datacohortworkspacelabs.dfs.core.windows.net/2019", True)
dbutils.fs.rm("abfss://bronze-artin@datacohortworkspacelabs.dfs.core.windows.net/2019", True)
dbutils.fs.rm("abfss://bronze-artin@datacohortworkspacelabs.dfs.core.windows.net/2014", True)

# COMMAND ----------

dbutils.fs.rm("abfss://gold-artin@datacohortworkspacelabs.dfs.core.windows.net/date_dim", True)
dbutils.fs.rm("abfss://gold-artin@datacohortworkspacelabs.dfs.core.windows.net/location_dim", True)
dbutils.fs.rm("abfss://gold-artin@datacohortworkspacelabs.dfs.core.windows.net/payment_dimi", True)
dbutils.fs.rm("abfss://gold-artin@datacohortworkspacelabs.dfs.core.windows.net/taxi_fact", True)
dbutils.fs.rm("abfss://gold-artin@datacohortworkspacelabs.dfs.core.windows.net/time_dim", True)
dbutils.fs.rm("abfss://gold-artin@datacohortworkspacelabs.dfs.core.windows.net/vendor_dim", True)

# COMMAND ----------

dbutils.fs.rm("abfss://landing-artin@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata_2014-01.csv", True)
dbutils.fs.rm("abfss://landing-artin@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata_2014-02.csv", True)
dbutils.fs.rm("abfss://landing-artin@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata_2014-03.csv", True)
dbutils.fs.rm("abfss://landing-artin@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata_2014-04.csv", True)
dbutils.fs.rm("abfss://landing-artin@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata_2014-05.csv", True)
dbutils.fs.rm("abfss://landing-artin@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata_2014-06.csv", True)
dbutils.fs.rm("abfss://landing-artin@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata_2014-07.csv", True)
dbutils.fs.rm("abfss://landing-artin@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata_2014-08.csv", True)
dbutils.fs.rm("abfss://landing-artin@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata_2014-09.csv", True)
dbutils.fs.rm("abfss://landing-artin@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata_2014-10.csv", True)
dbutils.fs.rm("abfss://landing-artin@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata_2014-11.csv", True)
dbutils.fs.rm("abfss://landing-artin@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata_2014-12.csv", True)

# COMMAND ----------

dbutils.fs.rm("abfss://landing-artin@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata_2019-01.csv", True)
dbutils.fs.rm("abfss://landing-artin@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata_2019-02.csv", True)
dbutils.fs.rm("abfss://landing-artin@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata_2019-03.csv", True)
dbutils.fs.rm("abfss://landing-artin@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata_2019-04.csv", True)
dbutils.fs.rm("abfss://landing-artin@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata_2019-05.csv", True)
dbutils.fs.rm("abfss://landing-artin@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata_2019-06.csv", True)
dbutils.fs.rm("abfss://landing-artin@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata_2019-07.csv", True)
dbutils.fs.rm("abfss://landing-artin@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata_2019-08.csv", True)
dbutils.fs.rm("abfss://landing-artin@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata_2019-09.csv", True)
dbutils.fs.rm("abfss://landing-artin@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata_2019-10.csv", True)
dbutils.fs.rm("abfss://landing-artin@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata_2019-11.csv", True)
dbutils.fs.rm("abfss://landing-artin@datacohortworkspacelabs.dfs.core.windows.net/yellow_tripdata_2019-12.csv", True)
