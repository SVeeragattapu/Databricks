# Databricks notebook source
# MAGIC %run /Poc/Utilities

# COMMAND ----------

df = spark.readStream.format("cloudFiles").option("cloudFiles.format",'json')\
    .option("cloudFiles.schemaLocation","/dbfs/FileStore/tables/schema/airlines")\
    .load('/mnt/raw_datalake/airlines/')

# COMMAND ----------

from pyspark.sql.functions import explode
df = spark.read.json('/mnt/raw_datalake/airlines/')
df1 = df.select(explode("response"),"Date_Part")
df_final = df1.select("col.*","Date_Part")

# COMMAND ----------

df_final.write.format("delta").mode("overwrite").save("/mnt/cleansed_datalake/airlines")

# COMMAND ----------

f_delta_cleansed_load('airlines','/mnt/cleansed_datalake/airlines','cleansed_poc')
