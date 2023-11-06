# Databricks notebook source
# MAGIC %run /Poc/Utilities

# COMMAND ----------

df = spark.readStream.format("cloudFiles").option("cloudFiles.format",'csv')\
    .option("cloudFiles.schemaLocation","/dbfs/FileStore/tables/schema/Airport")\
    .load('/mnt/raw_datalake/Airport/')

# COMMAND ----------

#dbutils.fs.rm('/dbfs/FileStore/tables/checkpointLocation/Airport',True)

# COMMAND ----------

df_base = df.selectExpr(
    "code",
    "split(Description,',')[0] as city",
    "split(split(Description,',')[1],':')[0] as country",
    "split(split(Description,',')[1],':')[1] as airport",     
    "to_date(Date_Part,'yyyy-MM-dd') as Date_Part",
    )

df_base.writeStream.trigger(once=True)\
    .format("delta")\
    .option("checkpointLocation","/dbfs/FileStore/tables/checkpointLocation/Airport")\
    .start("/mnt/cleansed_datalake/airport")



# COMMAND ----------

#schema=pre_schema('/mnt/cleansed_datalake/airport')
f_delta_cleansed_load('airport','/mnt/cleansed_datalake/airport','cleansed_poc')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_poc.airport;
