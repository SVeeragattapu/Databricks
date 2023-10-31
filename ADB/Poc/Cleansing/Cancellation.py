# Databricks notebook source
# MAGIC %run /Poc/Utilities

# COMMAND ----------

df = spark.readStream.format("cloudFiles").option("cloudFiles.format",'parquet')\
    .option("cloudFiles.schemaLocation","/dbfs/FileStore/tables/schema/Cancellation")\
    .load('/mnt/raw_datalake/Cancellation/')

# COMMAND ----------

dbutils.fs.rm('/dbfs/FileStore/tables/checkpointLocation/Cancellation',True)

# COMMAND ----------

df_base = df.selectExpr(
    "replace(Code,'\"','') as code",
    "replace(Description,'\"','') as description",    
    "to_date(Date_Part,'yyyy-MM-dd') as Date_Part"
    )

df_base.writeStream.trigger(once=True)\
    .format("delta")\
    .option("checkpointLocation","/dbfs/FileStore/tables/checkpointLocation/Cancellation")\
    .start("/mnt/cleansed_datalake/cancellation")



# COMMAND ----------

#schema=pre_schema('/mnt/cleansed_datalake/cancellation')
f_delta_cleansed_load('cancellation','/mnt/cleansed_datalake/cancellation','cleansed_poc')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_poc.cancellation;
