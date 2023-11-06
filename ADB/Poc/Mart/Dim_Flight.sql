-- Databricks notebook source
-- MAGIC %sql
-- MAGIC use mart_poc;

-- COMMAND ----------

DESC cleansed_poc.flight

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS Reporting_Flight (
  date DATE,
  ArrDelay INT,
  DepDelay INT,
  Origin STRING,
  Cancelled INT,
  CancellationCode STRING,
  UniqueCarrier STRING,
  FlightNum INT,
  TailNum STRING,
  deptime STRING
) USING DELTA 
PARTITIONED BY (date_year int)
LOCATION '/mnt/mart_datalake/Reporting_Flight'

-- COMMAND ----------

-- MAGIC %py
-- MAGIC #dbutils.fs.rm('/mnt/mart_datalake/Reporting_Flight',True)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC max_year = spark.sql("SELECT year(max(date)) from cleansed_poc.flight").collect()[0][0]
-- MAGIC #max_year=2007

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.sql(f"""
-- MAGIC INSERT OVERWRITE Reporting_Flight PARTITION (date_year = {max_year})
-- MAGIC SELECT
-- MAGIC   date,
-- MAGIC   ArrDelay,
-- MAGIC   DepDelay,
-- MAGIC   Origin,
-- MAGIC   Cancelled,
-- MAGIC   CancellationCode,
-- MAGIC   UniqueCarrier,
-- MAGIC   FlightNum,
-- MAGIC   TailNum,
-- MAGIC   deptime
-- MAGIC <<<<<<< Updated upstream
-- MAGIC   FROM cleansed_poc.flight where year(date)={max_year} """)
