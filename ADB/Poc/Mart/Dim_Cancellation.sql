-- Databricks notebook source
-- MAGIC %sql
-- MAGIC use mart_poc;

-- COMMAND ----------

DESC cleansed_poc.cancellation

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS DIM_CANCELLATION (
  code STRING,
  description STRING
) USING DELTA LOCATION '/mnt/mart_datalake/DIM_CANCELLATION'

-- COMMAND ----------

INSERT OVERWRITE DIM_CANCELLATION
SELECT
 code,
  description
  FROM cleansed_poc.cancellation
