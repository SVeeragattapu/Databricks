-- Databricks notebook source
-- MAGIC %sql
-- MAGIC use mart_poc;

-- COMMAND ----------

DESC cleansed_poc.unique_carriers

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS DIM_UNIQUE_CARRIERS (
  code STRING,
  description STRING
) USING DELTA LOCATION '/mnt/mart_datalake/DIM_UNIQUE_CARRIERS'

-- COMMAND ----------

INSERT OVERWRITE DIM_UNIQUE_CARRIERS
SELECT
 code,
  description
  FROM cleansed_poc.unique_carriers