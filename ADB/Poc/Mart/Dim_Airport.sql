-- Databricks notebook source
-- MAGIC %sql
-- MAGIC use mart_poc;

-- COMMAND ----------

DESC cleansed_poc.airport

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS DIM_AIRPORT (
  code STRING,
  city STRING,
  country STRING,
  airport STRING
) USING DELTA LOCATION '/mnt/mart_datalake/DIM_AIRPORT'

-- COMMAND ----------

INSERT OVERWRITE DIM_AIRPORT
SELECT
 code,
  city,
  country,
  airport
  FROM cleansed_poc.airport