-- Databricks notebook source
-- MAGIC %sql
-- MAGIC use mart_poc;

-- COMMAND ----------

DESC cleansed_poc.airlines

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS DIM_AIRLINES (
  iata_code STRING,
  icao_code STRING,
  name STRING
) USING DELTA LOCATION '/mnt/mart_datalake/DIM_AIRLINES'

-- COMMAND ----------

INSERT OVERWRITE DIM_AIRLINES
SELECT
 iata_code,
  icao_code,
  name
  FROM cleansed_poc.airlines
