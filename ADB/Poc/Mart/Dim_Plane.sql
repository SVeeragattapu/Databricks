-- Databricks notebook source
-- MAGIC %sql
-- MAGIC use mart_poc;

-- COMMAND ----------

DESC cleansed_poc.plane

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS DIM_PLANE (
  tailid STRING,
  type STRING,
  manufacturer STRING,
  issue_date DATE,
  model STRING,
  status STRING,
  aircraft_type STRING,
  engine_type STRING,
  year INT
) USING DELTA LOCATION '/mnt/mart_datalake/DIM_PLANE'

-- COMMAND ----------

INSERT OVERWRITE DIM_PLANE
SELECT
 tailid,
  type,
  manufacturer,
  issue_date,
  model,
  status,
  aircraft_type,
  engine_type,
  year
  FROM cleansed_poc.plane
