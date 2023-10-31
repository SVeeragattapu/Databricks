-- Databricks notebook source
-- MAGIC %sql
-- MAGIC use mart_poc;

-- COMMAND ----------

DESC cleansed_poc.flight

-- COMMAND ----------

DROP TABLE Reporting_Flight

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
) USING DELTA LOCATION '/mnt/mart_datalake/Reporting_Flight'

-- COMMAND ----------

INSERT OVERWRITE Reporting_Flight
SELECT
  date,
  ArrDelay,
  DepDelay,
  Origin,
  Cancelled,
  CancellationCode,
  UniqueCarrier,
  FlightNum,
  TailNum,
  deptime
  FROM cleansed_poc.flight