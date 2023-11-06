# Databricks notebook source
# MAGIC %run /Poc/Utilities

# COMMAND ----------


insert_query="select count(*) from mart_poc.dim_unique_carriers group by code having count(*)>1"
insert_test_cases("mart_poc",1,"Check if code is duplicated in the dim_uniquecarrier or not",insert_query,0)


# COMMAND ----------

insert_query="select count(*) from mart_poc.dim_airport group by code having count(*)>1"
insert_test_cases("mart_poc",2,"Check if code is duplicated in the dim_airport or not",insert_query,0)