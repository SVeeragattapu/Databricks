# Databricks notebook source
# MAGIC %run /Poc/Utilities

# COMMAND ----------

list_table_info = [
    ("STREAMING UPDATE", "plane", 100),
    ("STREAMING UPDATE", "flight", 100),
    ("STREAMING UPDATE", "Airport", 100),
    ("STREAMING UPDATE", "cancellation", 100),
    ("STREAMING UPDATE", "unique_carriers", 100),
    ("WRITE", "airlines", 100),
]

for i in list_table_info:
    f_count_check("cleansed_poc", i[0], i[1], i[2])