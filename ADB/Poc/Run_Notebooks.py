# Databricks notebook source
dbutils.widgets.text("Layer_Name","")
Layer_Name = dbutils.widgets.getArgument("Layer_Name")

# COMMAND ----------

Notebook_Path_Json = {
    "Raw": ["/Poc/Raw_Sourcing/Raw_Plain"],
    "Cleansed": [
        
            "/Poc/Cleansing/Airlines",
            "/Poc/Cleansing/Airport",
            "/Poc/Cleansing/Cancellation",
            "/Poc/Cleansing/Flight",
            "/Poc/Cleansing/Plane",
            "/Poc/Cleansing/Unique_Carrier",
        ],
    "Data_Quality_Cleansed":[
        "/Poc/Data_Quality_Noteboook/Cleansing_Data_Quality"
    ],
    "Mart": [
        "/Poc/Mart/Dim_Airlines",
        "/Poc/Mart/Dim_Airport",
        "/Poc/Mart/Dim_Cancellation",
        "/Poc/Mart/Dim_Flight",
        "/Poc/Mart/Dim_Plane",
        "/Poc/Mart/Dim_UniqueCarrier",
    ]

}

# COMMAND ----------

for notebook_paths in Notebook_Path_Json[Layer_Name]:
    dbutils.notebook.run(notebook_paths,0)