# Databricks notebook source
# MAGIC %md
# MAGIC **CREATING BRONZE LAYER DELTA TABLES**

# COMMAND ----------

from datetime import datetime

#Copy Current Date data from ADLS to Databricks Bronze schema
for folder in dbutils.fs.ls(base_path):
    folder_name = datetime.today().strftime("%Y-%m-%d")
    tblName= folder.name.replace("/", "")
    
    try:

        file_path=f"{folder.path}{folder_name}/"
        df = spark.read.parquet(file_path) 
        df.write.format("delta").mode("overwrite").saveAsTable(f"bronze.{tblName}")
        del df

    except Exception:
        # Ignore folders where the dated path does not exist or other errors
        pass
        