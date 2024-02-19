# Databricks notebook source
# MAGIC %md
# MAGIC #### Imports

# COMMAND ----------

import pyspark.sql.functions as F

SCOPE = "formula1-scope"
STORAGE_ACCOUNT_NAME = dbutils.secrets.get(scope=SCOPE, key="formula1dl-storage-account-name")
INPUT_CONTAINER = "raw"
FILE_DATE = "2021-04-18"

def get_path_to_file(storage_account_name, container, fname):
    return f"/mnt/{storage_account_name}/{container}/{FILE_DATE}/{fname}"

RESULTS_FILE_PATH = get_path_to_file(STORAGE_ACCOUNT_NAME, INPUT_CONTAINER, "results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC Load the data

# COMMAND ----------

results_sdf = spark.read.json(RESULTS_FILE_PATH)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION "/mnt/formula1dlpp/demo"

# COMMAND ----------

# MAGIC %md
# MAGIC Save to Data Lake

# COMMAND ----------

results_sdf.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

results_sdf_back = spark.read.format("delta").load("/mnt/formula1dlpp/demo/results_managed")

# COMMAND ----------

# MAGIC %md
# MAGIC Updates and Deletes

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed
# MAGIC SET points = 11 - position
# MAGIC WHERE position <= 10;

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1dlpp/demo/results_managed")
deltaTable.update("position <= 10", { "points": "21 - position" })

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.results_managed
# MAGIC WHERE position > 10;

# COMMAND ----------

deltaTable.delete("points = 0")

# COMMAND ----------

# MAGIC %md
# MAGIC Merge / Upsert
