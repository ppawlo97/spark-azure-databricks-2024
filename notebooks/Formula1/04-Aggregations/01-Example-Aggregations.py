# Databricks notebook source
# MAGIC %md
# MAGIC #### Imports

# COMMAND ----------

import pyspark.sql.functions as F

SCOPE = "formula1-scope"
STORAGE_ACCOUNT_NAME = dbutils.secrets.get(scope=SCOPE, key="formula1dl-storage-account-name")
INPUT_CONTAINER = "presentation"


def get_path_to_file(storage_account_name, container, fname):
    return f"/mnt/{storage_account_name}/{container}/{fname}"

RACE_RESULTS_PATH = get_path_to_file(STORAGE_ACCOUNT_NAME, INPUT_CONTAINER, "race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load the data

# COMMAND ----------

race_results_sdf = spark.read.parquet(RACE_RESULTS_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC Limit number of records

# COMMAND ----------

demo_sdf = race_results_sdf.filter("race_year == 2020")
display(demo_sdf)

# COMMAND ----------

# MAGIC %md
# MAGIC Perform example aggregations

# COMMAND ----------

# All not null race_name number
demo_sdf.select(F.count("race_name")).show()

# COMMAND ----------

# Distinct race_name
demo_sdf.select(F.countDistinct("race_name")).show()

# COMMAND ----------

# All points
demo_sdf.select(F.sum("points")).show()

# COMMAND ----------

# All points for Hamilton
demo_sdf.filter("driver_name == 'Lewis Hamilton'").select(F.sum("points")).show()

# COMMAND ----------

# All points and number of races for Hamilton
demo_sdf.filter("driver_name == 'Lewis Hamilton'").select(F.sum("points"), F.countDistinct("race_name")).show()
