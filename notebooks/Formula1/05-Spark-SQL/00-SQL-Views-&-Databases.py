# Databricks notebook source
# MAGIC %md
# MAGIC #### Imports

# COMMAND ----------

import pyspark.sql.functions as F

SCOPE = "formula1-scope"
STORAGE_ACCOUNT_NAME = dbutils.secrets.get(scope=SCOPE, key="formula1dl-storage-account-name")
INPUT_CONTAINER = "presentation"
OUTPUT_CONTAINER = "presentation"


def get_path_to_file(storage_account_name, container, fname):
    return f"/mnt/{storage_account_name}/{container}/{fname}"

RACE_RESULTS_FILE_PATH = get_path_to_file(STORAGE_ACCOUNT_NAME, INPUT_CONTAINER, "race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load the data

# COMMAND ----------

race_results_sdf = spark.read.parquet(RACE_RESULTS_FILE_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC Create a temporary view

# COMMAND ----------

# Use createOrReplaceGlobalTempView to make it accessible from other notebooks
race_results_sdf.createOrReplaceTempView("v_race_results") # OR createTempView

# COMMAND ----------

# MAGIC %md
# MAGIC Run SQL commands
# MAGIC
# MAGIC Add `global_temp.v_race_results` for global view access

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1)
# MAGIC FROM v_race_results
# MAGIC WHERE race_year = 2020

# COMMAND ----------

# MAGIC %md
# MAGIC Access the table from Python

# COMMAND ----------

year = 2020
race_results_2020_sdf = spark.sql(f"SELECT * FROM v_race_results WHERE race_year = {year}")

display(race_results_2020_sdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Operate on the Database

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW databases;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DATABASE EXTENDED demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CURRENT_DATABASE();

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES in demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Managed Tables
# MAGIC > Note: The data underneath is deleted with the table drop.
# MAGIC
# MAGIC Write the data into a table

# COMMAND ----------

race_results_sdf.write.saveAsTable("demo.race_results_python", format="parquet")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED race_results_python;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE race_results_sql AS
# MAGIC SELECT *
# MAGIC FROM demo.race_results_python
# MAGIC WHERE race_year = "2020";

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED demo.race_results_sql

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE demo.race_results_sql

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extended Tables

# COMMAND ----------

race_results_sdf.write.saveAsTable(
    "demo.race_results_ext_py", 
    format="parquet",
    path=get_path_to_file(STORAGE_ACCOUNT_NAME, OUTPUT_CONTAINER, "race_results_ext_py")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED demo.race_results_ext_py;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE demo.race_results_ext_py;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Views

# COMMAND ----------

# MAGIC %md
# MAGIC Change to `CREATE OR REPLACE GLOBAL TEMP VIEW` for global view

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMP VIEW v_race_results
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM demo.race_results_python
# MAGIC WHERE race_year = 2018;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM v_race_results;
