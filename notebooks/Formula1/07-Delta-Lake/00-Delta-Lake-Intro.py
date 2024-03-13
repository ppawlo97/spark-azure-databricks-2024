# Databricks notebook source
# MAGIC %md
# MAGIC #### Imports

# COMMAND ----------

import pyspark.sql.functions as F
from delta.tables import DeltaTable

SCOPE = "formula1-scope"
STORAGE_ACCOUNT_NAME = dbutils.secrets.get(scope=SCOPE, key="formula1dl-storage-account-name")
INPUT_CONTAINER = "raw"
FILE_DATE = "2021-04-18"

def get_path_to_file(storage_account_name, container, fname):
    return f"/mnt/{storage_account_name}/{container}/{FILE_DATE}/{fname}"

RESULTS_FILE_PATH = get_path_to_file(STORAGE_ACCOUNT_NAME, INPUT_CONTAINER, "results.json")
DRIVERS_FILE_PATH = get_path_to_file(STORAGE_ACCOUNT_NAME, INPUT_CONTAINER, "drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC Load the data

# COMMAND ----------

results_sdf = spark.read.json(RESULTS_FILE_PATH)
drivers_1_day_sdf = (spark.read.json(DRIVERS_FILE_PATH)
               .filter("driverId <= 10")
               .select("driverId", "dob", "name.forename", "name.surname"))
drivers_1_day_sdf.createOrReplaceTempView("drivers_day1")

drivers_2_day_sdf = (spark.read.json(DRIVERS_FILE_PATH)
               .filter("driverId BETWEEN 6 AND 15")
               .select("driverId", "dob", F.upper("name.forename").alias("forename"), F.upper("name.surname").alias("surname")))
drivers_2_day_sdf.createOrReplaceTempView("drivers_day2")


drivers_3_day_sdf = (spark.read.json(DRIVERS_FILE_PATH)
               .filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20")
               .select("driverId", "dob", F.upper("name.forename").alias("forename"), F.upper("name.surname").alias("surname")))


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
# MAGIC
# MAGIC SQL Approach

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge ( 
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge AS tgt
# MAGIC USING drivers_day1 AS upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC               tgt.forename = upd.forename,
# MAGIC               tgt.surname = upd.surname,
# MAGIC               tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename, surname, createdDate) VALUES (driverId, dob, forename, surname, current_timestamp);

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge AS tgt
# MAGIC USING drivers_day2 AS upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC               tgt.forename = upd.forename,
# MAGIC               tgt.surname = upd.surname,
# MAGIC               tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename, surname, createdDate) VALUES (driverId, dob, forename, surname, current_timestamp);

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark Approach

# COMMAND ----------

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1dlpp/demo/drivers_merge")

deltaTable.alias("tgt").merge(
  drivers_3_day_sdf.alias("upd"),
  "tgt.driverId = upd.driverId") \
  .whenMatchedUpdate(set = {"dob": "upd.dob", "forename": "upd.forename", "surname": "upd.surname", "updatedDate": "current_timestamp()"}) \
  .whenNotMatchedInsert(values = {
    "driverId": "upd.driverId",
    "dob": "upd.dob", "forename": "upd.forename", "surname": "upd.surname", "createdDate": "current_timestamp()"
  }) \
  .execute()

# COMMAND ----------

# MAGIC %md
# MAGIC History & Versoning, Time Travel, Vaccum

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM f1_demo.drivers_merge RETAIN 200 HOURS;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Convert Parquet to Delta
# MAGIC
# MAGIC If the data is stored in the table:
# MAGIC ```sql
# MAGIC CONVERT TO DELTA <table-name>
# MAGIC ```
# MAGIC
# MAGIC If the data is stored in a Parquet file:
# MAGIC ```sql
# MAGIC CONVERT TO DELTA parquet.'folder-name-path'
# MAGIC ```
