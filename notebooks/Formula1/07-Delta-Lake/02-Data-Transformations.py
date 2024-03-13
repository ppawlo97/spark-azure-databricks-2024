# Databricks notebook source
# MAGIC %md
# MAGIC #### Imports

# COMMAND ----------

import pyspark.sql.functions as F
from delta.tables import DeltaTable

SCOPE = "formula1-scope"
STORAGE_ACCOUNT_NAME = dbutils.secrets.get(scope=SCOPE, key="formula1dl-storage-account-name")
INPUT_CONTAINER = "processed"
OUTPUT_CONTAINER = "presentation"
FILE_DATE = "2021-04-18"


def get_path_to_file(storage_account_name, container, fname):
    return f"/mnt/{storage_account_name}/{container}/{fname}"

CIRCUITS_FILE_PATH = get_path_to_file(STORAGE_ACCOUNT_NAME, INPUT_CONTAINER, "circuits")
RACES_FILE_PATH = get_path_to_file(STORAGE_ACCOUNT_NAME, INPUT_CONTAINER, "races")
CONSTRUCTORS_FILE_PATH = get_path_to_file(STORAGE_ACCOUNT_NAME, INPUT_CONTAINER, "constructors")
DRIVERS_FILE_PATH = get_path_to_file(STORAGE_ACCOUNT_NAME, INPUT_CONTAINER, "drivers")
RESULTS_FILE_PATH = get_path_to_file(STORAGE_ACCOUNT_NAME, INPUT_CONTAINER, "results")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load the data

# COMMAND ----------

circuits_sdf = spark.read.format("delta").load(CIRCUITS_FILE_PATH)
races_sdf = spark.read.format("delta").load(RACES_FILE_PATH)
constructors_sdf = spark.read.format("delta").load(CONSTRUCTORS_FILE_PATH)
drivers_sdf = spark.read.format("delta").load(DRIVERS_FILE_PATH)
results_sdf = spark.read.format("delta").load(RESULTS_FILE_PATH)

# COMMAND ----------

results_sdf.show(n=1,vertical=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Apply required transformations

# COMMAND ----------

# Circuits and Races
circuits_sdf = circuits_sdf.withColumnRenamed("location", "circuit_location")
races_sdf = races_sdf.withColumnRenamed("name", "race_name")

final_sdf = (
    races_sdf
        .join(circuits_sdf, 
              races_sdf.circuit_id == circuits_sdf.circuit_id, 
              "left")
        .select(races_sdf.race_id, races_sdf.race_year, races_sdf.race_name, races_sdf.race_timestamp,
                circuits_sdf.circuit_location)
)

# Drivers, Constructors and Results
drivers_sdf = drivers_sdf.withColumnRenamed("name", "driver_name").withColumnRenamed("nationality", "driver_nationality").withColumnRenamed("number", "driver_number")
results_sdf = results_sdf.filter(f"file_date = '{FILE_DATE}'").withColumnRenamed("time", "race_time")
constructors_sdf = constructors_sdf.withColumnRenamed("name", "team")

final_results_sdf = (
    results_sdf
        .join(
            drivers_sdf,
            results_sdf.driver_id == drivers_sdf.driver_id,
            "left"
        )
        .join(
            constructors_sdf,
            results_sdf.constructor_id == constructors_sdf.constructor_id,
            "left"
        )
        .select(drivers_sdf.driver_name, drivers_sdf.driver_nationality, drivers_sdf.driver_number, constructors_sdf.team, results_sdf.race_id, results_sdf.grid, results_sdf.fastest_lap, results_sdf.points, results_sdf.race_time, results_sdf.position, results_sdf.file_date)
)

# Final Join
final_sdf = (
    final_results_sdf
        .join(
            final_sdf,
            final_results_sdf.race_id == final_sdf.race_id,
            "left"
        )
        .drop(final_sdf.race_id)
)

final_sdf = final_sdf.withColumn("created_time", F.current_timestamp())

columns_except_last = [col_name for col_name in final_sdf.columns if col_name != 'race_id']
new_columns_order = columns_except_last + ['race_id']
final_sdf = final_sdf.select(*new_columns_order)

# COMMAND ----------

final_sdf.show(1, vertical=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Check against a specific race

# COMMAND ----------

display(
    final_sdf
        .filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'")
        .orderBy(final_sdf.points.desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC Save results

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_presentation
# MAGIC LOCATION "/mnt/formula1dlpp/presentation";

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

def perform_incremental_load(sdf, db_name, table_name, merge_on_cols, partition_column: str = "race_id"):
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")
    merge_statement = " AND ".join([f"tgt.{merge_col} = src.{merge_col}" 
                                        for merge_col in merge_on_cols + [partition_column]])
    db_path = db_name.removeprefix("f1_")
    
    if spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}"):
        deltaTable = DeltaTable.forPath(spark, f"/mnt/formula1dlpp/{db_path}/{table_name}")
        deltaTable.alias("tgt").merge(
            sdf.alias("src"),
            merge_statement) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
        
    else:
        sdf.write.saveAsTable(
            f"{db_name}.{table_name}", 
            mode="overwrite",
            format="delta",
            partitionBy=partition_column
        )

# COMMAND ----------

db_name = "f1_presentation"

perform_incremental_load(
    sdf=final_sdf, 
    db_name=db_name, 
    table_name="race_results", 
    merge_on_cols=["driver_name"], 
    partition_column="race_id"
)
