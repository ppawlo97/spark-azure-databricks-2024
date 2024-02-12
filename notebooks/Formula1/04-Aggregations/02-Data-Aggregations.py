# Databricks notebook source
# MAGIC %md
# MAGIC #### Imports

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window

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
# MAGIC Perform driver/constructor standings aggregations

# COMMAND ----------

driver_standings_sdf = (
    race_results_sdf
        .groupBy("race_year", "driver_name", "driver_nationality", "team")
        .agg(F.sum("points").alias("total_points"),
             F.count(F.when(F.col("position") == 1, True)).alias("wins"))
)

display(driver_standings_sdf)

constructors_standings_sdf = (
    race_results_sdf
    .groupBy("race_year", "team")
    .agg(F.sum("points").alias("total_points"),
            F.count(F.when(F.col("position") == 1, True)).alias("wins"))
)

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(F.desc("total_points"), F.desc("wins"))

final_sdf = driver_standings_sdf.withColumn("rank", F.rank().over(driver_rank_spec))
final_constructors_sdf = constructors_standings_sdf.withColumn("rank", F.rank().over(driver_rank_spec))

display(final_sdf.filter("race_year = 2020"))
display(final_constructors_sdf.filter(F.col("race_year").isin([2020])))

# COMMAND ----------

# MAGIC %md
# MAGIC Save results

# COMMAND ----------

db_name = "f1_presentation"

final_sdf.write.saveAsTable(
    f"{db_name}.driver_standings", 
    mode="overwrite",
    format="parquet"
)

final_constructors_sdf.write.saveAsTable(
    f"{db_name}.constructor_standings", 
    mode="overwrite",
    format="parquet"
)

