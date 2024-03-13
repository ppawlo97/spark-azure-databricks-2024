# Databricks notebook source
# MAGIC %md
# MAGIC #### Imports

# COMMAND ----------

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql.window import Window

SCOPE = "formula1-scope"
STORAGE_ACCOUNT_NAME = dbutils.secrets.get(scope=SCOPE, key="formula1dl-storage-account-name")
INPUT_CONTAINER = "presentation"
OUTPUT_CONTAINER = "presentation"
FILE_DATE = "2021-03-21"


def get_path_to_file(storage_account_name, container, fname):
    return f"/mnt/{storage_account_name}/{container}/{fname}"

RACE_RESULTS_FILE_PATH = get_path_to_file(STORAGE_ACCOUNT_NAME, INPUT_CONTAINER, "race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load the data

# COMMAND ----------

race_results_sdf = spark.read.format("delta").load(RACE_RESULTS_FILE_PATH).filter(f"file_date = '{FILE_DATE}'")

race_year_list = [record.race_year for record in race_results_sdf.select("race_year").distinct().collect()]
race_results_sdf = race_results_sdf.filter(F.col("race_year").isin(race_year_list))

# COMMAND ----------

# MAGIC %md
# MAGIC Perform driver/constructor standings aggregations

# COMMAND ----------

driver_standings_sdf = (
    race_results_sdf
        .groupBy("race_year", "driver_name", "driver_nationality")
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
    table_name="driver_standings", 
    merge_on_cols=["driver_name"], 
    partition_column="race_year"
)

perform_incremental_load(
    sdf=final_constructors_sdf, 
    db_name=db_name, 
    table_name="constructor_standings", 
    merge_on_cols=["team"], 
    partition_column="race_year"
)

