# Databricks notebook source
# MAGIC %md
# MAGIC #### Imports

# COMMAND ----------

import pyspark.sql.functions as F

SCOPE = "formula1-scope"
STORAGE_ACCOUNT_NAME = dbutils.secrets.get(scope=SCOPE, key="formula1dl-storage-account-name")
INPUT_CONTAINER = "processed"


def get_path_to_file(storage_account_name, container, fname):
    return f"/mnt/{storage_account_name}/{container}/{fname}"

RACES_FILE_PATH = get_path_to_file(STORAGE_ACCOUNT_NAME, INPUT_CONTAINER, "races")
CIRCUITS_FILE_PATH = get_path_to_file(STORAGE_ACCOUNT_NAME, INPUT_CONTAINER, "circuits")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load the data

# COMMAND ----------

races_sdf = spark.read.parquet(RACES_FILE_PATH)
circuits_sdf = spark.read.parquet(CIRCUITS_FILE_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC Apply example filter transformations

# COMMAND ----------

races_filtered_sdf = races_sdf.filter("race_year = 2019 and round <= 5")
# More Pythonic alternative
# races_filtered_sdf = races_sdf.filter((races_sdf["race_year"] == 2019) & (races_sdf["round"] <= 5))

display(races_filtered_sdf)

# COMMAND ----------

# MAGIC %md
# MAGIC Example of Inner Join 

# COMMAND ----------

circuits_sdf = (
  circuits_sdf
    .withColumnRenamed("name", "circuit_name")
    .filter("circuit_id < 70")
)

race_circuits_sdf = (
    circuits_sdf
      .join(
        races_sdf.filter("race_year = 2019"), 
        circuits_sdf.circuit_id == races_sdf.circuit_id, 
        "inner"
      )
      .select(circuits_sdf.circuit_name, circuits_sdf.location, circuits_sdf.country, races_sdf.name, races_sdf.round)
)

display(race_circuits_sdf)

# COMMAND ----------

# MAGIC %md
# MAGIC Example of Outer Join

# COMMAND ----------

# Left Outer Join
race_circuits_sdf = (
    circuits_sdf
      .join(
        races_sdf.filter("race_year = 2019"), 
        circuits_sdf.circuit_id == races_sdf.circuit_id, 
        "left"
      )
      .select(circuits_sdf.circuit_name, circuits_sdf.location, circuits_sdf.country, races_sdf.name, races_sdf.round)
)

display(race_circuits_sdf)

# COMMAND ----------

# Right Outer Join
race_circuits_sdf = (
    circuits_sdf
      .join(
        races_sdf.filter("race_year = 2019"), 
        circuits_sdf.circuit_id == races_sdf.circuit_id, 
        "right"
      )
      .select(circuits_sdf.circuit_name, circuits_sdf.location, circuits_sdf.country, races_sdf.name, races_sdf.round)
)

display(race_circuits_sdf)

# COMMAND ----------

# Full Outer Join
race_circuits_sdf = (
    circuits_sdf
      .join(
        races_sdf.filter("race_year = 2019"), 
        circuits_sdf.circuit_id == races_sdf.circuit_id, 
        "full"
      )
      .select(circuits_sdf.circuit_name, circuits_sdf.location, circuits_sdf.country, races_sdf.name, races_sdf.round)
)

display(race_circuits_sdf)

# COMMAND ----------

# MAGIC %md
# MAGIC Semi Joins

# COMMAND ----------

# Inner join, but only selects columns from the left dataframe
race_circuits_sdf = (
    circuits_sdf
      .join(
        races_sdf.filter("race_year = 2019"), 
        circuits_sdf.circuit_id == races_sdf.circuit_id, 
        "semi"
      )
      .select(circuits_sdf.circuit_name, circuits_sdf.location, circuits_sdf.country)
)

display(race_circuits_sdf)

# COMMAND ----------

# MAGIC %md
# MAGIC Anti Joins

# COMMAND ----------

# Anything on the left that is not found on the right
race_circuits_sdf = (
      races_sdf.filter("race_year = 2019")
      .join(
        circuits_sdf, 
        circuits_sdf.circuit_id == races_sdf.circuit_id, 
        "anti"
      )
)

display(race_circuits_sdf)

# COMMAND ----------

# MAGIC %md
# MAGIC Cross Joins

# COMMAND ----------

# Cartesian product
race_circuits_sdf = (
      races_sdf.filter("race_year = 2019")
      .crossJoin(
        circuits_sdf
      )
)

display(race_circuits_sdf)
