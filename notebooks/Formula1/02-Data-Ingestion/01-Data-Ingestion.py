# Databricks notebook source
# MAGIC %md
# MAGIC #### Imports

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, FloatType

SCOPE = "formula1-scope"
STORAGE_ACCOUNT_NAME = dbutils.secrets.get(scope=SCOPE, key="formula1dl-storage-account-name")
INPUT_CONTAINER = "raw"
OUTPUT_CONTAINER = "processed"


def get_path_to_file(storage_account_name, container, fname):
    return f"/mnt/{storage_account_name}/{container}/{fname}"

CIRCUITS_FILE_PATH = get_path_to_file(STORAGE_ACCOUNT_NAME, INPUT_CONTAINER, "circuits.csv")
RACES_FILE_PATH = get_path_to_file(STORAGE_ACCOUNT_NAME, INPUT_CONTAINER, "races.csv")
CONSTRUCTORS_FILE_PATH = get_path_to_file(STORAGE_ACCOUNT_NAME, INPUT_CONTAINER, "constructors.json")
DRIVERS_FILE_PATH = get_path_to_file(STORAGE_ACCOUNT_NAME, INPUT_CONTAINER, "drivers.json")
RESULTS_FILE_PATH = get_path_to_file(STORAGE_ACCOUNT_NAME, INPUT_CONTAINER, "results.json")
PITSTOPS_FILE_PATH = get_path_to_file(STORAGE_ACCOUNT_NAME, INPUT_CONTAINER, "pit_stops.json")
LAP_TIMES_PATH = get_path_to_file(STORAGE_ACCOUNT_NAME, INPUT_CONTAINER, "lap_times/*.csv")
QUALI_PATH = get_path_to_file(STORAGE_ACCOUNT_NAME, INPUT_CONTAINER, "qualifying/*.json")

# COMMAND ----------

# MAGIC %md
# MAGIC Define data schemas

# COMMAND ----------

# Circuits
circuits_schema = StructType(
  fields=[
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True)
  ]
)

# Races
races_schema = StructType(
  fields=[
    StructField("raceId", IntegerType(), True),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("date", StringType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
  ]
)

# Drivers
name_schema = StructType(fields=[
  StructField("forename", StringType(), True),
  StructField("surname", StringType(), True)
])

drivers_schema = StructType(fields=[
  StructField("driverId", IntegerType(), False),
  StructField("driverRef", StringType(), True),
  StructField("number", IntegerType(), True),
  StructField("code", StringType(), True),
  StructField("name", name_schema),
  StructField("dob", DateType(), True),
  StructField("nationality", StringType(), True),
  StructField("url", StringType(), True),
])

# Results
results_schema = StructType(fields=[
  StructField("resultId", IntegerType(), False),
  StructField("raceId", IntegerType(), True),   
  StructField("driverId", IntegerType(), True),
  StructField("constructorId", IntegerType(), True),
  StructField("number", IntegerType(), True),
  StructField("grid", IntegerType(), True),
  StructField("position", IntegerType(), True),
  StructField("positionText", StringType(), True),
  StructField("positionOrder", IntegerType(), True),
  StructField("points", FloatType(), True),
  StructField("laps", IntegerType(), True),
  StructField("time", StringType(), True),
  StructField("miliseconds", IntegerType(), True),
  StructField("fastestLap", IntegerType(), True),
  StructField("rank", IntegerType(), True),
  StructField("fastestLapTime", StringType(), True),
  StructField("fastestLapSpeed", FloatType(), True),
  StructField("statusId", StringType(), True),
])

# Lap Times
lap_times_schema = StructType(fields=[
  StructField("raceId", IntegerType(), False),
  StructField("driverId", IntegerType(), True),
  StructField("lap", IntegerType(), True),
  StructField("position", IntegerType(), True),
  StructField("time", StringType(), True),
  StructField("miliseconds", IntegerType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC Load and inspect the data

# COMMAND ----------

circuits_sdf = spark.read.csv(CIRCUITS_FILE_PATH, header=True, schema=circuits_schema)
race_sdf = spark.read.csv(RACES_FILE_PATH, header=True, schema=races_schema)
constructors_sdf = spark.read.json(CONSTRUCTORS_FILE_PATH) # JSONL
drivers_sdf = spark.read.json(DRIVERS_FILE_PATH, schema=drivers_schema) # JSONL with nested fields
results_sdf = spark.read.json(RESULTS_FILE_PATH, schema=results_schema) # JSONL
pit_stops_sdf = spark.read.json(PITSTOPS_FILE_PATH, multiLine=True) # list of JSONs
lap_times_sdf = spark.read.csv(LAP_TIMES_PATH, schema=lap_times_schema) # multiple CSVs
quali_sdf = spark.read.json(QUALI_PATH, multiLine=True)

# COMMAND ----------

circuits_sdf.show(n=2, vertical=True)

# COMMAND ----------

circuits_sdf.printSchema()

# COMMAND ----------

circuits_sdf.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC Apply required transformations

# COMMAND ----------

# Circuits
select_cols = ["circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt"]
circuits_sdf_final = (
    circuits_sdf
        .select(*select_cols)
        .withColumnRenamed("circuitId", "circuit_id")
        .withColumnRenamed("circuitRef", "circuit_ref")
        .withColumnRenamed("lat", "latitude")
        .withColumnRenamed("lng", "longitute")
        .withColumnRenamed("alt", "altitude")
        .withColumn("ingestion_date", F.current_timestamp())
)

# Races
race_sdf_final = (
    race_sdf
        .withColumn(
            "race_timestamp", 
            F.to_timestamp(F.concat(F.col("date"), F.lit(" "), F.col("time")), "yyyy-MM-dd HH:mm:ss")
        )
        .withColumn("ingestion_date", F.current_timestamp())
        .select(
            F.col("raceId").alias("race_id"), 
            F.col("year").alias("race_year"),
            F.col("round"),
            F.col("circuitId").alias("circuit_id"),
            F.col("name"),
            F.col("race_timestamp"),
            F.col("ingestion_date")
        )
)

# Constructors
constructors_sdf_selected = (
    constructors_sdf
        .drop(F.col("url"))
        .withColumn("ingestion_date", F.current_timestamp())
        .withColumnRenamed("constructorId", "constructor_id") 
        .withColumnRenamed("constructorRef", "constructor_ref")
    )

# Drivers
drivers_sdf_selected = (
    drivers_sdf
        .withColumnRenamed("driverId", "driver_id")
        .withColumnRenamed("driverRef", "driver_ref")
        .withColumn("ingestion_date", F.current_timestamp())
        .withColumn("name", F.concat(F.col("name.forename"), F.lit(" "), F.col("name.surname")))
        .drop(F.col("url"))
)

# Results
results_sdf_final = (
    results_sdf
        .withColumnRenamed("resultId", "result_id")
        .withColumnRenamed("raceId", "race_id")
        .withColumnRenamed("driverId", "driver_id")
        .withColumnRenamed("constructorId", "constructor_id")
        .withColumnRenamed("positionText", "position_text")
        .withColumnRenamed("positionOrder", "position_order")
        .withColumnRenamed("fastestLap", "fastest_lap")
        .withColumnRenamed("fastestLapTime", "fastest_lap_time")
        .withColumnRenamed("FastestLapSpeed", "fastest_lap_speed")
        .withColumn("ingestion_date", F.current_timestamp())
)

# Pit stops
pit_stops_sdf_final = (
    pit_stops_sdf
        .withColumnRenamed("driverId", "driver_id")
        .withColumnRenamed("raceId", "race_id")
        .withColumn("ingestion_date", F.current_timestamp())
)

# Lap Times
lap_times_sdf_final = (
    lap_times_sdf
        .withColumnRenamed("driverId", "driver_id")
        .withColumnRenamed("raceId", "race_id")
        .withColumn("ingestion_date", F.current_timestamp())
)

# Quali
quali_sdf_final = (
    quali_sdf
        .withColumnRenamed("qualifyingId", "qualifying_id")
        .withColumnRenamed("driverId", "driver_id")
        .withColumnRenamed("raceId", "race_id")
        .withColumnRenamed("constructorId", "constructor_id")
        .withColumn("ingestion_date", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC Save processing results to `processed` container

# COMMAND ----------

circuits_sdf_final.write.parquet(
    get_path_to_file(STORAGE_ACCOUNT_NAME, OUTPUT_CONTAINER, "circuits"), 
    mode="overwrite"
)

race_sdf_final.write.parquet(
    get_path_to_file(STORAGE_ACCOUNT_NAME, OUTPUT_CONTAINER, "races"), 
    mode="overwrite"
)

constructors_sdf_selected.write.parquet(
    get_path_to_file(STORAGE_ACCOUNT_NAME, OUTPUT_CONTAINER, "constructors"), 
    mode="overwrite"
)

drivers_sdf_selected.write.parquet(
    get_path_to_file(STORAGE_ACCOUNT_NAME, OUTPUT_CONTAINER, "drivers"), 
    mode="overwrite"
)

results_sdf_final.write.parquet(
    get_path_to_file(STORAGE_ACCOUNT_NAME, OUTPUT_CONTAINER, "results"), 
    mode="overwrite"
)

pit_stops_sdf_final.write.parquet(
    get_path_to_file(STORAGE_ACCOUNT_NAME, OUTPUT_CONTAINER, "pit_stops"), 
    mode="overwrite"
)

lap_times_sdf_final.write.parquet(
    get_path_to_file(STORAGE_ACCOUNT_NAME, OUTPUT_CONTAINER, "lap_times"), 
    mode="overwrite"
)

quali_sdf_final.write.parquet(
    get_path_to_file(STORAGE_ACCOUNT_NAME, OUTPUT_CONTAINER, "qualifying"), 
    mode="overwrite"
)
