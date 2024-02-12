# Databricks notebook source
# MAGIC %md
# MAGIC #### Setup

# COMMAND ----------

SCOPE = "formula1-scope"
STORAGE_ACCOUNT_NAME = dbutils.secrets.get(scope=SCOPE, key="formula1dl-storage-account-name")

# COMMAND ----------

# MAGIC %md
# MAGIC Create the database and tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.circuits;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.circuits(
# MAGIC   circuitId INT,
# MAGIC   circuitRef STRING,
# MAGIC   name STRING,
# MAGIC   location STRING,
# MAGIC   country STRING,
# MAGIC   lat DOUBLE,
# MAGIC   lng DOUBLE,
# MAGIC   alt INT,
# MAGIC   url STRING
# MAGIC )
# MAGIC USING csv
# MAGIC OPTIONS (path '/mnt/formula1dlpp/raw/circuits.csv', header true);

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.races;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.races(
# MAGIC   raceId INT,
# MAGIC   year INT,
# MAGIC   round INT,
# MAGIC   circuitId INT,
# MAGIC   name STRING,
# MAGIC   date DATE,
# MAGIC   time STRING,
# MAGIC   url STRING
# MAGIC )
# MAGIC USING csv
# MAGIC OPTIONS (path '/mnt/formula1dlpp/raw/races.csv', header true);

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.constructors;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.constructors(
# MAGIC   constructorId INT,
# MAGIC   constructorRef STRING,
# MAGIC   name STRING,
# MAGIC   nationality STRING,
# MAGIC   url STRING
# MAGIC )
# MAGIC USING json
# MAGIC OPTIONS (path '/mnt/formula1dlpp/raw/constructors.json');

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.drivers;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.drivers(
# MAGIC   driverId INT,
# MAGIC   driverRef STRING,
# MAGIC   number INT,
# MAGIC   code STRING,
# MAGIC   name STRUCT<forename: STRING, surname: STRING>,
# MAGIC   dob DATE,
# MAGIC   nationality STRING,
# MAGIC   url STRING
# MAGIC )
# MAGIC USING json
# MAGIC OPTIONS (path '/mnt/formula1dlpp/raw/drivers.json');

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.results;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.results(
# MAGIC   resultId INT,
# MAGIC   raceId INT,
# MAGIC   driverId INT,
# MAGIC   constructorId INT,
# MAGIC   number INT,
# MAGIC   grid INT,
# MAGIC   position INT,
# MAGIC   positionText STRING,
# MAGIC   positionOrder INT,
# MAGIC   points INT,
# MAGIC   laps INT,
# MAGIC   time STRING,
# MAGIC   milliseconds INT,
# MAGIC   fastestLap INT,
# MAGIC   rank INT,
# MAGIC   fastestLapTime STRING,
# MAGIC   fastestLapSpeed FLOAT,
# MAGIC   statusId STRING
# MAGIC )
# MAGIC USING json
# MAGIC OPTIONS (path '/mnt/formula1dlpp/raw/results.json');

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.pit_stops;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
# MAGIC   raceId INT,
# MAGIC   driverId INT,
# MAGIC   duration STRING,
# MAGIC   lap INT,
# MAGIC   time STRING,
# MAGIC   milliseconds INT,
# MAGIC   stop INT
# MAGIC )
# MAGIC USING json
# MAGIC OPTIONS (path '/mnt/formula1dlpp/raw/pit_stops.json', multiLine true);

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.lap_times;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
# MAGIC   raceId INT,
# MAGIC   driverId INT,
# MAGIC   lap INT,
# MAGIC   position INT,
# MAGIC   time STRING,
# MAGIC   milliseconds INT
# MAGIC )
# MAGIC USING csv
# MAGIC OPTIONS (path '/mnt/formula1dlpp/raw/lap_times');

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.qualifying;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
# MAGIC   constructorId INT,
# MAGIC   driverId INT,
# MAGIC   number INT,
# MAGIC   position INT,
# MAGIC   q1 STRING,
# MAGIC   q2 STRING,
# MAGIC   q3 STRING,
# MAGIC   qualifyId INT,
# MAGIC   raceId INT
# MAGIC )
# MAGIC USING json
# MAGIC OPTIONS (path '/mnt/formula1dlpp/raw/qualifying', multiLine true);
