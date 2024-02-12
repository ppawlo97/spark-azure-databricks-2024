# Databricks notebook source
# MAGIC %md
# MAGIC #### Create a table for analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC USE f1_processed

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE f1_presentation.calculated_race_results
# MAGIC USING parquet
# MAGIC AS
# MAGIC SELECT races.race_year,
# MAGIC        constructors.name AS team_name,
# MAGIC        drivers.name AS driver_name,
# MAGIC        results.position,
# MAGIC        results.points,
# MAGIC        11 - results.position AS calculated_points
# MAGIC FROM results
# MAGIC JOIN drivers ON (results.driver_id = drivers.driver_id)
# MAGIC JOIN constructors ON (results.constructor_id = constructors.constructor_id)
# MAGIC JOIN races ON (results.race_id = races.race_id)
# MAGIC WHERE results.position <= 10;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Find dominant drivers

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_dominant_drivers
# MAGIC AS
# MAGIC SELECT driver_name,
# MAGIC        COUNT(1) AS total_races,
# MAGIC        SUM(calculated_points) AS total_points,
# MAGIC        AVG(calculated_points) AS avg_points,
# MAGIC        RANK() OVER(ORDER BY AVG(calculated_points ) DESC) AS driver_rank
# MAGIC FROM f1_presentation.calculated_race_results
# MAGIC GROUP BY driver_name
# MAGIC HAVING total_races >= 50
# MAGIC ORDER BY avg_points DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT driver_name,
# MAGIC        COUNT(1) AS total_races,
# MAGIC        SUM(calculated_points) AS total_points,
# MAGIC        AVG(calculated_points) AS avg_points
# MAGIC FROM f1_presentation.calculated_race_results
# MAGIC WHERE race_year BETWEEN 2011 AND 2020
# MAGIC GROUP BY driver_name
# MAGIC HAVING total_races >= 50
# MAGIC ORDER BY avg_points DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT team_name,
# MAGIC        COUNT(1) AS total_races,
# MAGIC        SUM(calculated_points) AS total_points,
# MAGIC        AVG(calculated_points) AS avg_points
# MAGIC FROM f1_presentation.calculated_race_results
# MAGIC GROUP BY team_name
# MAGIC HAVING total_races >= 50
# MAGIC ORDER BY avg_points DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT team_name,
# MAGIC        COUNT(1) AS total_races,
# MAGIC        SUM(calculated_points) AS total_points,
# MAGIC        AVG(calculated_points) AS avg_points
# MAGIC FROM f1_presentation.calculated_race_results
# MAGIC WHERE race_year BETWEEN 2011 AND 2020
# MAGIC GROUP BY team_name
# MAGIC HAVING total_races >= 50
# MAGIC ORDER BY avg_points DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Visualizations

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_year, 
# MAGIC        driver_name,
# MAGIC        COUNT(1) AS total_races,
# MAGIC        SUM(calculated_points) AS total_points,
# MAGIC        AVG(calculated_points) AS avg_points,
# MAGIC        RANK() OVER(ORDER BY AVG(calculated_points ) DESC) AS driver_rank
# MAGIC FROM f1_presentation.calculated_race_results
# MAGIC WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
# MAGIC GROUP BY race_year, driver_name 
# MAGIC ORDER BY race_year, avg_points DESC;
