# Databricks notebook source
# MAGIC %md
# MAGIC #### Simple queries

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SELECT CURRENT_DATABASE();

# COMMAND ----------

# MAGIC %sql
# MAGIC USE f1_processed;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM drivers
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC drivers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT name, dob AS date_of_birth, nationality
# MAGIC FROM drivers
# MAGIC WHERE (nationality = 'British'
# MAGIC        AND dob > '1990-01-01') OR
# MAGIC        nationality = "Indian"
# MAGIC ORDER BY dob DESC, name ASC;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Functions

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT name, 
# MAGIC        CONCAT(driver_ref, "-", code) AS new_driver_ref,
# MAGIC        SPLIT(name, " ")[0] forename, SPLIT(name, " ")[1] surname,
# MAGIC        current_timestamp ,
# MAGIC        date_format(dob, "dd-MM-yyyy")
# MAGIC FROM drivers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*), MAX(dob)
# MAGIC FROM drivers
# MAGIC WHERE nationality = 'British';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT nationality, COUNT(*) as count
# MAGIC FROM drivers
# MAGIC GROUP BY nationality
# MAGIC HAVING count > 55
# MAGIC ORDER BY count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT nationality, name, dob, RANK() OVER(PARTITION BY nationality ORDER BY dob DESC) AS age_rank
# MAGIC FROM drivers
# MAGIC ORDER BY age_rank, nationality;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Joins

# COMMAND ----------

# MAGIC %sql
# MAGIC USE f1_presentation;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC driver_standings;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_driver_standings_2018
# MAGIC AS
# MAGIC SELECT race_year, driver_name, team, total_points, wins, rank
# MAGIC FROM driver_standings
# MAGIC WHERE race_year = 2018;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_driver_standings_2020
# MAGIC AS
# MAGIC SELECT race_year, driver_name, team, total_points, wins, rank
# MAGIC FROM driver_standings
# MAGIC WHERE race_year = 2020;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM v_driver_standings_2018 d_2018
# MAGIC JOIN v_driver_standings_2020 d_2020
# MAGIC ON (d_2018.driver_name = d_2020.driver_name);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM v_driver_standings_2018 d_2018
# MAGIC LEFT JOIN v_driver_standings_2020 d_2020
# MAGIC ON (d_2018.driver_name = d_2020.driver_name);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM v_driver_standings_2018 d_2018
# MAGIC RIGHT JOIN v_driver_standings_2020 d_2020
# MAGIC ON (d_2018.driver_name = d_2020.driver_name);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM v_driver_standings_2018 d_2018
# MAGIC FULL JOIN v_driver_standings_2020 d_2020
# MAGIC ON (d_2018.driver_name = d_2020.driver_name);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM v_driver_standings_2018 d_2018
# MAGIC SEMI JOIN v_driver_standings_2020 d_2020
# MAGIC ON (d_2018.driver_name = d_2020.driver_name);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM v_driver_standings_2018 d_2018
# MAGIC ANTI JOIN v_driver_standings_2020 d_2020
# MAGIC ON (d_2018.driver_name = d_2020.driver_name);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM v_driver_standings_2018 d_2018
# MAGIC CROSS JOIN v_driver_standings_2020 d_2020;
