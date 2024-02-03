# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Magic Command - %fs

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /databricks-datasets

# COMMAND ----------

# MAGIC %md
# MAGIC **Note:** Use `dbutils` package

# COMMAND ----------

dbutils.fs.ls("/")

# COMMAND ----------

for files in dbutils.fs.ls("/databricks-datasets/COVID"):
    if files.name.endswith("/"):
        print(files.name)

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help("ls")
