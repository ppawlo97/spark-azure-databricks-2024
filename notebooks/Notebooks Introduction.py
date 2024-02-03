# Databricks notebook source
# MAGIC %md
# MAGIC # Notebooks Introduction
# MAGIC ## UI Introduction
# MAGIC ### Magic Commands
# MAGIC * %python
# MAGIC * %sql
# MAGIC * %scala
# MAGIC * %md
# MAGIC * %fs
# MAGIC * %sh

# COMMAND ----------

message = "Welcome to the Databricks Notebook Experience"
print(message)

# COMMAND ----------

# MAGIC %md
# MAGIC **Note:** Different runtimes can be used for each cell

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Hello"

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

# MAGIC %md
# MAGIC **Note:** Both the file system and shell can be easily accessed through magic commands 

# COMMAND ----------

# MAGIC %sh
# MAGIC ps
