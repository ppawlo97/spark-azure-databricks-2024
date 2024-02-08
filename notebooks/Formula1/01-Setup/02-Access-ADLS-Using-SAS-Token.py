# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using SAS token
# MAGIC 1. Set the SparkConfig
# MAGIC 2. List files from `demo` container
# MAGIC 3. Read data from `circuits.csv` file
# MAGIC
# MAGIC
# MAGIC ##### Setup

# COMMAND ----------

# The following should be hidden in environmental variables
STORAGE_ACCOUNT_NAME = ""
SAS_TOKEN = ""

# COMMAND ----------

# MAGIC %md
# MAGIC #### Spark Configuration

# COMMAND ----------

spark.conf.set(
    f"fs.azure.account.auth.type.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net", "SAS"
)

spark.conf.set(
    f"fs.azure.sas.token.provider.type.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider"
)

spark.conf.set(
    f"fs.azure.sas.fixed.token.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net", SAS_TOKEN
)

# COMMAND ----------

fpaths = []
for file in dbutils.fs.ls(f"abfss://demo@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"):
    print(file.name)
    fpaths.append(file.path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load data

# COMMAND ----------

sdf = spark.read.csv(fpaths[0])

# COMMAND ----------

sdf.count()
