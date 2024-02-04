# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using Service Principal
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate a secret/password for the Application
# MAGIC 3. Set Spark Config with App/Client Id, Directory/Tenant Id & Secret
# MAGIC 4. Assign Role 'Storage Blob Data Contributor` to the Data Lake
# MAGIC 5. Use secrets as well
# MAGIC
# MAGIC
# MAGIC ##### Setup

# COMMAND ----------

SCOPE = "formula1-scope"

CLIENT_ID = dbutils.secrets.get(scope=SCOPE, key="formula1dl-client-id")
TENANT_ID = dbutils.secrets.get(scope=SCOPE, key="formula1dl-tenant-id")
CLIENT_SECRET = dbutils.secrets.get(scope=SCOPE, key="formula1dl-client-secret")
STORAGE_ACCOUNT_NAME = dbutils.secrets.get(scope=SCOPE, key="formula1dl-storage-account-name")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Spark Configuration

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net", "OAuth")

spark.conf.set(f"fs.azure.account.oauth.provider.type.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")

spark.conf.set(f"fs.azure.account.oauth2.client.id.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net", CLIENT_ID)

spark.conf.set(f"fs.azure.account.oauth2.client.secret.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net", CLIENT_SECRET)

spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net", f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token")

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
