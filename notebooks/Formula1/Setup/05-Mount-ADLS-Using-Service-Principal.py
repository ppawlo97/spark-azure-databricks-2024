# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake using Service Principal
# MAGIC 1. Get client_id, tenant_id and client_secret from key vault
# MAGIC 2. Set Spark Config with App/Client Id, Directory/Tenant Id & Secret
# MAGIC 3. Call FS utility to mount the storage
# MAGIC 4. Explore other FS utilities related to mount
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

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": CLIENT_ID,
    "fs.azure.account.oauth2.client.secret": CLIENT_SECRET,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token"
}

dbutils.fs.mount(
    source=f"abfss://demo@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/",
    mount_point=f"/mnt/{STORAGE_ACCOUNT_NAME}/demo",
    extra_configs=configs
)

# COMMAND ----------

# MAGIC %md
# MAGIC Mount other containers that will be used throughout the project

# COMMAND ----------

for container_name in ["raw", "processed", "presentation"]:
    dbutils.fs.mount(
        source=f"abfss://{container_name}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/",
        mount_point=f"/mnt/{STORAGE_ACCOUNT_NAME}/{container_name}",
        extra_configs=configs
)

# COMMAND ----------

fpaths = []
for file in dbutils.fs.ls(f"/mnt/{STORAGE_ACCOUNT_NAME}/demo"):
    print(file.name)
    fpaths.append(file.path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load data

# COMMAND ----------

sdf = spark.read.csv(fpaths[0])

# COMMAND ----------

sdf.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Other mount-related commands

# COMMAND ----------

display(dbutils.fs.mounts())
