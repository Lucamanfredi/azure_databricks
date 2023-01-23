# Databricks notebook source
spark.conf.set("fs.azure.account.key.covidprstorageaccount120.blob.core.windows.net", dbutils.secrets.get(scope = "axisCapScope", key = "blobAccountKey"))

# COMMAND ----------

dbutils.fs.mount(
source = "wasbs://population@covidprstorageaccount120.blob.core.windows.net",
mount_point = "/mnt/blobstoragepoc",
extra_configs = 
    {"fs.azure.account.key.covidprstorageaccount120.blob.core.window.net" : dbutils.secrets.get(scope = "axisCapScope", key = "blobAccountKey")})

df = spark.read.text("/mnt/blobstoragepoc/jaffle_shop_orders.csv", header=True)

df.show()

# COMMAND ----------

check = dbutils.secrets.get(scope = "axisCapScope", key = "blobAccountKey")
print(check)

# COMMAND ----------

dbutils.fs.unmount("/mnt/blobstoragepoc")

# COMMAND ----------

dbutils.fs.mount(
source = "wasbs://population@covidprstorageaccount120.blob.core.windows.net",
mount_point = "/mnt/blobstoragepoc",
extra_configs = 
    {"fs.azure.account.key.covidprstorageaccount120.blob.core.window.net":dbutils.secrets.get(scope = "axisCapScope", key = "blobAccountKey")})

df = spark.read.text("/mnt/blobstoragepoc/jaffle_shop_orders.csv", header=True)

df.show()

# COMMAND ----------



# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "<application-id>",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="axisCapScope",key="blobAccountKey"),
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<directory-id>/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/",
  mount_point = "/mnt/<mount-name>",
  extra_configs = configs)

# COMMAND ----------



# COMMAND ----------

storage_account_name = "covidprstorageaccount120"
storage_account_key = "kpC9EETyHxixCNeANCf0rrX9pyH42t1iZ40Alx353LIdeLrOnD8PcYBvJPjTWQkqubonIdvCO1OY+AStKfm1yg=="
container = "population"

# COMMAND ----------

spark.conf.set("fs.azure.account.key.{0}.blob.core.windows.net".format(storage_account_name), storage_account_key)

# COMMAND ----------

dbutils.fs.mount(
 source = "wasbs://{0}@{1}.blob.core.windows.net".format(container, storage_account_name),
 mount_point = "/mnt/blobstoragepoc",
 extra_configs = {"fs.azure.account.key.{0}.blob.core.windows.net".format(storage_account_name): storage_account_key}
)

# COMMAND ----------

df = spark.read.csv("/mnt/<Mount name>/jaffle_shop_orders.csv", header=True)

df.show()

# COMMAND ----------


