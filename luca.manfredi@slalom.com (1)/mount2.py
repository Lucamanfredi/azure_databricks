# Databricks notebook source
# check that you are able to retrieve the azure account key
check = dbutils.secrets.get(scope = "axisCapScope", key = "blobAccountKey")
print(check)

# COMMAND ----------

# Create variables
storage_account_name = "covidprstorageaccount120"
storage_account_key = dbutils.secrets.get(scope = "axisCapScope", key = "blobAccountKey")
container = "population"

# COMMAND ----------

# set a Spark config to point to your instance of Azure Blob Storage.
spark.conf.set("fs.azure.account.key.{0}.blob.core.windows.net".format(storage_account_name), storage_account_key)

# COMMAND ----------

# mount Blob Storage in Azure Databricks
dbutils.fs.mount(
 source = "wasbs://{0}@{1}.blob.core.windows.net".format(container, storage_account_name),
 mount_point = "/mnt/blobstoragepoc",
 extra_configs = {"fs.azure.account.key.{0}.blob.core.windows.net".format(storage_account_name): storage_account_key}
)

df = spark.read.csv("/mnt/blobstoragepoc/jaffle_shop_orders.csv", header=True)

df.show()

# COMMAND ----------

spark_df = spark.read.format('csv').\
option('header', True).\
load("wasbs://population@covidprstorageaccount120.\        blob.core.windows.net/jaffle_shop_orders.csv")
print(spark_df.show())

# COMMAND ----------

# dbutils.fs.unmount("/mnt/blobstoragepoc")

# COMMAND ----------

df.head(5)

# COMMAND ----------

df.write.saveAsTable("jaffle_shop_orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.jaffle_shop_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

##########

# COMMAND ----------

spark.conf.set("fs.azure.account.key")

# COMMAND ----------



# COMMAND ----------


