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
spark.conf.set("fs.azure.account.key.{0}.blob.core.windows.net".format(storage_account_name), dbutils.secrets.get("axisCapScope", "blobAccountKey"))

# COMMAND ----------

# dbutils.fs.mounts()

# COMMAND ----------

# # mount Blob Storage in Azure Databricks
# dbutils.fs.mount(
#  source = "wasbs://{0}@{1}.blob.core.windows.net".format(container, storage_account_name),
#  mount_point = "/mnt/blobstoragepoc",
#  extra_configs = {"fs.azure.account.key.{0}.blob.core.windows.net".format(storage_account_name): storage_account_key}
# )

# COMMAND ----------

# This command reads jaffle_shop_orders.csv and writes it into a blob container folder named dbxoutput
(spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .csv("/mnt/blobstoragepoc/jaffle_shop_orders.csv")
      .write.mode("overwrite") # "overwrite" will replace older versions of the table with the latest one. The command "append" would populate the folder with additional files.
      .format("csv")
      .save("/mnt/blobstoragepoc/dbxoutput/" ))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.ls('/mnt/blobstoragepoc/partial-load-poc/')

# COMMAND ----------

def get_dir_content(ls_path):
    for dir_path in dbutils.fs.ls(ls_path):
        if dir_path.isFile():
            yield dir_path.path
        elif dir_path.isDir() and ls_path != dir_path.path:
            yield from get_dir_content(dir_path.path)

# create dataframe from list of objects
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

sklist = list(get_dir_content('/mnt/blobstoragepoc/partial-load-poc/'))
df = spark.createDataFrame(sklist, StringType())
display(df)

# COMMAND ----------

df.createOrReplaceTempView("CSET_SOURCE1")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- # create or replace table bronze
# MAGIC -- # (
# MAGIC -- # value string,
# MAGIC -- # timestamp timestamp
# MAGIC -- # )
# MAGIC -- # using delta
# MAGIC 
# MAGIC -- drop table bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into bronze
# MAGIC select a.*, current_timestamp() as timestamp
# MAGIC from CSET_SOURCE1 a
# MAGIC left join bronze b 
# MAGIC   on b.value = a.value
# MAGIC where b.value is null;
# MAGIC --where a.value not in (select distinct value from bronze);
# MAGIC 
# MAGIC select * from bronze

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

 # dbutils.fs.unmount("/mnt/blobstoragepoc")
