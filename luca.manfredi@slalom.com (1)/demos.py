# Databricks notebook source
pip install https://github.com/databricks-demos/dbdemos/raw/main/release/dbdemos-0.1-py3-none-any.whl --force

# COMMAND ----------

import dbdemos
dbdemos.help()
dbdemos.list_demos()

dbdemos.install('lakehouse-retail-churn', path='./', overwrite = True)

# COMMAND ----------

dbdemos.install('auto-loader')

# COMMAND ----------

dbdemos.install('dlt-loans')

# COMMAND ----------

dbdemos.install('lakehouse-retail-churn', overwrite=True)

# COMMAND ----------


