# Databricks notebook source
# MAGIC %md
# MAGIC # Data Exploration
# MAGIC This notebook performs exploratory data analysis on the dataset.
# MAGIC To expand on the analysis, attach this notebook to the **job-161869107836550-run-5403-Shared_job_cluster** cluster,
# MAGIC edit [the options of pandas-profiling](https://pandas-profiling.ydata.ai/docs/master/rtd/pages/advanced_usage.html), and rerun it.
# MAGIC - Explore completed trials in the [MLflow experiment](#mlflow/experiments/1005448801907223)
# MAGIC - Navigate to the parent notebook [here](#notebook/1005448801907224) (If you launched the AutoML experiment using the Experiments UI, this link isn't very useful.)
# MAGIC 
# MAGIC Runtime Version: _11.3.x-cpu-ml-scala2.12_

# COMMAND ----------

import os
import uuid
import shutil
import pandas as pd
import databricks.automl_runtime

from mlflow.tracking import MlflowClient

# Download input data from mlflow into a pandas DataFrame
# Create temporary directory to download data
temp_dir = os.path.join(os.environ["SPARK_LOCAL_DIRS"], "tmp", str(uuid.uuid4())[:8])
os.makedirs(temp_dir)

# Download the artifact and read it
client = MlflowClient()
training_data_path = client.download_artifacts("6c501e0e663141f9bdb4054443821427", "data", temp_dir)
df = pd.read_parquet(os.path.join(training_data_path, "training_data"))

# Delete the temporary data
shutil.rmtree(temp_dir)

target_col = "churn"

# Drop columns created by AutoML before pandas-profiling
df = df.drop(['_automl_split_col_5380'], axis=1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Semantic Type Detection Alerts
# MAGIC 
# MAGIC For details about the definition of the semantic types and how to override the detection, see
# MAGIC [Databricks documentation on semantic type detection](https://docs.microsoft.com/azure/databricks/applications/machine-learning/automl#semantic-type-detection).
# MAGIC 
# MAGIC - Semantic type `categorical` detected for columns `age_group`, `event_count`, `gender`, `order_count`, `session_count`. Training notebooks will encode features based on categorical transformations.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Truncate rows
# MAGIC Only the first 10000 rows will be considered for pandas-profiling to avoid out-of-memory issues.
# MAGIC Comment out next cell and rerun the notebook to profile the full dataset.

# COMMAND ----------

df = df.iloc[:10000, :]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Profiling Results

# COMMAND ----------

from pandas_profiling import ProfileReport
df_profile = ProfileReport(df, title="Profiling Report", progress_bar=False, infer_dtypes=False)
profile_html = df_profile.to_html()

displayHTML(profile_html)
