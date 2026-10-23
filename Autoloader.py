# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### AUTOLOADER

# COMMAND ----------

# MAGIC %md
# MAGIC **Streaming Dataframe**

# COMMAND ----------

base = "abfss://mycontainer@vivstandardisedzone.dfs.core.windows.net"

df = (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", f"{base}/autosink/schema")
    .load(f"{base}/autosource"))

(df.writeStream.format("parquet")
    .option("checkpointLocation", f"{base}/autosink/checkpoint")
    .trigger(availableNow=True)
    .start(f"{base}/autosink/data"))