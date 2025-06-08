# Databricks notebook source
# MAGIC %md
# MAGIC #Incremental Data Loading
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists netflix_catalog.netsflixschema

# COMMAND ----------

checkpoint_location ="abfss://silver@projectnetflix.dfs.core.windows.net/checkpoint"

# COMMAND ----------

df = spark.readStream.format("cloudFiles")\
                .option("cloudFiles.format", "csv")\
                .option("cloudFiles.schemaLocation", checkpoint_location)\
                .load("abfss://raw@projectnetflix.dfs.core.windows.net")

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------


df.writeStream.format("delta")\
    .option("checkpointLocation", checkpoint_location)\
    .trigger(once=True)\
    .start("abfss://bronze@projectnetflix.dfs.core.windows.net/netflix_titles")