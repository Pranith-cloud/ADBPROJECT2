# Databricks notebook source
# MAGIC %md
# MAGIC #Silver Notebook Lookup Tables
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #Parameters
# MAGIC

# COMMAND ----------


dbutils.widgets.text("sourcefolder","netflix_directors")
dbutils.widgets.text("targetfolder","netflix_directors")

# COMMAND ----------

dbutils.widgets.get("sourcefolder")


# COMMAND ----------

# MAGIC %md
# MAGIC #Variables

# COMMAND ----------

src_data = dbutils.widgets.get("sourcefolder")
tgt_data = dbutils.widgets.get("targetfolder")

# COMMAND ----------

src_data

# COMMAND ----------

df = spark.read.format("csv")\
      .option("header","true")\
      .option("inferSchema","true")\
      .load(f"abfs://bronze@projectnetflix.dfs.core.windows.net/{src_data}")

# COMMAND ----------

df.write.format("delta")\
    .mode("append")\
    .option("path",f"abfs://silver@projectnetflix.dfs.core.windows.net/{tgt_data}")\
    .save()

# COMMAND ----------

