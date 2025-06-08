# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format("delta")\
       .option("heder",True)\
        .option("inferSchema",True)\
        .load("abfss://bronze@projectnetflix.dfs.core.windows.net/netflix_titles")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.limit(20).display()

# COMMAND ----------

df = df.fillna({"duration_minutes": 0, "duration_seasons": 1})

# COMMAND ----------

df.limit(20).display()

# COMMAND ----------

df = df.filter(col("type") != "1944")

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

df = df.withColumn("duration_rank", dense_rank().over(Window.orderBy(col("duration_minutes").desc())))

# COMMAND ----------

df.limit(20).display()

# COMMAND ----------

df = df.withColumn("duration_minutes", col("duration_minutes").cast(IntegerType()))\
     .withColumn("duration_seasons", col("duration_seasons").cast(IntegerType()))

# COMMAND ----------

df.limit(20).display()

# COMMAND ----------

df = df.withColumn("type_flg",when(col("type")=="Movie",1)\
                       .when(col("type")=="Tv Show",2)\
                           .otherwise(0))

# COMMAND ----------

df = df.groupBy("type").agg(count("*").alias("type_count"))
df.display()

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
    .option("path","abfss://silver@projectnetflix.dfs.core.windows.net/netflix_titles")\
    .save()