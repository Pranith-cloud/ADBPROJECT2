# Databricks notebook source
looktables_rule = {
    "rule1" : "showid is NOT NULL"
}

# COMMAND ----------

@dlt.table(
    name = "gold_netflixdirectors"
)
@dlt.expect_all_or_drop(looktables_rule)
def myfunc():
    df = spark.readstream.format("delta")\
             .load("abfss://silver@projectnetflix.dfs.core.windows.net/netflix_directors")
    return df

# COMMAND ----------

@dlt.table(
    name = "gold_category"
)
@dlt.expect_all_or_drop(looktables_rule)
def myfunc():
    df = spark.readstream.format("delta")\
             .load("abfss://silver@projectnetflix.dfs.core.windows.net/netflix_category")
    return df

# COMMAND ----------

@dlt.table(
    name = "gold_netflixcountries"
)
@dlt.expect_all_or_drop(looktables_rule)
def myfunc():
    df = spark.readstream.format("delta")\
             .load("abfss://silver@projectnetflix.dfs.core.windows.net/netflix_countries")
    return df

# COMMAND ----------

@dlt.table(
    name = "gold_netflixcast"
)
@dlt.expect_all_or_drop(looktables_rule)
def myfunc():
    df = spark.readstream.format("delta")\
             .load("abfss://silver@projectnetflix.dfs.core.windows.net/netflix_cast")
    return df

# COMMAND ----------

@dlt.table(
    name = "gold_netflixtitles"
)
@dlt.expect_all_or_drop(looktables_rule)
def myfunc():
    df = spark.readstream.format("delta")\
             .load("abfss://silver@projectnetflix.dfs.core.windows.net/netflix_titles")
    return df