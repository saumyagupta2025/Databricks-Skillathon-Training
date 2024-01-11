# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/

# COMMAND ----------

df = spark.read.json("dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/constructors.json")

# COMMAND ----------

from pyspark.sql.functions import *
df1 = df.withColumn("ingestiondate", current_timestamp())

# COMMAND ----------

df1.write.saveAsTable("saumya.constructor")

# COMMAND ----------

# MAGIC %sql 
# MAGIC Create table saumya.constructors as 
# MAGIC select * from json.`dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/constructors.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.saumya.constructors

# COMMAND ----------


