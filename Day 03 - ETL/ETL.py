# Databricks notebook source
# MAGIC %fs ls  dbfs:/mnt/adlsshelldatabricks

# COMMAND ----------

# MAGIC %fs ls  dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles

# COMMAND ----------

# MAGIC %md
# MAGIC #### Working with CSVs
# MAGIC - Dataframe is partition and distributed data. Its is not same as a table. Tables are not partitioned and distributed

# COMMAND ----------

# 1. Read the csv
df = spark.read.option("header",True).option('inferSchema', True).csv("dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/circuits.csv")

# COMMAND ----------

df.show() #this is an action

# COMMAND ----------

df.display() #this is an action

# COMMAND ----------

# 1. Read the csv
df = spark.read.csv("dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/circuits.csv", header = True, inferSchema= True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Transformation 
# MAGIC
# MAGIC ##### Dataframe functions 
# MAGIC - Always have a dot (.) for its star
# MAGIC - examples: select, alias
# MAGIC - 
# MAGIC
# MAGIC ##### Pyspark Functions
# MAGIC - can be called anytime
# MAGIC - examples: col()

# COMMAND ----------

df.select("circuitId","location").display()

# COMMAND ----------

from pyspark.sql.functions import * 
df1 = df.select(col("circuitId").alias("circuit_id"), "*")

# COMMAND ----------

df1.display()

# COMMAND ----------

df.withColumnRenamed("circuitId", "circuit_id").display()

# COMMAND ----------

df.withColumn("location",concat("location", "country")).display()

# COMMAND ----------

df.withColumn("newcolumn",concat("location"," ","country")).display()

# COMMAND ----------

df1.write.parquet("dbfs:/mnt/adlsshelldatabricks/raw/processed/saumya/circuit")

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema saumya
# MAGIC

# COMMAND ----------

df1.write.saveAsTable("saumya.circuit")
