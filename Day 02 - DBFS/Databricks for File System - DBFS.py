# Databricks notebook source
dbutils.help()

# COMMAND ----------

# %fs
dbutils.fs.help() 
# both gives same output

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mounting ADLS - Any account can be mounted
# MAGIC 1. Access Key - Mounts the entire container, has been deprecated, not secure
# MAGIC 2. SAS Token - Very Specific, fine grained control, gives control for a specific files or folders
# MAGIC 3. Service Principle - Mounts the entire container

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Querying files directly 
# MAGIC There are two options to query files - 
# MAGIC 1. PySpark DataFrame style
# MAGIC 2. Spark SQL Style
# MAGIC #### Using Spark SQL
# MAGIC This will not apply options, you cannot give options like headers 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from json.`dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/constructors.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/circuits.csv`

# COMMAND ----------

df = spark.read.option("header",True).option("inferschema",True).csv("dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/circuits.csv")

# COMMAND ----------

df.display()

# COMMAND ----------


