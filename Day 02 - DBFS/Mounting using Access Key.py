# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Key
# MAGIC

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://<containername>@<storageaccount>.blob.core.windows.net",
  mount_point = "/mnt/<storageaccount>/<containername>",
  extra_configs = {"fs.azure.account.key.<storageaccount>.blob.core.windows.net":dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")})

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://raw@adlsshelldatabricks.blob.core.windows.net",
  mount_point = "/mnt/adlsshelldatabricks/raw",
  extra_configs = {"fs.azure.account.key.adlsshelldatabricks.blob.core.windows.net":"tJ0eQCyuq1zgc1/2TeJG5+47ZJtnx8JEBt7Nlo07RaBQtXlIt3NWtRqogwYILj5MEgareqxQHhn4+ASt9tTD+Q=="})

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls "dbfs:/mnt/adlsshelldatabricks/raw/inputfiles"

# COMMAND ----------


