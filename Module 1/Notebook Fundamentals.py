# Databricks notebook source
print("Hello")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 'test_row';

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Run Another notebook Here

# COMMAND ----------

# MAGIC %run ./Setup

# COMMAND ----------

# MAGIC %fs ls '/'

# COMMAND ----------

# MAGIC %fs ls '/databricks-datasets/'

# COMMAND ----------

files=dbutils.fs.ls('/databricks-datasets/COVID/')
display(files)

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.secrets.help()
