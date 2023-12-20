-- Databricks notebook source
-- MAGIC %sql
-- MAGIC DESCRIBE HISTORY demo_table;

-- COMMAND ----------

-- MAGIC %md ##Time Travel

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from demo_table version as of 1;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from demo_table version as of 2;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from demo_table @V1;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DELETE from demo_table;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from demo_table;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC RESTORE TABLE demo_table TO VERSION AS OF 1;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DESCRIBE HISTORY demo_table;

-- COMMAND ----------

-- MAGIC %md ## Optimize Multiple Files into less files

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC optimize demo_table zorder by (id)

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DESCRIBE HISTORY demo_table;

-- COMMAND ----------

-- MAGIC %md ## Remove Unused Files

-- COMMAND ----------

VACUUM demo_table;

-- COMMAND ----------

DROP TABLE demo_table;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/demo_table'

-- COMMAND ----------

DESCRIBE HISTORY demo_table;
