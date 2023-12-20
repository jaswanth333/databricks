-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Create Delta Table

-- COMMAND ----------

-- MAGIC %sql 
-- MAGIC
-- MAGIC create table demo_table (id int,name string);
-- MAGIC

-- COMMAND ----------

-- MAGIC %sql 
-- MAGIC
-- MAGIC insert into table demo_table values(1,'A'),(2,'B'),(3,'C'),(4,'E'),(5,'F');

-- COMMAND ----------

DESCRIBE DETAIL demo_table;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/demo_table'

-- COMMAND ----------

UPDATE demo_table set name='5th row' where id= 5;

-- COMMAND ----------

DESCRIBE DETAIL demo_table;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/demo_table'

-- COMMAND ----------

DESCRIBE HISTORY demo_table;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/demo_table/_delta_log/'

-- COMMAND ----------

-- MAGIC %fs head 'dbfs:/user/hive/warehouse/demo_table/_delta_log/00000000000000000002.json'
