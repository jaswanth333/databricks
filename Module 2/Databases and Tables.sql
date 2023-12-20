-- Databricks notebook source
-- MAGIC %md ## Default DB

-- COMMAND ----------

create table internal_table (id int);

-- COMMAND ----------

insert into internal_table values(1);

-- COMMAND ----------

DESCRIBE EXTENDED internal_table;

--LOCATION IS dbfs:/user/hive/warehouse/internal_table

-- COMMAND ----------

create table external_table (id int)
location 'dbfs:/mnt/demo/external_table';

-- COMMAND ----------

insert into external_table values(1);

-- COMMAND ----------

DESCRIBE EXTENDED external_table;
--dbfs:/mnt/demo/external_table

-- COMMAND ----------

DROP table external_table;
DROP table internal_table;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/internal_table'

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/external_table'

-- COMMAND ----------

-- MAGIC %md ##Create Schema/Database

-- COMMAND ----------

CREATE DATABASE new_default;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED  new_default;

-- COMMAND ----------

USE new_default;

-- COMMAND ----------

create table manged_new_default (id int,age int);
insert into manged_new_default values (1,20);


-- COMMAND ----------

create table external_new_default (id int,age int) location 'dbfs:/mnt/demo/external_new_default';
insert into external_new_default values (1,20);


-- COMMAND ----------

describe extended manged_new_default;
--dbfs:/user/hive/warehouse/new_default.db/manged_new_default


-- COMMAND ----------

describe extended external_new_default;


-- COMMAND ----------

DROP TABLE manged_new_default;
DROP TABLE external_new_default;

-- COMMAND ----------

-- MAGIC %md ##Create Schema/Database Outside Hive Directory

-- COMMAND ----------

create schema custom location 'dbfs:/mnt/custom.db';

-- COMMAND ----------

describe database extended custom;

-- COMMAND ----------


