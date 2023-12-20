-- Databricks notebook source
-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
-- MAGIC ##dbfs:/mnt/demo-datasets/bookstore

-- COMMAND ----------

-- MAGIC %md ##Reading JSON files from Storage(NON DELTA)

-- COMMAND ----------

SELECT * from json. `dbfs:/mnt/demo-datasets/bookstore/customers-json/export_001.json`

-- COMMAND ----------

-- MAGIC %md ##Read Multiple Files with wildcard

-- COMMAND ----------

SELECT * from json. `/mnt/demo-datasets/bookstore/customers-json/export*.json`

-- COMMAND ----------

-- MAGIC %md ##Read All Files In directory(assuming same format)

-- COMMAND ----------

-- MAGIC %md ###JSON Format

-- COMMAND ----------

SELECT * from json. `/mnt/demo-datasets/bookstore/customers-json/`

-- COMMAND ----------

select *,input_file_name() as source_file from json. `/mnt/demo-datasets/bookstore/customers-json/`

-- COMMAND ----------

-- MAGIC %md ##Use text for reading JSON/CSV/TSV

-- COMMAND ----------

SELECT * from text. `/mnt/demo-datasets/bookstore/customers-json/`

-- COMMAND ----------

SELECT * from binaryFile. `/mnt/demo-datasets/bookstore/customers-json/`

-- COMMAND ----------

-- MAGIC %md ##CSV Format

-- COMMAND ----------

select * from csv. `/mnt/demo-datasets/bookstore/books-csv/`

-- COMMAND ----------

-- MAGIC %md #### Solution(NOT DELTA)

-- COMMAND ----------

create table books_csv(id string,title string,author string,category string,price double)
using csv
options(
  header='true',
  delimiter=';'
)
location '/mnt/demo-datasets/bookstore/books-csv/'

-- COMMAND ----------

select * from books_csv;

-- COMMAND ----------

DESCRIBE EXTENDED books_csv;

-- COMMAND ----------

-- MAGIC %md ####Disadvantage of not having delta format

-- COMMAND ----------

-- MAGIC %fs ls '/mnt/demo-datasets/bookstore/books-csv/'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #ADD NEW CSV FILE TO ABOVE DIRECTORY
-- MAGIC (spark.read.table("books_csv")
-- MAGIC  .write.mode("append")
-- MAGIC .format("csv")
-- MAGIC .option('header', 'true')
-- MAGIC .option('delimiter', ';')
-- MAGIC .save("/mnt/demo-datasets/bookstore/books-csv"))
-- MAGIC

-- COMMAND ----------

-- MAGIC %fs ls '/mnt/demo-datasets/bookstore/books-csv/'

-- COMMAND ----------

-- MAGIC %md #####New rows are not reflected

-- COMMAND ----------

SELECT COUNT(*) FROM books_csv;


-- COMMAND ----------

-- MAGIC %md #####Solution - Manually Refresh(For Large Data it  takes so much time)

-- COMMAND ----------

REFRESH TABLE books_csv;
SELECT COUNT(*) FROM books_csv;

-- COMMAND ----------

-- MAGIC %md ## Create Delta TABLE From External Storage(Works for JSON/Parquet but not CSV)

-- COMMAND ----------

create table customers as select * from json. `/mnt/demo-datasets/bookstore/customers-json/`;

-- COMMAND ----------

describe extended customers;
--provider Delta

-- COMMAND ----------

create table customers as select * from json. `/mnt/demo-datasets/bookstore/customers-json/`;

-- COMMAND ----------

-- MAGIC %md ## Create Delta TABLE From External Storage(FOR CSV)

-- COMMAND ----------

create temp view books_tmp_view(id string,title string,author string,category string,price double)
using csv
options(
  header='true',
  delimiter=';',
  path='/mnt/demo-datasets/bookstore/books-csv/'
);

create table books as select * from books_tmp_view;

select * from books;


-- COMMAND ----------

describe extended books;
--data type:DELTA

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------


