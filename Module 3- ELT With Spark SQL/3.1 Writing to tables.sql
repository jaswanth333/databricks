-- Databricks notebook source
-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

create table orders as select * from parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

select * from orders;

-- COMMAND ----------

-- MAGIC %md ##Overwrite DELTA tables(Creates new version)

-- COMMAND ----------

-- MAGIC %md ####Method 1

-- COMMAND ----------

create or replace table orders as select * from parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

DESCRIBE HISTORY orders;

-- COMMAND ----------

-- MAGIC %md ####Method2(Insert overwrites on existing table only- Recommended)

-- COMMAND ----------

insert overwrite orders select * from parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

DESCRIBE HISTORY orders;

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT *, current_timestamp() FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

-- MAGIC %md ###Append Data(Creates Duplicates)

-- COMMAND ----------

INSERT INTO orders
SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

SELECT count(*) FROM orders

-- COMMAND ----------

-- MAGIC %md ###Merging Data(Avoid Duplicates and peforms inserts and updates)

-- COMMAND ----------

-- MAGIC %md ####Example 1

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW customers_updates AS 
SELECT * FROM json.`${dataset.bookstore}/customers-json-new`;

MERGE INTO customers c
USING customers_updates u
ON c.customer_id = u.customer_id
WHEN MATCHED AND c.email IS NULL AND u.email IS NOT NULL THEN
  UPDATE SET email = u.email, updated = u.updated
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- MAGIC %md ####Example 2

-- COMMAND ----------

create or replace temp view book_updates (id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS(
  header='true',
  delimiter=';',
  path='${dataset.bookstore}/books-csv'
);

select * from book_updates;

-- COMMAND ----------

MERGE INTO books b USING book_updates bu ON b.id=bu.id AND b.title = bu.title
WHEN NOT MATCHED AND bu.category = 'Computer Science' THEN 
  INSERT *

-- COMMAND ----------


