-- Databricks notebook source
-- MAGIC %run ./Includes/Copy-Datasets

-- COMMAND ----------

SET datasets.path=dbfs:/mnt/demo-datasets/bookstore;

-- COMMAND ----------

-- MAGIC %md ##Bronze
-- MAGIC
-- MAGIC ###orders_raw(Live streaming table)

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE orders_raw
AS SELECT * FROM cloud_files("${datasets.path}/orders-json-raw", "json",
map("cloudFiles.inferColumnTypes", "true"));

-- COMMAND ----------

-- MAGIC %md ###Customers - live table

-- COMMAND ----------

create or refresh live table customers
as select * from json.`${datasets.path}/customers-json`

-- COMMAND ----------

-- MAGIC %md ##Silver
-- MAGIC
-- MAGIC ###

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE orders_cleaned(CONSTRAINT valid_order_no EXPECT(order_id IS NOT NULL) ON VIOLATION DROP ROW)
AS
  SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
         cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp, o.books,
         c.profile:address:country as country
  FROM STREAM(LIVE.orders_raw) o LEFT JOIN LIVE.customers c ON c.customer_id=o.customer_id

-- COMMAND ----------

-- MAGIC %md ##Gold

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE cn_daily_customer_books
COMMENT "Daily number of books per customer in China"
AS
  SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
  FROM LIVE.orders_cleaned
  WHERE country = "China"
  GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)
