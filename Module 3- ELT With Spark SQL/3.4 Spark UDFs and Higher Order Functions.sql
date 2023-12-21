-- Databricks notebook source
-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %md ##1.Filter

-- COMMAND ----------

select * from orders;

-- COMMAND ----------

select order_id,books,filter(books, i -> i.quantity>=2 ) as multiple_copies from orders;

-- COMMAND ----------

select order_id,multiple_copies 
from 
(select order_id,books,filter(books,i->i.quantity>=2 ) as multiple_copies from orders
)
where size(multiple_copies) > 0;

-- COMMAND ----------

-- MAGIC %md ##2.Transform

-- COMMAND ----------

select order_id,books, transform(books, b -> cast(b.subtotal*0.8 as double)) as discount from orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## User Defined Functions (UDF)

-- COMMAND ----------

CREATE OR REPLACE FUNCTION get_url(email STRING)
RETURNS STRING

RETURN concat("https://www.", split(email, "@")[1])

-- COMMAND ----------

SELECT email, get_url(email) domain
FROM customers

-- COMMAND ----------

DESCRIBE FUNCTION get_url

-- COMMAND ----------

DESCRIBE FUNCTION EXTENDED get_url

-- COMMAND ----------

CREATE FUNCTION site_type(email STRING)
RETURNS STRING
RETURN CASE 
          WHEN email like "%.com" THEN "Commercial business"
          WHEN email like "%.org" THEN "Non-profits organization"
          WHEN email like "%.edu" THEN "Educational institution"
          ELSE concat("Unknow extenstion for domain: ", split(email, "@")[1])
       END;

-- COMMAND ----------

SELECT email,get_url(email),site_type(email) as domain_category
FROM customers

-- COMMAND ----------

SELECT email, site_type(email) as domain_category
FROM customers

-- COMMAND ----------

DROP FUNCTION get_url;
DROP FUNCTION site_type;
