# Databricks notebook source
filename=dbutils.widgets.get('filename')
tablename=dbutils.widgets.get('tablename')
tableschema=dbutils.widgets.get('tableschema')

# COMMAND ----------

dbutils.fs.mount(
    source="wasbs://gold@dbtsparksa.blob.core.windows.net",
    mount_point="/mnt/gold",
    extra_configs = {'fs.azure.account.key.dbtsparksa.blob.core.windows.net':dbutils.secrets.get(scope="userScope",key="dbt-spark-sa-access-key")}
)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP table saleslt.sales;

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.unmount('/mnt/silver')

# COMMAND ----------

dbutils.fs.ls('/mnt/bronze/2023-12-19/')

# COMMAND ----------

#Create DB
spark.sql(f'CREATE DATABASE IF NOT EXISTS {tableschema}')

# COMMAND ----------

#Create TABLE
spark.sql("""
          CREATE TABLE IF NOT EXISTS """+tableschema+"""."""+tablename+"""
          USING PARQUET 
          LOCATION '/mnt/bronze/"""+filename+"""/"""+tableschema+"""."""+tablename+""".parquet'
          """)

# COMMAND ----------

spark.sql("""
          CREATE TABLE IF NOT EXISTS SalesLT.Address
          USING PARQUET 
          LOCATION '/mnt/bronze/2023-12-19/SalesLT.Address.parquet'
          """)

spark.sql("""
          CREATE TABLE IF NOT EXISTS SalesLT.Customer
          USING PARQUET 
          LOCATION '/mnt/bronze/2023-12-19/SalesLT.Customer.parquet'
          """)         


          
spark.sql("""
          CREATE TABLE IF NOT EXISTS SalesLT.CustomerAddress
          USING PARQUET 
          LOCATION '/mnt/bronze/2023-12-19/SalesLT.CustomerAddress.parquet'
          """)   
spark.sql("""
          CREATE TABLE IF NOT EXISTS SalesLT.Product
          USING PARQUET 
          LOCATION '/mnt/bronze/2023-12-19/SalesLT.Product.parquet'
          """)   
spark.sql("""
          CREATE TABLE IF NOT EXISTS SalesLT.ProductCategory
          USING PARQUET 
          LOCATION '/mnt/bronze/2023-12-19/SalesLT.ProductCategory.parquet'
          """)   
spark.sql("""
          CREATE TABLE IF NOT EXISTS SalesLT.ProductDescription
          USING PARQUET 
          LOCATION '/mnt/bronze/2023-12-19/SalesLT.ProductDescription.parquet'
          """)   
spark.sql("""
          CREATE TABLE IF NOT EXISTS SalesLT.ProductModel
          USING PARQUET 
          LOCATION '/mnt/bronze/2023-12-19/SalesLT.ProductModel.parquet'
          """) 
spark.sql("""
          CREATE TABLE IF NOT EXISTS SalesLT.ProductModelProductDescription
          USING PARQUET 
          LOCATION '/mnt/bronze/2023-12-19/SalesLT.ProductModelProductDescription.parquet'
          """)    

spark.sql("""
          CREATE TABLE IF NOT EXISTS SalesLT.SalesOrderDetail
          USING PARQUET 
          LOCATION '/mnt/bronze/2023-12-19/SalesLT.SalesOrderDetail.parquet'
          """)

spark.sql("""
          CREATE TABLE IF NOT EXISTS SalesLT.SalesOrderHeader
          USING PARQUET 
          LOCATION '/mnt/bronze/2023-12-19/SalesLT.SalesOrderHeader.parquet'
          """)              
