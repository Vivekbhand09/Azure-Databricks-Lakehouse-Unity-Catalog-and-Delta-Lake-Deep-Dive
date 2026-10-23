# Databricks notebook source
# MAGIC %md
# MAGIC ## Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE man_cata.man_schema.deltatbl
# MAGIC (
# MAGIC     id INT,
# MAGIC     name STRING,
# MAGIC     city STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://mycontainer@vivstandardisedzone.dfs.core.windows.net/deltalake/deltatbl'

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE man_cata.man_schema.deltatbl SET TBLPROPERTIES ('delta.enableDeletionVectors'=false);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO man_cata.man_schema.deltatbl
# MAGIC VALUES
# MAGIC (1,'aa','delhi'),
# MAGIC (2,'bb','mumbai'),
# MAGIC (3,'cc','chennai')

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED man_cata.man_schema.deltatbl

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from man_cata.man_schema.deltatbl

# COMMAND ----------

# MAGIC %md
# MAGIC ## UPDATES IN DELTA TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE man_cata.man_schema.deltatbl
# MAGIC SET city='toronto' where id=1

# COMMAND ----------

# MAGIC %md
# MAGIC Here before update in azure table path there was only parquet file which has 3 entries right but after update was happened there was 2 files one which has latest data and one which has previous data (before update ) and both files are in parquet format . 
# MAGIC So as we know delta table format are built on top of parquet files but with additional functionalities and its one of feature is time travel means we can see that past data using versioning because all operation happened on that delta table it stores that data in paruqet so we can easily get those data . 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY man_cata.man_schema.deltatbl

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time Travel 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE man_cata.man_schema.deltatbl TO VERSION AS OF 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM man_cata.man_schema.deltatbl

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deletion Vector
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE man_cata.man_schema.deltatbl2
# MAGIC (
# MAGIC     id INT,
# MAGIC     name STRING,
# MAGIC     city STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://mycontainer@vivstandardisedzone.dfs.core.windows.net/deltalake/deltatbl2'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO man_cata.man_schema.deltatbl2
# MAGIC VALUES
# MAGIC (1,'aa','delhi'),
# MAGIC (2,'bb','mumbai'),
# MAGIC (3,'cc','chennai')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Updates in Deletion Vector Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE man_cata.man_schema.deltatbl2
# MAGIC SET city='seattle' where id=1

# COMMAND ----------

display(dbutils.fs.ls("abfss://mycontainer@vivstandardisedzone.dfs.core.windows.net/deltalake/deltatbl2"))

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE man_cata.man_schema.deltatbl2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deep Clone V/S Shallow Clone

# COMMAND ----------

# MAGIC %md
# MAGIC ### DEEP CLONE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE man_cata.man_schema.deepclonetbl
# MAGIC DEEP CLONE man_cata.man_schema.deltatbl;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from man_cata.man_schema.deepclonetbl

# COMMAND ----------

# MAGIC %md
# MAGIC ### SHALLOW CLONE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE man_cata.man_schema.shallowclonetbl
# MAGIC DEEP CLONE man_cata.man_schema.man_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from man_cata.man_schema.shallowclonetbl