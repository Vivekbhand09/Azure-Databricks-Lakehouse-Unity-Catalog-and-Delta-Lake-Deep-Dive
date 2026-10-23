# Databricks notebook source
# MAGIC %md
# MAGIC ### Scenario - 1
# MAGIC **-Managed Catalog**
# MAGIC **-Managed Schema**
# MAGIC **-Managed Table**

# COMMAND ----------

# MAGIC %md
# MAGIC **Managed Catalog**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG man_cata

# COMMAND ----------

# MAGIC %md
# MAGIC **Managed Schema/Database**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA man_cata.man_schema

# COMMAND ----------

# MAGIC %md
# MAGIC **Managed Table**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE man_cata.man_schema.man_table
# MAGIC (
# MAGIC   id INT,
# MAGIC   name STRING
# MAGIC )
# MAGIC USING DELTA  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario - 2
# MAGIC **-External Catalog**
# MAGIC **-Managed Schema**
# MAGIC **-Managed Table**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG ext_cata
# MAGIC MANAGED LOCATION 'abfss://mycontainer@vivstandardisedzone.dfs.core.windows.net/external_catalog'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA ext_cata.man_schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE ext_cata.man_schema.man_table2
# MAGIC (
# MAGIC   id INT,
# MAGIC   name STRING
# MAGIC )
# MAGIC USING DELTA  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario - 3
# MAGIC **-External Catalog**
# MAGIC **-External Schema**
# MAGIC **-Managed Table**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA ext_cata.ext_schema
# MAGIC MANAGED LOCATION 'abfss://mycontainer@vivstandardisedzone.dfs.core.windows.net/ext_schema'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE ext_cata.ext_schema.man_table3
# MAGIC (
# MAGIC   id INT,
# MAGIC   name STRING
# MAGIC )
# MAGIC USING DELTA  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario - 4
# MAGIC **-External Table**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE man_cata.man_schema.ext_table
# MAGIC (
# MAGIC   id INT,
# MAGIC   name STRING
# MAGIC )
# MAGIC USING DELTA  
# MAGIC LOCATION 'abfss://mycontainer@vivstandardisedzone.dfs.core.windows.net/ext_table/man_table3'

# COMMAND ----------

# MAGIC %md
# MAGIC ### DROP Managed Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Unity Catalog ensures that your table will be deleted after 7 days. so it will not visble in workspace but in azure storage path it will . 
# MAGIC DROP TABLE man_cata.man_schema.man_table;

# COMMAND ----------

# MAGIC %md
# MAGIC ### UNDROP Managed Table

# COMMAND ----------

# MAGIC %sql
# MAGIC UNDROP TABLE man_cata.man_schema.man_table;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Querying Files using SELECT

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO man_cata.man_schema.ext_table
# MAGIC VALUES
# MAGIC (1,'Viv'),
# MAGIC (2,'Bhand'),
# MAGIC (3,'Raw Agent')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM man_cata.man_schema.ext_table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`abfss://mycontainer@vivstandardisedzone.dfs.core.windows.net/ext_table/man_table3` where id = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Permanent Views

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW man_cata.man_schema.view1
# MAGIC AS 
# MAGIC SELECT * FROM delta.`abfss://mycontainer@vivstandardisedzone.dfs.core.windows.net/ext_table/man_table3` WHERE id = 1 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM man_cata.man_schema.view1

# COMMAND ----------

# MAGIC %md
# MAGIC ### TEMP VIEWS

# COMMAND ----------

# MAGIC %md
# MAGIC **Temp View**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW temp_view
# MAGIC AS 
# MAGIC SELECT * FROM delta.`abfss://mycontainer@vivstandardisedzone.dfs.core.windows.net/ext_table/man_table3` WHERE id = 1 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM temp_view

# COMMAND ----------

# MAGIC %md
# MAGIC ### VOLUMES

# COMMAND ----------

# MAGIC %md
# MAGIC **Creating a directory for volume**

# COMMAND ----------

dbutils.fs.mkdirs('abfss://mycontainer@vivstandardisedzone.dfs.core.windows.net/volumes')

# COMMAND ----------

# MAGIC %md
# MAGIC **Creating a volume**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL VOLUME man_cata.man_schema.extvolume
# MAGIC LOCATION 'abfss://mycontainer@vivstandardisedzone.dfs.core.windows.net/volumes'

# COMMAND ----------

# MAGIC %md
# MAGIC **Copy file for volume**

# COMMAND ----------

dbutils.fs.cp('abfss://mycontainer@vivstandardisedzone.dfs.core.windows.net/source/Sales', 'abfss://mycontainer@vivstandardisedzone.dfs.core.windows.net/volumes/Sales')

# COMMAND ----------

# MAGIC %md
# MAGIC **Querying Volumes**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM csv.`/Volumes/man_cata/man_schema/extvolume/Sales`