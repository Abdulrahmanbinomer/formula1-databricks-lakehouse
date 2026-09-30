-- Databricks notebook source
CREATE EXTERNAL LOCATION IF NOT EXISTS 
Formula1 
URL 'abfss://formula1-incr@newdbcourse.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `access_db_arbo`);

DESCRIBE EXTERNAL LOCATION 
`formula1-incr` ;

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://formula1-incr@newdbcourse.dfs.core.windows.net/landing'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # setting up the unity catologe project enironment
-- MAGIC 1. Create Catalog formula1 project
-- MAGIC 2. Create Schemas Landing, bronze,silver and gold layer 
-- MAGIC 3. Create Volume files in the landing schema 
-- MAGIC
-- MAGIC

-- COMMAND ----------

SHOW CATALOGS;

-- COMMAND ----------

-- DBTITLE 1,Create catalog
CREATE CATALOG IF NOT EXISTS `formula1-incr`
MANAGED LOCATION 'abfss://formula1-incr@newdbcourse.dfs.core.windows.net/';

DESCRIBE CATALOG EXTENDED formula1-incr;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating Schemas landing,bronze,silver,gold

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS `formula1-incr`.landing;
CREATE SCHEMA IF NOT EXISTS `formula1-incr`.bronze
  MANAGED LOCATION 'abfss://formula1-incr@newdbcourse.dfs.core.windows.net/bronze';
  CREATE SCHEMA IF NOT EXISTS `formula1-incr`.silver
  MANAGED LOCATION 'abfss://formula1-incr@newdbcourse.dfs.core.windows.net/silver';
  CREATE SCHEMA IF NOT EXISTS `formula1-incr`.gold
  MANAGED LOCATION 'abfss://formula1-incr@newdbcourse.dfs.core.windows.net/gold';

-- COMMAND ----------

show schemas;


-- COMMAND ----------

SELECT current_catalog();


-- COMMAND ----------

use catalog `formula1-incr`;
show schemas;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating the volume
-- MAGIC

-- COMMAND ----------

CREATE EXTERNAL VOLUME `formula1-incr`.landing.files
LOCATION  'abfss://formula1-incr@newdbcourse.dfs.core.windows.net/landing';

-- COMMAND ----------

-- MAGIC %fs ls /Volumes/formula1-incr/landing/files