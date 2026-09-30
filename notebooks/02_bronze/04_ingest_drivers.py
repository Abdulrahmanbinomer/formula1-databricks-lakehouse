# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest Drivers.JSON File
# MAGIC 1. Read the file using spark dataframe reader API and also use DDL
# MAGIC 2. add Metadata Columns
# MAGIC       - Source File 
# MAGIC       - Ingestion Timestamp 
# MAGIC 3. write to bronze deltya table
# MAGIC 4. define and enforce schema (preserve the nested structure)

# COMMAND ----------

# MAGIC %run "/<your-workspace-folder>/formula1-project-incremental-load/00-common/01.envirounment config"

# COMMAND ----------

# MAGIC %run "/<your-workspace-folder>/formula1-project-incremental-load/00-common/envirounment helpers"

# COMMAND ----------

# MAGIC %run "/<your-workspace-folder>/formula1-project-incremental-load/00-common/bronze helpers"

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
p_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

source_file = f"/Volumes/formula1-incr/landing/files/{p_batch_id}/drivers.json"
table_name = f"formula1-incr.{bronze_schema}.drivers"
table_namer = f"`formula1-incr`.{bronze_schema}.drivers"

# COMMAND ----------

#define the schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
name_schema = StructType([

       StructField('givenName',StringType()),
       StructField('familyName',StringType())
       
])
drivers_schema = StructType([

       StructField('driverId',StringType()),
       StructField('name',name_schema),
       StructField('dateOfBirth',DateType()),
       StructField('nationality',StringType()),
       StructField('url',StringType())
])

# COMMAND ----------

drivers_df = (

    spark.read 
    .format('json')
    .schema(drivers_schema)
    .option('mode','FAILFAST')
    .load(source_file)

)

# COMMAND ----------

drivers_final_df = add_ingestion_metadata(drivers_df) 

# COMMAND ----------

write_to_bronze(
   input_df = drivers_final_df,
   table_name = table_namer,
   batch_id = p_batch_id
)