# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest Constructor.JSON File
# MAGIC 1. Read the file using spark dataframe reader API and also use DDL
# MAGIC 2. add Metadata Columns
# MAGIC       - Source File 
# MAGIC       - Ingestion Timestamp 
# MAGIC 3. write to bronze deltya table

# COMMAND ----------

# MAGIC %run "/<your-workspace-folder>/formula1-project-incremental-load/00-common/01.envirounment config"
# MAGIC

# COMMAND ----------

# MAGIC %run "/<your-workspace-folder>/formula1-project-incremental-load/00-common/envirounment helpers"

# COMMAND ----------

# MAGIC %run "/<your-workspace-folder>/formula1-project-incremental-load/00-common/bronze helpers"

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
p_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# DBTITLE 1,Cell 4
source_file = f"/Volumes/formula1-incr/landing/files/{p_batch_id}/constructors.json"
table_name = f"formula1-incr.{bronze_schema}.contstructors"
table_namer = f"`formula1-incr`.{bronze_schema}.constructors"


# COMMAND ----------

# DBTITLE 1,Cell 5
#define the schema using DDL
contructors_schema = """
                       constructorId STRING,
                       name STRING,
                       nationality STRING,
                       url STRING
 
 """


# COMMAND ----------

#reading the data from the constructors file 
constructors_df = (

    spark.read 
    .format('json')
    .schema(contructors_schema)
    .option('mode','FAILFAST')
    .load(source_file)

)

# COMMAND ----------

constructors_final_df = add_ingestion_metadata(constructors_df) 

# COMMAND ----------

# DBTITLE 1,Cell 9
write_to_bronze(
   input_df = constructors_final_df,
   table_name = table_namer,
   batch_id = p_batch_id
)

# COMMAND ----------

# MAGIC %md
# MAGIC