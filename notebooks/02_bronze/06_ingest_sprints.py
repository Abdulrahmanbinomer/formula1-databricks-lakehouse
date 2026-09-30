# Databricks notebook source
# MAGIC %run "/<your-workspace-folder>/formula1-project-incremental-load/00-common/01.envirounment config"

# COMMAND ----------

# MAGIC %run "/<your-workspace-folder>/formula1-project-incremental-load/00-common/envirounment helpers"

# COMMAND ----------

# MAGIC %run "/<your-workspace-folder>/formula1-project-incremental-load/00-common/bronze helpers"

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
p_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

source_file = f"/Volumes/formula1-incr/landing/files/{p_batch_id}/sprints/"

table_name = f"formula1-incr.{bronze_schema}.sprints"
table_namer = f"`formula1-incr`.{bronze_schema}.sprints"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, FloatType

sprints_schema = StructType([

       StructField('date',DateType()),
       StructField('raceName',StringType()),
       StructField('round',IntegerType()),
       StructField('season',IntegerType()),
       StructField('url',StringType()),
       StructField('constructorId',StringType()),
       StructField('driverId',StringType()),
       StructField('grid',IntegerType()),
       StructField('laps',IntegerType()),
       StructField('number',IntegerType()),
       StructField('points',FloatType()),
       StructField('position',IntegerType()),
       StructField('positionText',StringType()),
       StructField('status',StringType())
       
])

# COMMAND ----------

sprints_df = (

    spark.read 
    .format('json')
    .schema(sprints_schema)
    .option('mode','FAILFAST')
    .option('multiLine','true') 
    .load(source_file)

)

# COMMAND ----------

sprints_final_df = add_ingestion_metadata(sprints_df) 

# COMMAND ----------

write_to_bronze(
   input_df = sprints_final_df,
   table_name = table_namer,
   batch_id = p_batch_id
)