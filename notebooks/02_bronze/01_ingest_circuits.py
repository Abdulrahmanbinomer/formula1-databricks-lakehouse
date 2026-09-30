# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest Circuit.csv file
# MAGIC 1. Read the file using dataframebreader API
# MAGIC 2. Add Metadata Columums
# MAGIC        - source
# MAGIC        - ingestion timestamp
# MAGIC 3. Write to bronze delta table 

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
p_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# DBTITLE 1,Cell 2
# MAGIC %run "/<your-workspace-folder>/formula1-project-incremental-load/00-common/01.envirounment config"

# COMMAND ----------

# MAGIC %run "/<your-workspace-folder>/formula1-project-incremental-load/00-common/envirounment helpers"

# COMMAND ----------

# MAGIC %run "/<your-workspace-folder>/formula1-project-incremental-load/00-common/bronze helpers"

# COMMAND ----------



source_file = f"/Volumes/formula1-incr/landing/files/{p_batch_id}/circuits.csv"

table_name = f"{catalog_name}.{bronze_schema}.circuits"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

circuits_schema = StructType([

  StructField('circuitid', StringType()),
  StructField("url", StringType()),
  StructField("circuitName", StringType()),
  StructField("lat", DoubleType()),
  StructField("long", DoubleType()),
  StructField("locality", StringType()),
  StructField("country", StringType()),
])

# COMMAND ----------

# DBTITLE 1,Cell 2
circuits_df = (spark.read
    .format('csv')
    .option('header', 'true')
    #.option('inferSchema','true')
    .schema(circuits_schema)  
    .option('mode','FAILFAST')  
    .load(source_file))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 2 - Add Metadata Columns
# MAGIC    - Source file
# MAGIC    - Ingestion Timestamp

# COMMAND ----------


circuits_final_df = add_ingestion_metadata(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3 Write to the bronze delta table
# MAGIC  

# COMMAND ----------

#circuits_final_df1 = circuits_final_df.withColumn("batch_id",F.lit(p_batch_id))

# COMMAND ----------



#(

    #circuits_final_df1
          #.write.format('delta')
          #.mode('overwrite')
         # .partitionBy('batch_id')
         # .option('replaceWhere', f"batch_id = '{p_batch_id}'")
         # .saveAsTable(table_name)


#)

# COMMAND ----------

write_to_bronze(
   input_df = circuits_final_df,
   table_name = table_name,
   batch_id = p_batch_id
)

# COMMAND ----------



