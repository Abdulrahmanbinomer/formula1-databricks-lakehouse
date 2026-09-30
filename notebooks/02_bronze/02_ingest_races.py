# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC ###Ingest Races.csv file
# MAGIC 1. Read the file using dataframebreader API
# MAGIC 2. Add Metadata Columums
# MAGIC        - source
# MAGIC        - ingestion timestamp
# MAGIC 3. Write to bronze delta table 

# COMMAND ----------

# DBTITLE 1,Load environment config
dbutils.widgets.text("p_batch_id","")
p_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run "/<your-workspace-folder>/formula1-project-incremental-load/00-common/01.envirounment config"

# COMMAND ----------

# MAGIC %run "/<your-workspace-folder>/formula1-project-incremental-load/00-common/bronze helpers"

# COMMAND ----------

# DBTITLE 1,Define races schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,  DoubleType

races_schema = StructType([

  StructField('season', IntegerType()),
  StructField("round", IntegerType()),
  StructField("url", StringType()),
  StructField("raceName", StringType()),
  StructField("date", StringType()),
  StructField("circuitId", StringType()),
  
])

# COMMAND ----------

# DBTITLE 1,Set source and target table
source_file = f"/Volumes/formula1-incr/landing/files/{p_batch_id}/races.csv"
table_name = f"formula1-incr.{bronze_schema}.races"
table_namer = f"`formula1-incr`.{bronze_schema}.races"


# COMMAND ----------

# DBTITLE 1,Read races CSV initial attempt
races_df = (spark.read
    .format('csv')
    .option('header', 'true')
    #.option('inferSchema','true')
    .schema(races_schema)  
    .option('mode','FAILFAST')  
    .load(source_file))
#display(races_df)

# COMMAND ----------

# DBTITLE 1,Metadata step description
# MAGIC %md
# MAGIC ###Step 2 - Add Metadata Columns
# MAGIC    - Source file
# MAGIC    - Ingestion Timestamp

# COMMAND ----------

# DBTITLE 1,Add metadata columns
from pyspark.sql import functions as F
races_final_df = (
    races_df
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("sorce_file", F.col("_metadata.file_path"))
        .withColumn("batch_id", F.lit(p_batch_id))
)



# COMMAND ----------

# DBTITLE 1,Write step description
# MAGIC %md
# MAGIC ###Step 3 Write to the bronze delta table
# MAGIC  

# COMMAND ----------

# DBTITLE 1,Write bronze table
write_to_bronze(
   input_df = races_final_df,
   table_name = table_namer,
   batch_id = p_batch_id
)