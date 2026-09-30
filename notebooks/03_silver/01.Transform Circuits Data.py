# Databricks notebook source
# MAGIC %md
# MAGIC ##Transform Circuits Data
# MAGIC - read bronze table
# MAGIC - keep only columns required for analytics (drop url column)
# MAGIC - Rename column names using snake_case (circuitID  to circuit_id, circuitName to circuit_name)
# MAGIC - Rename column to make them more meaningful(lat to latitude , long to longtude)
# MAGIC - remove duplicate records
# MAGIC - transform values of columns circuit_name and locality to title case
# MAGIC - write the transformed data to silver circuits table 

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
p_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run "/<your-workspace-folder>/formula1-project-incremental-load/00-common/01.envirounment config"

# COMMAND ----------

bronze_table =  f"{catalog_name}.{bronze_schema}.circuits"
silver_table =  f"{catalog_name}.{silver_schema}.circuits"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

#circuits_df = spark.read.table(bronze_table)

# COMMAND ----------

circuits_df = (
    spark.table(bronze_table).filter(F.col("batch_id") == p_batch_id)
    
    
    )
  



# COMMAND ----------

#keep only the column required for analytics(drop url column)
circuits_selected_df = circuits_df.select(
         "circuitid",
         "circuitName",
         "lat",
         "long",
         "locality",
         "country",
         "ingestion_timestamp",
         "sorce_file"

)

# COMMAND ----------

circuits_selected_df = circuits_df.select(
         F.col("circuitid"),
         F.col("circuitName"),
         F.col("lat"),
         F.col("long"),
         F.col("locality"),
         F.col("country"),
         F.col("ingestion_timestamp"),
         F.col("sorce_file"),
         F.col("batch_id")

)

# COMMAND ----------

# standardizing the names to the snake case 
circuits_renamed_df = (

    circuits_selected_df
         .withColumnRenamed("circuitid", "circuit_id")
         .withColumnRenamed("circuitName", "circuit_name")
         .withColumnRenamed("lat", "latitude")
         .withColumnRenamed("long", "longitude")
         .withColumnRenamed("sorce_file", "source_file"),

)

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# standardizing the names to the snake case 
circuits_renamed_df = (

    circuits_selected_df
    .withColumnsRenamed({
        
        "circuitid":"circuit_id",
        "circuitName":"circuit_name",
        "lat":"latitude",
        "long":"longitude",
        "sorce_file":"source_file"       
        })
)

# COMMAND ----------

circuits_valid_df = circuits_renamed_df.filter(
              "circuit_id IS NOT NULL"

)

# COMMAND ----------

# above is the sql method and down here is the pyspark method
circuits_valid_df = circuits_renamed_df.filter(
             F.col("circuit_id").isNotNull()

)

# COMMAND ----------

#removing duplicates usinfg dataframeAPI to data from distinct method
#circuits_distinct_df = circuits_valid_df.distinct()

# COMMAND ----------

#above method is only usde when there i need to remove duplicates from each columns but there could be less chance that we need to remove duplicates from each columns only primary key need to be reomved in terms of duplication following the way where we usually use to remove duplicates 

circuits_distinct_df = circuits_valid_df.dropDuplicates(["circuit_id"])

# COMMAND ----------

#Now the functuions is using capitalise and standardize the title case 

circuits_final_df =( circuits_distinct_df
         .withColumn('circuit_name',F.initcap(F.col("circuit_name")))
         .withColumn('locality',F.initcap(F.col("locality")))


)

# COMMAND ----------

circuits_final_df = (
      circuits_final_df  
        .withColumn("created_timestamp",F.current_timestamp())
        .withColumn("updated_timestamp",F.current_timestamp())
)

# COMMAND ----------

from delta.tables import DeltaTable

if not spark.catalog.tableExists(silver_table):

    (
        circuits_final_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
    )

else:

    delta_table = DeltaTable.forName(spark, silver_table)

    (
        delta_table.alias("t")
        .merge(
            circuits_final_df.alias("s"),
            "t.circuit_id = s.circuit_id"
        )
        .whenMatchedUpdate(
            condition="s.batch_id >= t.batch_id",
            set={
                "circuit_name": "s.circuit_name",
                "latitude": "s.latitude",
                "longitude": "s.longitude",
                "locality": "s.locality",
                "country": "s.country",
                "ingestion_timestamp": "s.ingestion_timestamp",
                "source_file": "s.source_file",
                "batch_id": "s.batch_id",
                "updated_timestamp": "s.updated_timestamp"
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )