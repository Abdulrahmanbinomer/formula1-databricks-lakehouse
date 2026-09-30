# Databricks notebook source
# MAGIC %run "/<your-workspace-folder>/formula1-project-incremental-load/00-common/01.envirounment config"

# COMMAND ----------

# MAGIC %run "/<your-workspace-folder>/formula1-project-incremental-load/00-common/Silver Helpers"

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
p_batch_id = dbutils.widgets.get("p_batch_id")


# COMMAND ----------

bronze_table =  f"{catalog_name}.{bronze_schema}.constructors"
silver_table =  f"{catalog_name}.{silver_schema}.constructors"

# COMMAND ----------

constructors_df = spark.table(bronze_table)

# COMMAND ----------

constructors_droped_df = constructors_df.drop("url")

# COMMAND ----------

constructors_renamed_df = (

    constructors_droped_df
         .withColumnRenamed("constructorId", "constructor_id")
         .withColumnRenamed("name", "constructor_name")
         .withColumnRenamed("sorce", "source_file")

)
display(constructors_renamed_df)

# COMMAND ----------

constructors_distinct_df = constructors_droped_df.dropDuplicates(["constructors_id"])

# COMMAND ----------

constructors_final_df = constructors_distinct_df.withColumn("nationality", F.initcap(F.col("nationality")))


# COMMAND ----------

# DBTITLE 1,Cell 10
constructors_final_df = (
    spark.table(bronze_table)
        .filter((F.col("batch_id") == p_batch_id))
        .drop("url")
         .withColumnRenamed("constructorId", "constructor_id")
         .withColumnRenamed("name", "constructor_name")
         .withColumnRenamed("sorce_file", "source_file")
         .dropDuplicates(["constructor_id"])
         .withColumn("nationality", F.initcap(F.col("nationality")))
)



# COMMAND ----------

write_to_silver(
    input_df=constructors_final_df,
    target_table=silver_table,
    merge_condition="t.constructor_id = s.constructor_id",
    columns_to_update=[
        "constructor_name",
        "nationality",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)

# COMMAND ----------

