# Databricks notebook source
# MAGIC %md
# MAGIC ##Transform Racess Data
# MAGIC - read bronze table
# MAGIC - keep only columns required for analytics (drop url column)
# MAGIC - Rename column names using snake_case (circuitID  to circuit_id, circuitName to circuit_name)
# MAGIC - Rename column to make them more meaningful(lat to latitude , long to longtude)
# MAGIC - remove duplicate records
# MAGIC - transform values of columns circuit_name and locality to title case
# MAGIC - write the transformed data to silver circuits table 

# COMMAND ----------

# MAGIC %run "/<your-workspace-folder>/formula1-project-incremental-load/00-common/01.envirounment config"

# COMMAND ----------

# MAGIC %run "/<your-workspace-folder>/formula1-project-incremental-load/00-common/Silver Helpers"

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
p_batch_id = dbutils.widgets.get("p_batch_id")


# COMMAND ----------


bronze_table =  f"{catalog_name}.{bronze_schema}.races"
silver_table =  f"{catalog_name}.{silver_schema}.races"

# COMMAND ----------

races_df = (spark.table(bronze_table)
            .filter((F.col("batch_id") == p_batch_id)))

# COMMAND ----------

races_selected_df = races_df.select(
         F.col("season"),
         F.col("round"),
         F.col("raceName"),
         F.col("date"),
         F.col("circuitId"),
         F.col("ingestion_timestamp"),
         F.col("sorce_file"),
         F.col("batch_id")

)

# COMMAND ----------

# standardizing the names to the snake case 
races_renamed_df = (

    races_selected_df
         .withColumnRenamed("circuitId", "circuit_id")
         .withColumnRenamed("raceName", "race_name")
         .withColumnRenamed("date", "race_date")
         .withColumnRenamed("sorce_file", "source_file"),

)

# COMMAND ----------

# DBTITLE 1,Cell 9
#dedublicating the data 
races_distinct_df = (races_renamed_df[0] if isinstance(races_renamed_df, tuple) else races_renamed_df).dropDuplicates(["season", "round"])


# COMMAND ----------


print(spark.catalog.tableExists(silver_table))

# COMMAND ----------

races_capitalized_df = races_distinct_df.withColumn("race_name", F.initcap(F.col("race_name")))




# COMMAND ----------

write_to_silver(
    input_df=races_capitalized_df,
    target_table=silver_table,
    merge_condition="t.season = s.season AND t.round = s.round",
    columns_to_update=[
        "race_name",
        "race_date",
        "circuit_id",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)