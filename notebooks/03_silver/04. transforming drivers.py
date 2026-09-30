# Databricks notebook source
# MAGIC %run "/<your-workspace-folder>/formula1-project-incremental-load/00-common/01.envirounment config"

# COMMAND ----------

# MAGIC %run "/<your-workspace-folder>/formula1-project-incremental-load/00-common/Silver Helpers"

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
p_batch_id = dbutils.widgets.get("p_batch_id")


# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.drivers"
silver_table = f"{catalog_name}.{silver_schema}.drivers"

# COMMAND ----------

drivers_df = spark.table(bronze_table)

# COMMAND ----------

drivers_droped_df = (drivers_df.drop(F.col("url"))
                     .filter((F.col("batch_id") == p_batch_id)))

# COMMAND ----------

drivers_renamed_df = (
   drivers_droped_df
    .withColumnsRenamed(

        {"dateOfBirth":"date_of_birth",
         "driverId":"driver_id",
         "sorce_file":"source_file"
         }
    )


)

# COMMAND ----------

drivers_concatenated_df = (
   drivers_renamed_df
   .withColumn("driver_name",
               F.concat_ws(" ",F.col("name.givenName"), F.col("name.familyName")))
    .drop("name")

)

# COMMAND ----------

display(drivers_concatenated_df)

# COMMAND ----------

drivers_distinct_df = drivers_concatenated_df.dropDuplicates(["driver_id"])

# COMMAND ----------

drivers_final_df = drivers_distinct_df.withColumn("nationality", F.initcap(F.col("nationality"))).withColumn("driver_name", F.initcap(F.col("driver_name")))

# COMMAND ----------

write_to_silver(
    input_df=drivers_final_df,
    target_table=silver_table,
    merge_condition="t.driver_id = s.driver_id",
    columns_to_update=[
        "driver_name",
        "date_of_birth",
        "nationality",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)

# COMMAND ----------

display(spark.table(silver_table))