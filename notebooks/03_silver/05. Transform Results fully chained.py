# Databricks notebook source
# MAGIC %run "/<your-workspace-folder>/formula1-project-incremental-load/00-common/01.envirounment config"

# COMMAND ----------

# MAGIC %run "/<your-workspace-folder>/formula1-project-incremental-load/00-common/Silver Helpers"

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
p_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

bronze_table =  f"{catalog_name}.{bronze_schema}.results"
silver_table =  f"{catalog_name}.{silver_schema}.results"


# COMMAND ----------

results_df = (
    
    spark.table(bronze_table)
    .filter((F.col("batch_id") == p_batch_id))
    .select("season",
            "round",
            "constructorId",
            "driverId",
             "date",
             "raceName",
             "grid",
             "laps",
             "number",
             "points",
             "position",
             "positionText",
             "status",
             "ingestion_timestamp",
             "sorce_file",
             "batch_id"
                        )
    .withColumnsRenamed({
        
        "constructorId":"constructor_id",
        "driverId":"driver_id",
        "raceName":"race_name",
        "date":"race_date",
        "sorce_file":"source_file",
        "grid":"grid_position",
        "laps":"completed_laps",
        "number":"car_number",
        "position":"final_position",
        "positionText":"final_position_text"     
        })
        .filter(
          F.col("season").isNotNull() &
          F.col("round").isNotNull() &
          F.col("constructor_id").isNotNull() &
          F.col("driver_id").isNotNull() 

    )
       .dropDuplicates(["constructor_id","round","season","driver_id"])
       .withColumn("race_name", F.initcap(F.col("race_name")))
    )

# COMMAND ----------

write_to_silver(
    input_df=results_df,
    target_table=silver_table,
    merge_condition="""
        t.season = s.season
        AND t.round = s.round
        AND t.constructor_id = s.constructor_id
        AND t.driver_id = s.driver_id
    """,
    columns_to_update=[
        "race_name",
        "race_date",
        "grid_position",
        "completed_laps",
        "car_number",
        "points",
        "final_position",
        "final_position_text",
        "status",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)