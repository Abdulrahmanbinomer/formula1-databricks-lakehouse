# Databricks notebook source
# MAGIC %run "/<your-workspace-folder>/formula1-project/00-common/01.envirounment config"

# COMMAND ----------

bronze_table =  f"{catalog_name}.{bronze_schema}.results"
silver_table =  f"{catalog_name}.{silver_schema}.results"


# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

results_df = spark.table(bronze_table)

# COMMAND ----------

results_droped_df = results_df.drop("url")

# COMMAND ----------

# standardizing the names to the snake case 
results_renamed_df = (

    results_droped_df
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
)

# COMMAND ----------

results_valid_df = (
    results_renamed_df
    .filter(
          F.col("season").isNotNull() &
          F.col("round").isNotNull() &
          F.col("constructor_id").isNotNull() &
          F.col("driver_id").isNotNull() 

    )
)

# COMMAND ----------

display(results_renamed_df.count() - results_valid_df.count())

# COMMAND ----------

results_distinct_df = results_valid_df.dropDuplicates(["constructor_id","round","season","driver_id"])

# COMMAND ----------

results_final_df = results_distinct_df.withColumn("race_name", F.initcap(F.col("race_name")))

# COMMAND ----------

(

    results_final_df          
          .write.format('delta')
          .mode('overwrite')
          .saveAsTable(silver_table)

)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1.silver.results;