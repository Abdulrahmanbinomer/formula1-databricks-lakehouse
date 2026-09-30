# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Build Races Dimension 
# MAGIC 1. read silver races table 
# MAGIC 2. read silver circuits table 
# MAGIC 3. join the datafrom races with circuits using circuits_id 
# MAGIC 4. select the required columns 
# MAGIC       - Races.seasons 
# MAGIC       - races.round 
# MAGIC       - races.race_name 
# MAGIC       - races.race_date
# MAGIC       - circuits.circuit_name
# MAGIC       - circuits.locality
# MAGIC       - circuits.country
# MAGIC 5. write the transformed data to gold dim_races table 

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
p_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run "/<your-workspace-folder>/formula1-project-incremental-load/00-common/Gold Helpers"

# COMMAND ----------

# MAGIC %run "/<your-workspace-folder>/formula1-project-incremental-load/00-common/01.envirounment config"

# COMMAND ----------

circuits_table =  f"{catalog_name}.{silver_schema}.circuits"
races_table =  f"{catalog_name}.{silver_schema}.races"
target_table = f"{catalog_name}.{gold_schema}.dim_races"


# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_races"

# COMMAND ----------

circuits_df = (
    spark.table(f"{catalog_name}.{silver_schema}.circuits")
         .filter(F.col("batch_id") == p_batch_id)
)
races_df = (
    spark.table(f"{catalog_name}.{silver_schema}.races")
         .filter(F.col("batch_id") == p_batch_id)
)

# COMMAND ----------

# DBTITLE 1,Cell 6
#joining the tables and selection required columns

Dim_races_df = (
    
    races_df
          .join(
                  circuits_df,
                  races_df.circuit_id == circuits_df.circuit_id,
                  "inner"
                  )
           .select(
               
               races_df.season,
               races_df.round,
               races_df.race_name,
               races_df.race_date,
               circuits_df.circuit_name,
               circuits_df.locality,
               circuits_df.country

           )


)





# COMMAND ----------

write_to_gold(
    input_df=Dim_races_df,
    target_table=target_table,
    merge_condition="t.season = s.season AND t.round = s.round",
    columns_to_update=[
        "race_name",
        "race_date",
        "circuit_name",
        "locality",
        "country"
    ]
)