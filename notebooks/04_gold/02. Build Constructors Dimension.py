# Databricks notebook source
# MAGIC %md
# MAGIC # Build Constructors Dimension
# MAGIC
# MAGIC 1. Read silver `constructors` table
# MAGIC 1. Read gold `ref_nationality_region` table
# MAGIC 1. Join the data from `constructors` with `ref_nationality_region` using `nationality`
# MAGIC 1. Select the required columns
# MAGIC     - constructors.constructor_id
# MAGIC     - constructors.constructor_name
# MAGIC     - constructors.nationality
# MAGIC     - ref_nationality_region.region
# MAGIC 1. Write the transformed data to gold `dim_constructors` table
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
p_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run "/<your-workspace-folder>/formula1-project-incremental-load/00-common/Gold Helpers"

# COMMAND ----------

# MAGIC %run "/<your-workspace-folder>/formula1-project-incremental-load/00-common/01.envirounment config"

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_constructors"

# COMMAND ----------

constructors_df = (spark.table(f"{catalog_name}.{silver_schema}.constructors").filter(F.col("batch_id") == p_batch_id))
ref_nationality_region_df = spark.table(f"formula1.{gold_schema}.ref_nationality_region")

# COMMAND ----------

dim_constructors_df = (
    constructors_df
        .join(
            ref_nationality_region_df,
            constructors_df.nationality == ref_nationality_region_df.nationality,
            "left"
            )
        .select (
            constructors_df.constructor_id,
            constructors_df.constructor_name,
            constructors_df.nationality,
            ref_nationality_region_df.region.alias("nationality_region")
        )
)

# COMMAND ----------

write_to_gold(
    input_df=dim_constructors_df,
    target_table=target_table,
    merge_condition="t.constructor_id = s.constructor_id",
    columns_to_update=[
        "constructor_name",
        "nationality",
        "nationality_region"
    ]
)