# Databricks notebook source
# MAGIC %md
# MAGIC # Build Drivers Dimension
# MAGIC
# MAGIC 1. Read silver `drivers` table
# MAGIC 1. Read gold `ref_nationality_region` table
# MAGIC 1. Join the data from `drivers` with `ref_nationality_region` using `nationality`
# MAGIC 1. Select the required columns
# MAGIC     - drivers.driver_id
# MAGIC     - drivers.driver_name
# MAGIC     - drivers.date_of_birth
# MAGIC     - drivers.nationality
# MAGIC     - ref_nationality_region.region
# MAGIC 1. Write the transformed data to gold `dim_drivers` table
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
p_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run "/<your-workspace-folder>/formula1-project-incremental-load/00-common/Gold Helpers"
# MAGIC

# COMMAND ----------

# MAGIC %run "/<your-workspace-folder>/formula1-project-incremental-load/00-common/01.envirounment config"

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_drivers"

# COMMAND ----------

drivers_df = (spark.table(f"{catalog_name}.{silver_schema}.drivers").filter(F.col("batch_id") == p_batch_id))
ref_nationality_region_df = spark.table(f"formula1.{gold_schema}.ref_nationality_region")

# COMMAND ----------

dim_drivers_df = (
    drivers_df
        .join(
            ref_nationality_region_df,
            drivers_df.nationality == ref_nationality_region_df.nationality,
            "left"
        )
        .select(
            drivers_df.driver_id,
            drivers_df.driver_name,
            drivers_df.date_of_birth,
            drivers_df.nationality,
            ref_nationality_region_df.region.alias("nationality_region")
        )
)

# COMMAND ----------

write_to_gold(
    input_df=dim_drivers_df,
    target_table=target_table,
    merge_condition="t.driver_id = s.driver_id",
    columns_to_update=[
        "driver_name",
        "date_of_birth",
        "nationality",
        "nationality_region"
    ]
)