# Databricks notebook source
# MAGIC %run "/<your-workspace-folder>/formula1-project-incremental-load/00-common/01.envirounment config"

# COMMAND ----------

control_table = f"{catalog_name}.{control_schema}.batch_control"

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
p_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

if p_batch_id:
    delta_table = DeltaTable.forName(spark, control_table)

    source_df = (
        spark.createDataFrame([(p_batch_id,)], ["batch_id"])
            .withColumn("status", F.lit("completed"))
            .withColumn("updated_timestamp", F.current_timestamp())
    )

    (
        delta_table.alias("t")
            .merge(
                source_df.alias("s"),
                "t.batch_id = s.batch_id AND t.status = 'in_progress'"
            )
            .whenMatchedUpdate(set={
                "status": "s.status",
                "updated_timestamp": "s.updated_timestamp"
            })
            .execute()
    )

    print(f"Marked batch {p_batch_id} as completed")
else:
    raise Exception("batch_id is missing")  

# COMMAND ----------

