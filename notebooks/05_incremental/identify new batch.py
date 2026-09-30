# Databricks notebook source
# MAGIC %run "/<your-workspace-folder>/formula1-project-incremental-load/00-common/01.envirounment config"

# COMMAND ----------

control_table = f"{catalog_name}.{control_schema}.batch_control"

# COMMAND ----------

print(control_table)

# COMMAND ----------

from pyspark.sql import functions as F

# Get batch folders from landing
landing_batches = sorted([
    file.name.rstrip("/")
    for file in dbutils.fs.ls("/Volumes/formula1-incr/landing/files/")
    if file.isDir()
])

# Check whether control table exists using SQL
control_table_exists = (
    spark.sql("""
        SHOW TABLES IN `formula1-incr`.control
        LIKE 'batch_control'
    """).count() > 0
)

# Read tracked batches
if control_table_exists:
    tracked_batches = [
        row.batch_id
        for row in (
            spark.sql("""
                SELECT DISTINCT batch_id
                FROM `formula1-incr`.control.batch_control
                WHERE status IN ('in_progress', 'completed')
            """)
            .collect()
        )
    ]
else:
    tracked_batches = []

# Identify earliest unprocessed batch
new_batches = sorted(
    list(set(landing_batches) - set(tracked_batches))
)

next_batch = new_batches[0] if new_batches else None

print(f"Landing batches      : {landing_batches}")
print(f"Tracked batches      : {tracked_batches}")
print(f"Next batch to process: {next_batch}")

if next_batch is None:
    dbutils.jobs.taskValues.set(
        key="p_batch_id",
        value=""
    )

    dbutils.jobs.taskValues.set(
        key="has_batch",
        value="false"
    )

else:
    dbutils.jobs.taskValues.set(
        key="p_batch_id",
        value=next_batch
    )

    dbutils.jobs.taskValues.set(
        key="has_batch",
        value="true"
    )

# COMMAND ----------

