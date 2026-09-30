# Databricks notebook source


# COMMAND ----------

def write_to_bronze(
    input_df,
    table_name,
    batch_id
):

    final_df = input_df.withColumn(
        "batch_id",
        F.lit(batch_id)
    )

    print("batch_id:", batch_id)
    print("replaceWhere:", f"batch_id = '{batch_id}'")

    (
        final_df
        .write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"batch_id = '{batch_id}'")
        .saveAsTable(table_name)
    )