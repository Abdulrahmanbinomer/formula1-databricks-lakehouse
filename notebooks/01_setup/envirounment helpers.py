# Databricks notebook source
#helper function to add file metadata for ingestion (source file ingestion timestamp)

from pyspark.sql import functions as F

def add_ingestion_metadata(df):
    return(
         df.withColumn('ingestion_timestamp', F.current_timestamp())
         .withColumn('sorce_file', F.col('_metadata.file_path'))
         )

# COMMAND ----------

