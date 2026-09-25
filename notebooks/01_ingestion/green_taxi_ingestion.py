# Databricks notebook source

# COMMAND ----------

# # 01 · Inspect a monthly Green Taxi source file
# Read and inspect the source; this notebook does not write tables.

# COMMAND ----------

from quality_rules import parse_month
import re
from pyspark.sql import functions as F

dbutils.widgets.text("run_month", "2026-03", "Run month (YYYY-MM)")
dbutils.widgets.text("source_dir", "/Volumes/nyc_mobility/nyc_bronze/ftw-b12-de-r2/groups/week-08/group-c/landing/green_taxi", "Green Taxi volume directory")

run_month = dbutils.widgets.get("run_month").strip()
source_dir = dbutils.widgets.get("source_dir").strip().rstrip("/")
parse_month(run_month)
if not re.fullmatch(r"/Volumes/[A-Za-z0-9_./-]+", source_dir):
    raise ValueError("Set source_dir to the Green Taxi directory under /Volumes")

file_path = f"{source_dir}/green_tripdata_{run_month}.parquet"
print(f"Reading: {file_path}")

# COMMAND ----------

taxi_df = spark.read.parquet(file_path)
print("Source rows:", taxi_df.count())
taxi_df.printSchema()
display(taxi_df.limit(5))

# COMMAND ----------

display(taxi_df.select(
    F.min("lpep_pickup_datetime").alias("earliest_pickup"),
    F.max("lpep_pickup_datetime").alias("latest_pickup"),
    F.sum(F.col("lpep_pickup_datetime").isNull().cast("long")).alias("missing_pickup"),
    F.sum((F.date_format("lpep_pickup_datetime", "yyyy-MM") != run_month).cast("long"))
        .alias("outside_run_month"),
))
display(taxi_df.groupBy(
    F.date_format("lpep_pickup_datetime", "yyyy-MM").alias("pickup_month")
).count().orderBy("pickup_month"))
