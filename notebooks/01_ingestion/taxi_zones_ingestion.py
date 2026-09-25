# Databricks notebook source

# COMMAND ----------

# # 01 · Inspect Taxi Zones source
# Read-only source profiling. This notebook does not create or change a table; the Bronze notebook performs the actual load.

# COMMAND ----------

import re
from pyspark.sql import functions as F

dbutils.widgets.text("zones_source_dir", "/Volumes/nyc_mobility/nyc_bronze/ftw-b12-de-r2/groups/week-08/group-c/landing/taxi_zones", "Taxi Zones directory")
dbutils.widgets.text("source_file", "taxi_zone_lookup.csv", "Taxi Zones CSV filename")
source_dir = dbutils.widgets.get("zones_source_dir").strip().rstrip("/")
source_file = dbutils.widgets.get("source_file").strip()
if not re.fullmatch(r"/Volumes/[A-Za-z0-9_./-]+", source_dir):
    raise ValueError("Use a /Volumes/... directory in zones_source_dir (without dbfs:)")
if not re.fullmatch(r"[A-Za-z0-9_.-]+\.csv", source_file):
    raise ValueError("Set source_file to a CSV filename in the Taxi Zones directory")
if source_file not in {item.name for item in dbutils.fs.ls(source_dir)}:
    raise FileNotFoundError(f"{source_file} not found in {source_dir}")
path = f"{source_dir}/{source_file}"
print("Inspecting:", path)

# COMMAND ----------

zones_df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
print("Source rows:", zones_df.count())
zones_df.printSchema()
display(zones_df.limit(10))

# COMMAND ----------

required = {"LocationID", "Borough", "Zone", "service_zone"}
if not required.issubset(zones_df.columns):
    raise ValueError(f"Missing Taxi Zones columns: {sorted(required - set(zones_df.columns))}")
display(zones_df.select(
    F.count("*").alias("total_rows"),
    F.countDistinct("LocationID").alias("unique_location_ids"),
    F.sum(F.col("LocationID").isNull().cast("long")).alias("missing_location_id"),
    F.sum((F.col("Zone").isNull() | (F.trim("Zone") == "")).cast("long")).alias("missing_zone"),
))
display(zones_df.groupBy("LocationID").count().filter(F.col("count") > 1))
