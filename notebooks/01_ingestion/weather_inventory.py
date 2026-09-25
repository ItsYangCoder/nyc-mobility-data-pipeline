# Databricks notebook source

# COMMAND ----------

# # 01 · Inspect Weather files
# Read-only notebook. Inspect actual filenames and fields before choosing the file to load; no assumed weather schema is written to the repo.

# COMMAND ----------

import re

dbutils.widgets.text("weather_source_dir", "/Volumes/nyc_mobility/nyc_bronze/ftw-b12-de-r2/groups/week-08/group-c/landing/weather", "Weather directory")
source_dir = dbutils.widgets.get("weather_source_dir").strip().rstrip("/")
if not re.fullmatch(r"/Volumes/[A-Za-z0-9_./-]+", source_dir):
    raise ValueError("Use a /Volumes/... directory in weather_source_dir (without dbfs:)")
files = [item for item in dbutils.fs.ls(source_dir) if not item.isDir()]
for item in files:
    print(item.name, item.size, "bytes")

# COMMAND ----------

dbutils.widgets.text("source_file", "weather_2026-03-01_2026-03-31.json", "Weather file to inspect")
source_file = dbutils.widgets.get("source_file").strip()
if source_file:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+\.(?:parquet|csv|json)", source_file):
        raise ValueError("Choose one .parquet, .csv or .json filename listed above")
    if source_file not in {item.name for item in files}:
        raise FileNotFoundError(f"{source_file} not found in {source_dir}")
    path = f"{source_dir}/{source_file}"
    if source_file.endswith(".csv"):
        sample = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    elif source_file.endswith(".json"):
        sample = spark.read.option("multiLine", "true").json(path)
    else:
        sample = spark.read.parquet(path)
    print("Rows:", sample.count())
    sample.printSchema()
    display(sample.limit(5))
else:
    print("Set source_file to one of the listed filenames, then rerun this cell.")
