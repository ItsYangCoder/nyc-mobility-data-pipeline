# Databricks notebook source

# COMMAND ----------

# # 02 · Load one Weather file to Bronze
# The filename and directory are widgets. Repeating COPY INTO on an already loaded file adds no rows; select the next file to continue.

# COMMAND ----------

import re

dbutils.widgets.text("weather_source_dir", "/Volumes/nyc_mobility/nyc_bronze/ftw-b12-de-r2/groups/week-08/group-c/landing/weather", "Weather directory")
dbutils.widgets.text("source_file", "weather_2026-03-01_2026-03-31.json", "Weather filename (including extension)")
dbutils.widgets.text("catalog", "nyc_mobility", "Catalog")
dbutils.widgets.text("bronze_schema", "nyc_bronze", "Bronze schema")
source_dir = dbutils.widgets.get("weather_source_dir").strip().rstrip("/")
source_file = dbutils.widgets.get("source_file").strip()
catalog = dbutils.widgets.get("catalog").strip()
bronze_schema = dbutils.widgets.get("bronze_schema").strip()
if not re.fullmatch(r"/Volumes/[A-Za-z0-9_./-]+", source_dir):
    raise ValueError("Set weather_source_dir to a /Volumes/... directory (without dbfs:)")
if not re.fullmatch(r"[A-Za-z0-9_.-]+\.(?:parquet|csv|json)", source_file):
    raise ValueError("Set source_file to a listed .parquet, .csv or .json file")
if source_file.endswith("_metadata.json"):
    raise ValueError("Choose the weather data JSON, not the metadata JSON")
for name in (catalog, bronze_schema):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Invalid catalog/schema: {name!r}")
if source_file not in {item.name for item in dbutils.fs.ls(source_dir)}:
    raise FileNotFoundError(f"{source_file} not found in {source_dir}")
format_name = source_file.rsplit(".", 1)[1].upper()
format_options = {
    "csv": "FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')",
    "json": "FORMAT_OPTIONS ('multiLine' = 'true', 'inferSchema' = 'true')",
    "parquet": "",
}[format_name.lower()]
bronze_table = f"{catalog}.{bronze_schema}.weather_raw"
print(f"Loading {source_file} ({format_name}) into {bronze_table}")

# COMMAND ----------

spark.sql(f"CREATE TABLE IF NOT EXISTS {bronze_table}")
# Timestamp-without-timezone fields require this Delta feature when present.
spark.sql(f"""
    ALTER TABLE {bronze_table}
    SET TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')
""")
result = spark.sql(f"""
    COPY INTO {bronze_table}
    FROM (
      SELECT *, _metadata.file_path AS source_file_path,
             current_timestamp() AS source_ingested_at
      FROM '{source_dir}'
    )
    FILEFORMAT = {format_name}
    FILES = ('{source_file}')
    {format_options}
    COPY_OPTIONS ('mergeSchema' = 'true')
""")
copy_result = result.first().asDict()
display(spark.createDataFrame([copy_result]))
display(spark.sql(f"SELECT COUNT(*) AS bronze_rows FROM {bronze_table}"))
import json
dbutils.notebook.exit(json.dumps(copy_result))
