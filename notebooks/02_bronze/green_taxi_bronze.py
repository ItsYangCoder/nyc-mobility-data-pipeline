# Databricks notebook source

# COMMAND ----------

# # 02 · Load Green Taxi Bronze
# Set widgets, then run all cells. COPY INTO skips a file already loaded into the same table.

# COMMAND ----------

from quality_rules import parse_month
import re

dbutils.widgets.text("run_month", "2026-03", "Run month (YYYY-MM)")
dbutils.widgets.text("source_dir", "/Volumes/nyc_mobility/nyc_bronze/ftw-b12-de-r2/groups/week-08/group-c/landing/green_taxi", "Green Taxi volume directory")
dbutils.widgets.text("catalog", "nyc_mobility", "Target catalog")
dbutils.widgets.text("bronze_schema", "nyc_bronze", "Bronze schema")

run_month = dbutils.widgets.get("run_month").strip()
source_dir = dbutils.widgets.get("source_dir").strip().rstrip("/")
catalog = dbutils.widgets.get("catalog").strip()
bronze_schema = dbutils.widgets.get("bronze_schema").strip()
parse_month(run_month)
if not re.fullmatch(r"/Volumes/[A-Za-z0-9_./-]+", source_dir):
    raise ValueError("Set source_dir to the Green Taxi directory under /Volumes")
for identifier in (catalog, bronze_schema):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        raise ValueError(f"Invalid catalog or schema: {identifier!r}")

bronze_table = f"{catalog}.{bronze_schema}.green_taxi_raw"
file_name = f"green_tripdata_{run_month}.parquet"
print(f"Loading {source_dir}/{file_name} into {bronze_table}")

# COMMAND ----------

# Create an empty, schemaless Delta table so COPY INTO can infer the Parquet schema.
spark.sql(f"CREATE TABLE IF NOT EXISTS {bronze_table}")
# The source timestamps use TIMESTAMP_NTZ. Enable this feature BEFORE loading.
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
    FILEFORMAT = PARQUET
    FILES = ('{file_name}')
    COPY_OPTIONS ('mergeSchema' = 'true')
""")
copy_result = result.first().asDict()
display(spark.createDataFrame([copy_result]))

# COMMAND ----------

display(spark.sql(f"SELECT COUNT(*) AS bronze_rows FROM {bronze_table}"))
import json
dbutils.notebook.exit(json.dumps(copy_result))
