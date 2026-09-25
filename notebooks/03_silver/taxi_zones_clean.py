# Databricks notebook source

# COMMAND ----------

# # 03 · Clean Taxi Zones
# Validate the primary key first, then replace the Silver view. Whitespace is trimmed without changing the original Bronze rows.

# COMMAND ----------

import re
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "nyc_mobility", "Catalog")
dbutils.widgets.text("bronze_schema", "nyc_bronze", "Bronze schema")
dbutils.widgets.text("silver_schema", "nyc_silver", "Silver schema")
catalog = dbutils.widgets.get("catalog").strip()
bronze_schema = dbutils.widgets.get("bronze_schema").strip()
silver_schema = dbutils.widgets.get("silver_schema").strip()
for name in (catalog, bronze_schema, silver_schema):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Invalid catalog/schema: {name!r}")
bronze_table = f"{catalog}.{bronze_schema}.taxi_zones_raw"
silver_view = f"{catalog}.{silver_schema}.vw_taxi_zones_clean"
zones_df = spark.table(bronze_table)
required = {"LocationID", "Borough", "Zone", "service_zone"}
if not required.issubset(zones_df.columns):
    raise ValueError(f"Missing source columns: {sorted(required - set(zones_df.columns))}")
checks = zones_df.agg(
    F.count("*").alias("rows"),
    F.countDistinct("LocationID").alias("unique_ids"),
    F.sum(F.col("LocationID").isNull().cast("long")).alias("missing_ids"),
    F.sum((F.col("Zone").isNull() | (F.trim("Zone") == "")).cast("long")).alias("missing_zones"),
).first()
if checks.rows == 0 or checks.rows != checks.unique_ids or checks.missing_ids or checks.missing_zones:
    raise ValueError(f"Taxi Zone key or zone-name quality failed: {checks.asDict()}")
print(f"Validated {checks.rows} distinct Taxi Zones")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW {silver_view} AS
    SELECT
        CAST(LocationID AS INT) AS location_id,
        NULLIF(TRIM(Borough), '') AS borough,
        TRIM(Zone) AS zone,
        NULLIF(TRIM(service_zone), '') AS service_zone
    FROM {bronze_table}
""")
display(spark.sql(f"SELECT COUNT(*) AS silver_rows, COUNT(DISTINCT location_id) AS unique_ids FROM {silver_view}"))
