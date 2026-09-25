# Databricks notebook source

# COMMAND ----------

# # 04 · Taxi Zones and Weather checks
# Run after both Silver notebooks. Weather checks report duplicate hours; assess expected grain before joining weather to taxi.

# COMMAND ----------

import re

dbutils.widgets.text("catalog", "nyc_mobility", "Catalog")
dbutils.widgets.text("silver_schema", "nyc_silver", "Silver schema")
catalog = dbutils.widgets.get("catalog").strip()
silver_schema = dbutils.widgets.get("silver_schema").strip()
for name in (catalog, silver_schema):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Invalid catalog/schema: {name!r}")
zones = f"{catalog}.{silver_schema}.vw_taxi_zones_clean"
weather = f"{catalog}.{silver_schema}.vw_weather_hourly_clean"

# COMMAND ----------

display(spark.sql(f"""
    SELECT COUNT(*) AS zones, COUNT(DISTINCT location_id) AS unique_zone_ids,
           COUNT_IF(location_id IS NULL OR zone IS NULL OR zone = '') AS invalid_zones
    FROM {zones}
"""))
display(spark.sql(f"""
    SELECT COUNT(*) AS weather_rows, COUNT(DISTINCT observation_hour) AS unique_hours,
           COUNT_IF(observation_hour IS NULL) AS missing_time,
           COUNT_IF(temperature_2m IS NULL) AS missing_temperature
    FROM {weather}
"""))
