# Databricks notebook source

# COMMAND ----------

# # Pipeline · Volume to Gold quality
# Use as a Databricks Job notebook task after the three source directories contain complete monthly files. The run stops on a failed preflight, load, or quality gate; analytics views remain queryable afterward.

# COMMAND ----------

import json
import re

for name, value in (
    ("taxi_source_dir", "/Volumes/nyc_mobility/nyc_bronze/ftw-b12-de-r2/groups/week-08/group-c/landing/green_taxi"),
    ("weather_source_dir", "/Volumes/nyc_mobility/nyc_bronze/ftw-b12-de-r2/groups/week-08/group-c/landing/weather"),
    ("zones_source_dir", "/Volumes/nyc_mobility/nyc_bronze/ftw-b12-de-r2/groups/week-08/group-c/landing/taxi_zones"),
    ("zone_file", "taxi_zone_lookup.csv"),
    ("catalog", "nyc_mobility"),
    ("bronze_schema", "nyc_bronze"),
    ("silver_schema", "nyc_silver"),
    ("gold_schema", "nyc_gold"),
    ("quality_schema", "nyc_quality"),
    ("max_valid_distance_miles", "100"),
):
    dbutils.widgets.text(name, value)

catalog = dbutils.widgets.get("catalog").strip()
bronze_schema = dbutils.widgets.get("bronze_schema").strip()
silver_schema = dbutils.widgets.get("silver_schema").strip()
gold_schema = dbutils.widgets.get("gold_schema").strip()
for name in (catalog, bronze_schema, silver_schema, gold_schema):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Invalid catalog/schema: {name!r}")
args = {name: dbutils.widgets.get(name).strip() for name in (
    "taxi_source_dir", "weather_source_dir", "zones_source_dir", "zone_file",
    "catalog", "bronze_schema", "quality_schema",
)}
manifest = json.loads(dbutils.notebook.run("../02_bronze/bronze_auto_ingest", 0, args))
window = {"start_month": manifest["start_month"], "end_month": manifest["end_month"]}
shared = {"catalog": catalog, "bronze_schema": bronze_schema, "silver_schema": silver_schema}
tasks = [
    ("Taxi Silver", "../03_silver/green_taxi_clean", {**shared, **window}),
    ("Weather Silver", "../03_silver/weather_clean", {**shared, **window}),
    ("Zones Silver", "../03_silver/taxi_zones_clean", shared),
    ("Taxi quality", "../04_quality/green_taxi_quality", shared),
    ("Zones and Weather quality", "../04_quality/zones_weather_quality",
     {"catalog": catalog, "silver_schema": silver_schema}),
    ("Gold", "../04_gold/gold_marts",
     {"catalog": catalog, "silver_schema": silver_schema, "gold_schema": gold_schema,
      "max_valid_distance_miles": dbutils.widgets.get("max_valid_distance_miles").strip()}),
    ("Gold quality", "../04_quality/gold_quality",
     {"catalog": catalog, "silver_schema": silver_schema, "gold_schema": gold_schema}),
]
for label, notebook, parameters in tasks:
    print(f"Starting {label} for {window}")
    dbutils.notebook.run(notebook, 0, parameters)
    print(f"Passed {label}")
print(f"Pipeline PASS: {window}; bronze run_id={manifest['run_id']}")
