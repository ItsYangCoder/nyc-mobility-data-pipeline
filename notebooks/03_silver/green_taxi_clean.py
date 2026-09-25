# Databricks notebook source

# COMMAND ----------

# # 03 · Build a clean Silver view
# The date window comes from widgets. Run this notebook after Bronze; rerunning replaces the view with the selected window. Zero-distance rides are retained and flagged.

# COMMAND ----------

from silver_taxi import build_silver_taxi

dbutils.widgets.text("start_month", "2026-03", "First month (YYYY-MM)")
dbutils.widgets.text("end_month", "2026-05", "Last month (YYYY-MM, inclusive)")
dbutils.widgets.text("catalog", "nyc_mobility", "Target catalog")
dbutils.widgets.text("bronze_schema", "nyc_bronze", "Bronze schema")
dbutils.widgets.text("silver_schema", "nyc_silver", "Silver schema")

silver_view = build_silver_taxi(
    spark,
    start_month=dbutils.widgets.get("start_month"),
    end_month=dbutils.widgets.get("end_month"),
    catalog=dbutils.widgets.get("catalog").strip(),
    bronze_schema=dbutils.widgets.get("bronze_schema").strip(),
    silver_schema=dbutils.widgets.get("silver_schema").strip(),
)
print(f"Created {silver_view}")

# COMMAND ----------

display(spark.sql(f"""
    SELECT COUNT(*) AS silver_rows,
           COUNT_IF(is_zero_distance) AS zero_distance_rows
    FROM {silver_view}
"""))
