"""Databricks Spark stage; callable directly from Python tests."""

from runtime import Parameters, execute

def run(spark, options=None, dbutils=None, display=None):
    params = Parameters(options)
    if display is None:
        display = lambda frame: frame.show(20, truncate=False)
    if dbutils is None:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)

    # Validate the primary key first, then replace the Silver view. Whitespace is trimmed without changing the original Bronze rows.


    import re
    from pyspark.sql import functions as F

    params.register("catalog", "nyc_mobility", "Catalog")
    params.register("bronze_schema", "nyc_bronze", "Bronze schema")
    params.register("silver_schema", "nyc_silver", "Silver schema")
    catalog = params.get("catalog").strip()
    bronze_schema = params.get("bronze_schema").strip()
    silver_schema = params.get("silver_schema").strip()
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


if __name__ == "__main__":
    execute(run)
