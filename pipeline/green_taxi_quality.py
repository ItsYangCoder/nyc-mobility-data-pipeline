"""Databricks Spark stage; callable directly from Python tests."""

from runtime import Parameters, execute

def run(spark, options=None, dbutils=None, display=None):
    params = Parameters(options)
    if display is None:
        display = lambda frame: frame.show(20, truncate=False)
    if dbutils is None:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)

    # Run after Silver. The date window is taken from the Silver view, while Bronze quality checks cover all loaded files.


    import re

    params.register("catalog", "nyc_mobility", "Target catalog")
    params.register("bronze_schema", "nyc_bronze", "Bronze schema")
    params.register("silver_schema", "nyc_silver", "Silver schema")
    catalog = params.get("catalog").strip()
    bronze_schema = params.get("bronze_schema").strip()
    silver_schema = params.get("silver_schema").strip()
    for identifier in (catalog, bronze_schema, silver_schema):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
            raise ValueError(f"Invalid catalog or schema: {identifier!r}")
    bronze_table = f"{catalog}.{bronze_schema}.green_taxi_raw"
    silver_view = f"{catalog}.{silver_schema}.vw_green_taxi_clean"


    display(spark.sql(f"""
        SELECT COUNT(*) AS bronze_rows,
               COUNT_IF(lpep_pickup_datetime IS NULL) AS missing_pickup,
               COUNT_IF(lpep_dropoff_datetime IS NULL) AS missing_dropoff,
               COUNT_IF(lpep_dropoff_datetime < lpep_pickup_datetime) AS reversed_time,
               COUNT_IF(trip_distance IS NULL) AS missing_distance,
               COUNT_IF(trip_distance = 0) AS zero_distance,
               COUNT_IF(trip_distance < 0) AS negative_distance,
               COUNT_IF(PULocationID IS NULL OR DOLocationID IS NULL) AS missing_zone
        FROM {bronze_table}
    """))
    display(spark.sql(f"""
        SELECT COUNT(*) AS silver_rows,
               COUNT_IF(is_zero_distance) AS zero_distance_rows
        FROM {silver_view}
    """))


    bronze_df = spark.table(bronze_table)
    duplicate_groups = bronze_df.groupBy(*bronze_df.columns).count().filter("count > 1")
    display(duplicate_groups.selectExpr(
        "count(*) AS duplicate_groups",
        "coalesce(sum(count - 1), 0) AS extra_rows",
    ))


if __name__ == "__main__":
    execute(run)
