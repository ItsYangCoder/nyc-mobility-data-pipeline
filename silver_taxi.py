"""Build the Green Taxi Silver view from a selected month window."""

import argparse
import re

from quality_rules import month_window


def build_silver_taxi(spark, *, start_month, end_month, catalog, bronze_schema, silver_schema):
    """Replace the Silver view and return its fully qualified name."""
    start, end_exclusive = month_window(start_month, end_month)
    for identifier in (catalog, bronze_schema, silver_schema):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
            raise ValueError(f"Invalid catalog or schema: {identifier!r}")

    bronze_table = f"{catalog}.{bronze_schema}.green_taxi_raw"
    silver_view = f"{catalog}.{silver_schema}.vw_green_taxi_clean"
    spark.sql(f"""
        CREATE OR REPLACE VIEW {silver_view} AS
        SELECT
            lpep_pickup_datetime AS pickup_datetime,
            lpep_dropoff_datetime AS dropoff_datetime,
            PULocationID AS pickup_zone_id,
            DOLocationID AS dropoff_zone_id,
            passenger_count,
            trip_distance,
            fare_amount,
            tip_amount,
            total_amount,
            trip_distance = 0 AS is_zero_distance
        FROM {bronze_table}
        WHERE lpep_pickup_datetime >= TIMESTAMP_NTZ '{start} 00:00:00'
          AND lpep_pickup_datetime < TIMESTAMP_NTZ '{end_exclusive} 00:00:00'
          AND lpep_dropoff_datetime >= lpep_pickup_datetime
    """)
    return silver_view


def main(argv=None):
    parser = argparse.ArgumentParser(description="Build the Green Taxi Silver view")
    for name in ("start_month", "end_month", "catalog", "bronze_schema", "silver_schema"):
        parser.add_argument(f"--{name.replace('_', '-')}", required=True)
    args = parser.parse_args(argv)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    view = build_silver_taxi(spark, **vars(args))
    spark.sql(f"SELECT COUNT(*) AS silver_rows, COUNT_IF(is_zero_distance) AS zero_distance_rows FROM {view}").show()


if __name__ == "__main__":
    main()
