"""Databricks Spark stage; callable directly from Python tests."""

from runtime import Parameters, execute

def run(spark, options=None, dbutils=None, display=None):
    params = Parameters(options)
    if display is None:
        display = lambda frame: frame.show(20, truncate=False)
    import re
    params.register("catalog", "nyc_mobility")
    params.register("gold_schema", "nyc_gold")
    catalog, gold_schema = params.get("catalog"), params.get("gold_schema")
    if not all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) for name in (catalog, gold_schema)):
        raise ValueError("Invalid catalog or Gold schema")

    # Pickup grain: date + hour + zone. Higher trip count means more observed Green Taxi pickups, not all NYC trips.
    # The Gold outlier flag excludes extreme source distances from distance metrics; all trips remain in demand counts. Review the configured threshold before presenting conclusions.


    display(spark.sql(f"""
    SELECT d.full_date, d.day_name, h.hour, z.borough, z.zone,
           COUNT(*) AS trip_count,
           COUNT_IF(t.is_distance_outlier) AS flagged_distance_trips,
           ROUND(SUM(t.valid_trip_distance), 2) AS total_distance,
           ROUND(AVG(t.valid_trip_distance), 2) AS avg_trip_distance,
           ROUND(SUM(CASE WHEN t.fare_amount >= 0 THEN t.fare_amount END), 2) AS nonnegative_fare_total
    FROM {catalog}.{gold_schema}.fact_taxi_trip t
    JOIN {catalog}.{gold_schema}.dim_date d ON t.pickup_date_key = d.date_key
    JOIN {catalog}.{gold_schema}.dim_hour h ON t.pickup_hour_key = h.hour_key
    JOIN {catalog}.{gold_schema}.dim_zone z ON t.pickup_location_id = z.location_id
    GROUP BY d.full_date, d.day_name, h.hour, z.borough, z.zone
    ORDER BY trip_count DESC, d.full_date, h.hour
    LIMIT 100
    """))


if __name__ == "__main__":
    execute(run)
