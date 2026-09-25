"""Databricks Spark stage; callable directly from Python tests."""

from runtime import Parameters, execute

def run(spark, options=None, dbutils=None, display=None):
    params = Parameters(options)
    if display is None:
        display = lambda frame: frame.show(20, truncate=False)
    if dbutils is None:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)

    # Run after all three Silver views. Gold objects are views; reruns replace definitions. Weather describes one observation location and is joined at pickup hour. Distances over the configurable limit (default 100 miles) are flagged, retained in trip counts, and excluded only from distance metrics. This is a provisional analytical threshold, not a change to the source. The current sources do not capture the original ingestion time.


    import re
    from quality_rules import distance_limit, distance_projection_sql, distance_summary_sql
    from pyspark.sql import functions as F

    params.register("catalog", "nyc_mobility", "Catalog")
    params.register("silver_schema", "nyc_silver", "Silver schema")
    params.register("gold_schema", "nyc_gold", "Gold schema")
    params.register("max_valid_distance_miles", "100", "Maximum valid trip distance (miles)")
    catalog = params.get("catalog").strip()
    silver_schema = params.get("silver_schema").strip()
    gold_schema = params.get("gold_schema").strip()
    max_valid_distance_miles = distance_limit(params.get("max_valid_distance_miles"))
    for name in (catalog, silver_schema, gold_schema):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise ValueError(f"Invalid catalog/schema name: {name!r}")

    taxi_view = f"{catalog}.{silver_schema}.vw_green_taxi_clean"
    zones_view = f"{catalog}.{silver_schema}.vw_taxi_zones_clean"
    weather_view = f"{catalog}.{silver_schema}.vw_weather_hourly_clean"
    date_dim = f"{catalog}.{gold_schema}.dim_date"
    hour_dim = f"{catalog}.{gold_schema}.dim_hour"
    zone_dim = f"{catalog}.{gold_schema}.dim_zone"
    taxi_fact = f"{catalog}.{gold_schema}.fact_taxi_trip"
    weather_dim = f"{catalog}.{gold_schema}.dim_weather_hour"
    daily_view = f"{catalog}.{gold_schema}.vw_mobility_daily"

    taxi = spark.table(taxi_view)
    zones = spark.table(zones_view)
    weather = spark.table(weather_view)
    taxi_columns = ["pickup_datetime", "dropoff_datetime", "pickup_zone_id",
                    "dropoff_zone_id", "passenger_count", "trip_distance",
                    "fare_amount", "tip_amount", "total_amount", "is_zero_distance"]
    for frame, required in ((taxi, taxi_columns),
                            (zones, ["location_id", "borough", "zone", "service_zone"]),
                            (weather, ["observation_hour", "temperature_2m", "precipitation",
                                       "wind_speed_10m", "timezone"])):
        missing = set(required) - set(frame.columns)
        if missing:
            raise ValueError(f"Silver view lacks columns: {sorted(missing)}")
    print("Silver sources are available")


    # Fail before replacing views if a source grain could multiply analytical rows.
    taxi_rows = taxi.count()
    zone_rows = zones.count()
    weather_rows = weather.count()
    if taxi_rows == 0 or taxi.select(*taxi_columns).distinct().count() != taxi_rows:
        raise ValueError("Taxi Silver rows must be nonempty and distinct on the key fields")
    if zone_rows == 0 or zones.filter("location_id IS NULL").limit(1).count() or zones.select("location_id").distinct().count() != zone_rows:
        raise ValueError("Zone Silver IDs must be non-null and unique")
    if weather_rows == 0 or weather.filter("observation_hour IS NULL").limit(1).count() or weather.select("observation_hour").distinct().count() != weather_rows:
        raise ValueError("Weather Silver observation_hour must be non-null and unique")
    if weather.filter("timezone IS NULL OR timezone <> 'America/New_York'").limit(1).count():
        raise ValueError("Weather timestamps must use the America/New_York source timezone")

    pickup_hour = F.expr("CAST(DATE_FORMAT(pickup_datetime, 'yyyy-MM-dd HH:00:00') AS TIMESTAMP_NTZ)")
    matched = (taxi.withColumn("pickup_weather_hour", pickup_hour)
        .join(zones.select(F.col("location_id").alias("pickup_match")),
              F.col("pickup_zone_id") == F.col("pickup_match"), "left")
        .join(zones.select(F.col("location_id").alias("dropoff_match")),
              F.col("dropoff_zone_id") == F.col("dropoff_match"), "left")
        .join(weather.select(F.col("observation_hour").alias("weather_match")),
              F.col("pickup_weather_hour") == F.col("weather_match"), "left")
        .agg(F.count("*").alias("joined_rows"),
             F.sum(F.col("pickup_match").isNull().cast("long")).alias("missing_pickup_zone"),
             F.sum(F.col("dropoff_match").isNull().cast("long")).alias("missing_dropoff_zone"),
             F.sum(F.col("weather_match").isNull().cast("long")).alias("missing_weather_hour"))
        .first())
    if matched.joined_rows != taxi_rows or any(matched[k] for k in
        ("missing_pickup_zone", "missing_dropoff_zone", "missing_weather_hour")):
        raise ValueError(f"Source relationship check failed: {matched.asDict()}")
    print(f"Validated grains and joins: taxi={taxi_rows}, zones={zone_rows}, weather={weather_rows}")


    # Shared dimensions. Date members follow the current Silver windows.
    spark.sql(f"""
        CREATE OR REPLACE VIEW {date_dim} AS
        WITH days AS (
          SELECT TO_DATE(pickup_datetime) AS full_date FROM {taxi_view}
          UNION
          SELECT TO_DATE(dropoff_datetime) AS full_date FROM {taxi_view}
          UNION
          SELECT TO_DATE(observation_hour) AS full_date FROM {weather_view}
        )
        SELECT CAST(DATE_FORMAT(full_date, 'yyyyMMdd') AS INT) AS date_key,
               full_date, YEAR(full_date) AS year, MONTH(full_date) AS month,
               DAY(full_date) AS day_of_month,
               WEEKDAY(full_date) + 1 AS day_of_week,
               DATE_FORMAT(full_date, 'EEEE') AS day_name,
               WEEKDAY(full_date) >= 5 AS is_weekend
        FROM days WHERE full_date IS NOT NULL
    """)
    spark.sql(f"""
        CREATE OR REPLACE VIEW {hour_dim} AS
        SELECT hour_key, hour_key AS hour FROM (SELECT EXPLODE(SEQUENCE(0, 23)) AS hour_key)
    """)
    spark.sql(f"""
        CREATE OR REPLACE VIEW {zone_dim} AS
        SELECT location_id, borough, zone, service_zone FROM {zones_view}
    """)
    spark.sql(f"""
        CREATE OR REPLACE VIEW {weather_dim} AS
        SELECT observation_hour, temperature_2m, precipitation, wind_speed_10m,
               timezone, CASE WHEN precipitation IS NULL THEN NULL
                              ELSE precipitation > 0 END AS precipitation_flag
        FROM {weather_view}
    """)
    print("Created dim_date, dim_hour, dim_zone and dim_weather_hour")


    # One fact per retained Silver Taxi row; the hourly weather observation is a dimension.
    # trip_key is a fingerprint of Silver values because the source does not include a trip ID.
    trip_values = ", ".join(taxi_columns)
    spark.sql(f"""
        CREATE OR REPLACE VIEW {taxi_fact} AS
        SELECT SHA2(TO_JSON(STRUCT({trip_values})), 256) AS trip_key,
               CAST(DATE_FORMAT(pickup_datetime, 'yyyyMMdd') AS INT) AS pickup_date_key,
               CAST(DATE_FORMAT(dropoff_datetime, 'yyyyMMdd') AS INT) AS dropoff_date_key,
               HOUR(pickup_datetime) AS pickup_hour_key,
               HOUR(dropoff_datetime) AS dropoff_hour_key,
               pickup_zone_id AS pickup_location_id,
               dropoff_zone_id AS dropoff_location_id,
               CAST(DATE_FORMAT(pickup_datetime, 'yyyy-MM-dd HH:00:00') AS TIMESTAMP_NTZ) AS weather_hour,
               pickup_datetime, dropoff_datetime, passenger_count, trip_distance,
               TIMESTAMPDIFF(SECOND, pickup_datetime, dropoff_datetime) / 60.0 AS trip_duration_minutes,
               fare_amount, tip_amount, total_amount, is_zero_distance,
               {distance_projection_sql(max_valid_distance_miles)}
        FROM {taxi_view}
    """)
    spark.sql(f"""
        CREATE OR REPLACE VIEW {daily_view} AS
        SELECT pickup_date_key AS date_key, pickup_location_id AS location_id,
               {distance_summary_sql()},
               SUM(CASE WHEN fare_amount >= 0 THEN fare_amount END) AS nonnegative_fare_total
        FROM {taxi_fact}
        GROUP BY pickup_date_key, pickup_location_id
    """)
    display(spark.sql(f"SELECT COUNT(*) AS taxi_trips, COUNT(DISTINCT trip_key) AS unique_trip_keys FROM {taxi_fact}"))
    display(spark.sql(f"SELECT COUNT(*) AS weather_hours, COUNT(DISTINCT observation_hour) AS unique_weather_hours FROM {weather_dim}"))
    display(spark.sql(f"SELECT COUNT(*) AS zone_rows FROM {zone_dim}"))
    print("Created one fact, four dimensions and the daily mobility output")
    # Prior Gold runs created a virtual weather fact. Remove it after the replacement views exist.
    spark.sql(f"DROP VIEW IF EXISTS {catalog}.{gold_schema}.fact_weather_hourly")
    print("Retired the previous fact_weather_hourly view")


if __name__ == "__main__":
    execute(__file__, run)
