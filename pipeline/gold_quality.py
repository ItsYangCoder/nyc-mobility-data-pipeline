"""Databricks Spark stage; callable directly from Python tests."""

from runtime import Parameters, execute

def run(spark, options=None, dbutils=None, display=None):
    params = Parameters(options)
    if display is None:
        display = lambda frame: frame.show(20, truncate=False)
    if dbutils is None:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)

    # Run after gold_marts. Fails on duplicate keys, orphan dimensions, missing weather, multiplied joins or mismatched Silver counts. Reports flagged distance outliers without removing trips.


    import re

    params.register("catalog", "nyc_mobility", "Catalog")
    params.register("silver_schema", "nyc_silver", "Silver schema")
    params.register("gold_schema", "nyc_gold", "Gold schema")
    catalog = params.get("catalog").strip()
    silver_schema = params.get("silver_schema").strip()
    gold_schema = params.get("gold_schema").strip()
    for name in (catalog, silver_schema, gold_schema):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise ValueError(f"Invalid catalog/schema name: {name!r}")
    gold = f"{catalog}.{gold_schema}"
    silver = f"{catalog}.{silver_schema}"
    taxi = f"{gold}.fact_taxi_trip"
    weather = f"{gold}.dim_weather_hour"
    date_dim = f"{gold}.dim_date"
    hour_dim = f"{gold}.dim_hour"
    zone_dim = f"{gold}.dim_zone"


    for table, key in ((date_dim, "date_key"), (hour_dim, "hour_key"),
                       (zone_dim, "location_id"), (taxi, "trip_key"),
                       (weather, "observation_hour")):
        result = spark.sql(f"SELECT COUNT(*) AS n, COUNT(DISTINCT {key}) AS keys FROM {table}").first()
        print(f"{table}: rows={result.n}, distinct_keys={result.keys}")
        if result.n == 0 or result.n != result.keys:
            raise ValueError(f"Missing or duplicate key: {table}.{key}")

    silver_taxi = spark.table(f"{silver}.vw_green_taxi_clean").count()
    silver_weather = spark.table(f"{silver}.vw_weather_hourly_clean").count()
    if spark.table(taxi).count() != silver_taxi or spark.table(weather).count() != silver_weather:
        raise ValueError("Gold Taxi fact or Weather dimension rows do not reconcile with Silver")

    checks = spark.sql(f"""
        SELECT COUNT(*) AS joined_rows,
               COUNT_IF(pd.date_key IS NULL OR dd.date_key IS NULL) AS missing_taxi_date,
               COUNT_IF(ph.hour_key IS NULL OR dh.hour_key IS NULL) AS missing_taxi_hour,
               COUNT_IF(p.location_id IS NULL OR d.location_id IS NULL) AS missing_zone,
               COUNT_IF(w.observation_hour IS NULL) AS missing_weather
        FROM {taxi} t
        LEFT JOIN {date_dim} pd ON t.pickup_date_key = pd.date_key
        LEFT JOIN {date_dim} dd ON t.dropoff_date_key = dd.date_key
        LEFT JOIN {hour_dim} ph ON t.pickup_hour_key = ph.hour_key
        LEFT JOIN {hour_dim} dh ON t.dropoff_hour_key = dh.hour_key
        LEFT JOIN {zone_dim} p ON t.pickup_location_id = p.location_id
        LEFT JOIN {zone_dim} d ON t.dropoff_location_id = d.location_id
        LEFT JOIN {weather} w ON t.weather_hour = w.observation_hour
    """).first()
    print(checks.asDict())
    if checks.joined_rows != silver_taxi or any(checks[k] for k in
        ("missing_taxi_date", "missing_taxi_hour", "missing_zone", "missing_weather")):
        raise ValueError(f"Gold join quality failed: {checks.asDict()}")
    print("Single-fact Gold grain, reconciliation and joins: PASS")
    # The source distance stays unchanged; only distance aggregates ignore flagged rows.
    distance_checks = spark.sql(f"""
        SELECT COUNT_IF(is_distance_outlier) AS flagged_distance_trips,
               COUNT_IF(is_distance_outlier IS NULL) AS missing_distance_flags,
               COUNT_IF(is_distance_outlier AND valid_trip_distance IS NOT NULL) AS invalid_flagged_distance
        FROM {taxi}
    """).first()
    print(f"Flagged distance trips: {distance_checks.flagged_distance_trips} of {silver_taxi}")
    if distance_checks.missing_distance_flags or distance_checks.invalid_flagged_distance:
        raise ValueError(f"Invalid distance flags: {distance_checks.asDict()}")


if __name__ == "__main__":
    execute(__file__, run)
