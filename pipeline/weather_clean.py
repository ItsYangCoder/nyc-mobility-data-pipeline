"""Databricks Spark stage; callable directly from Python tests."""

from runtime import Parameters, execute

def run(spark, options=None, dbutils=None, display=None):
    params = Parameters(options)
    if display is None:
        display = lambda frame: frame.show(20, truncate=False)
    if dbutils is None:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)

    # Supports flat hourly data and the nested Open-Meteo `hourly` array structure. Validates fields before replacing the view. Weather timestamps are interpreted as local NYC time when source values have no timezone; confirm the source timezone before joining to Taxi.


    from quality_rules import month_window
    import re
    from pyspark.sql.types import StructType, ArrayType

    params.register("start_month", "", "First month YYYY-MM")
    params.register("end_month", "", "Last month YYYY-MM (inclusive)")
    params.register("time_column", "", "Flat hourly time column (blank = detect)")
    params.register("catalog", "nyc_mobility", "Catalog")
    params.register("bronze_schema", "nyc_bronze", "Bronze schema")
    params.register("silver_schema", "nyc_silver", "Silver schema")

    start, end_exclusive = month_window(params.get("start_month"), params.get("end_month"))
    end = end_exclusive
    catalog = params.get("catalog").strip()
    bronze_schema = params.get("bronze_schema").strip()
    silver_schema = params.get("silver_schema").strip()
    for name in (catalog, bronze_schema, silver_schema):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise ValueError(f"Invalid catalog/schema: {name!r}")
    bronze_table = f"{catalog}.{bronze_schema}.weather_raw"
    silver_view = f"{catalog}.{silver_schema}.vw_weather_hourly_clean"
    schema = spark.table(bronze_table).schema
    fields = {field.name: field for field in schema.fields}
    metadata_names = [name for name in ("timezone", "latitude", "longitude") if name in fields]
    metadata_sql = ", ".join(f"`{name}` AS `{name}`" for name in metadata_names)
    hourly = fields.get("hourly")
    if hourly is not None and isinstance(hourly.dataType, StructType):
        hourly_fields = {f.name: f for f in hourly.dataType.fields}
        if "time" not in hourly_fields or not isinstance(hourly_fields["time"].dataType, ArrayType):
            raise ValueError("Nested hourly.time must be an array")
        metric_names = [name for name in ("temperature_2m", "precipitation", "wind_speed_10m", "rain", "snowfall")
                        if name in hourly_fields and isinstance(hourly_fields[name].dataType, ArrayType)]
        if not metric_names:
            raise ValueError("No supported hourly weather metric arrays; inspect Weather inventory")
        names = ["time"] + metric_names
        zipped = ", ".join(f"hourly.`{name}`" for name in names)
        metric_sql = [f"TRY_CAST(h.`{name}` AS DOUBLE) AS {name}" for name in metric_names]
        expanded = f"SELECT h.`time` AS raw_time, {', '.join(metric_sql)}{(', ' + metadata_sql) if metadata_sql else ''} FROM {bronze_table} LATERAL VIEW EXPLODE(ARRAYS_ZIP({zipped})) x AS h"
    else:
        selected = params.get("time_column").strip()
        candidates = (selected,) if selected else ("time", "timestamp", "datetime", "observation_time", "date")
        time_name = next((name for name in candidates if name in fields), None)
        if not time_name:
            raise ValueError(f"No hourly time field detected. Set time_column; available fields: {list(fields)}")
        metric_names = [name for name in ("temperature_2m", "precipitation", "wind_speed_10m", "rain", "snowfall") if name in fields]
        if not metric_names:
            raise ValueError(f"No supported weather metrics; inspect the source schema: {list(fields)}")
        metric_sql = [f"TRY_CAST(`{name}` AS DOUBLE) AS {name}" for name in metric_names]
        expanded = f"SELECT `{time_name}` AS raw_time, {', '.join(metric_sql)}{(', ' + metadata_sql) if metadata_sql else ''} FROM {bronze_table}"
    if "temperature_2m" not in metric_names:
        raise ValueError("An hourly temperature_2m field is required for the planned weather mart")
    print(f"Columns: {metric_names}; date window: {start} to {end} (exclusive)")


    # Validate timestamps before replacing the view so malformed source dates cannot vanish silently.
    invalid = spark.sql(f"""
        SELECT COUNT(*) AS bad_times FROM ({expanded}) src
        WHERE raw_time IS NULL OR TRY_CAST(raw_time AS TIMESTAMP_NTZ) IS NULL
    """).first().bad_times
    if invalid:
        raise ValueError(f"Weather data contains {invalid} invalid time values; inspect source before publishing Silver")
    select_metrics = ", ".join(metric_names + metadata_names)
    spark.sql(f"""
        CREATE OR REPLACE VIEW {silver_view} AS
        SELECT TRY_CAST(raw_time AS TIMESTAMP_NTZ) AS observation_hour, {select_metrics}
        FROM ({expanded}) weather
        WHERE TRY_CAST(raw_time AS TIMESTAMP_NTZ) >= TIMESTAMP_NTZ '{start} 00:00:00'
          AND TRY_CAST(raw_time AS TIMESTAMP_NTZ) < TIMESTAMP_NTZ '{end} 00:00:00'
    """)
    display(spark.sql(f"""
        SELECT COUNT(*) AS silver_rows, MIN(observation_hour) AS first_hour,
               MAX(observation_hour) AS last_hour,
               COUNT(*) - COUNT(DISTINCT observation_hour) AS duplicate_hours
        FROM {silver_view}
    """))


if __name__ == "__main__":
    execute(__file__, run)
