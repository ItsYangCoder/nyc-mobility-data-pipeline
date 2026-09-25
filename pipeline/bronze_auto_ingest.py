"""Databricks Spark stage; callable directly from Python tests."""

from runtime import Parameters, execute

def run(spark, options=None, dbutils=None, display=None):
    params = Parameters(options)
    if display is None:
        display = lambda frame: frame.show(20, truncate=False)
    if dbutils is None:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)

    from pathlib import Path
    from runtime import run_file



    # Runs complete matching source months from the existing Volume. Preflight precedes COPY INTO; each attempted file is recorded in nyc_quality.bronze_file_runs. Same-path source replacements require a planned backfill.


    import json
    import re
    import uuid
    from datetime import datetime, timezone

    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, StructType
    from source_discovery import plan_batch, validate_weather_hour_stats

    for name, value in (
        ("taxi_source_dir", ""),
        ("weather_source_dir", ""),
        ("zones_source_dir", ""),
        ("zone_file", "taxi_zone_lookup.csv"),
        ("catalog", "nyc_mobility"),
        ("bronze_schema", "nyc_bronze"),
        ("quality_schema", "nyc_quality"),
    ):
        params.register(name, value)

    config = {name: params.get(name).strip().rstrip("/")
              for name in ("taxi_source_dir", "weather_source_dir", "zones_source_dir")}
    for path in config.values():
        if not re.fullmatch(r"/Volumes/[A-Za-z0-9_./-]+", path):
            raise ValueError(f"Invalid Volume directory: {path!r}")
    catalog = params.get("catalog").strip()
    bronze_schema = params.get("bronze_schema").strip()
    quality_schema = params.get("quality_schema").strip()
    for identifier in (catalog, bronze_schema, quality_schema):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
            raise ValueError(f"Invalid catalog/schema name: {identifier!r}")

    batch = plan_batch(
        dbutils.fs.ls(config["taxi_source_dir"]),
        dbutils.fs.ls(config["weather_source_dir"]),
        dbutils.fs.ls(config["zones_source_dir"]),
        params.get("zone_file").strip(),
    )
    print(f"Preflight months: {batch.start_month} to {batch.end_month}")
    print(f"Taxi: {len(batch.taxi_months)} files; Weather: {len(batch.weather_files)} files; Zones: {batch.zone_file}")


    # Validate source schemas and weather coverage before any COPY INTO writes.
    taxi_required = {
        "lpep_pickup_datetime", "lpep_dropoff_datetime", "PULocationID",
        "DOLocationID", "passenger_count", "trip_distance",
        "fare_amount", "tip_amount", "total_amount",
    }
    reference_types = None
    for month in batch.taxi_months:
        frame = spark.read.parquet(f"{config['taxi_source_dir']}/green_tripdata_{month}.parquet")
        missing = taxi_required - set(frame.columns)
        if missing:
            raise ValueError(f"Taxi {month} missing fields: {sorted(missing)}")
        types = {name: frame.schema[name].dataType.simpleString() for name in taxi_required}
        if reference_types is not None and types != reference_types:
            raise ValueError(f"Taxi schema drift in {month}: {types}")
        reference_types = types

    reference_weather_schema = None
    for filename in batch.weather_files:
        path = f"{config['weather_source_dir']}/{filename}"
        ext = filename.rsplit(".", 1)[-1]
        if ext == "json":
            frame = spark.read.option("multiLine", "true").json(path)
        elif ext == "csv":
            frame = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
        else:
            frame = spark.read.parquet(path)
        if "timezone" not in frame.columns or frame.filter(
            "timezone IS NULL OR timezone <> 'America/New_York'"
        ).limit(1).count():
            raise ValueError(f"Weather timezone must be America/New_York: {filename}")
        hourly = frame.schema["hourly"].dataType if "hourly" in frame.columns else None
        if isinstance(hourly, StructType):
            fields = {field.name: field.dataType for field in hourly.fields}
            if "time" not in fields or not isinstance(fields["time"], ArrayType):
                raise ValueError(f"Weather hourly.time missing: {filename}")
            if "temperature_2m" not in fields or not isinstance(fields["temperature_2m"], ArrayType):
                raise ValueError(f"Weather temperature_2m missing: {filename}")
            shape = ("nested", tuple(sorted(
                (name, data_type.simpleString()) for name, data_type in fields.items()
            )))
            for name, data_type in fields.items():
                if isinstance(data_type, ArrayType) and frame.filter(
                    F.col("hourly.time").isNull()
                    | F.col(f"hourly.`{name}`").isNull()
                    | (F.size(F.col(f"hourly.`{name}`")) != F.size("hourly.time"))
                ).limit(1).count():
                    raise ValueError(f"Weather array length mismatch ({name}): {filename}")
            hours = frame.select(F.explode("hourly.time").alias("hour"))
        else:
            time_col = next((col for col in ("time", "timestamp", "datetime", "observation_time", "date")
                             if col in frame.columns), None)
            if time_col is None or "temperature_2m" not in frame.columns:
                raise ValueError(f"Missing flat Weather time or temperature: {filename}")
            shape = ("flat", time_col, tuple(sorted(
                (name, frame.schema[name].dataType.simpleString())
                for name in ("temperature_2m", "precipitation", "wind_speed_10m")
                if name in frame.columns
            )))
            hours = frame.select(F.col(time_col).cast("string").alias("hour"))
        if reference_weather_schema is not None and shape != reference_weather_schema:
            raise ValueError(f"Weather schema/layout drift: {filename}")
        reference_weather_schema = shape
        hours = hours.select(F.date_format(F.expr("TRY_CAST(hour AS TIMESTAMP_NTZ)"),
                                          "yyyy-MM-dd'T'HH:mm").alias("hour"))
        stats = hours.agg(
            F.count("*").alias("rows"),
            F.countDistinct("hour").alias("unique"),
            F.min("hour").alias("first"),
            F.max("hour").alias("last"),
        ).first()
        validate_weather_hour_stats(stats, filename)

    zones = spark.read.option("header", "true").csv(
        f"{config['zones_source_dir']}/{batch.zone_file}"
    )
    missing_zones = {"LocationID", "Borough", "Zone", "service_zone"} - set(zones.columns)
    if missing_zones:
        raise ValueError(f"Taxi Zones missing fields: {sorted(missing_zones)}")
    print("Source preflight: PASS")


    # One audit row per attempted source file; COPY INTO remains the idempotency authority.
    audit_table = f"{catalog}.{quality_schema}.bronze_file_runs"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {audit_table} (
          run_id STRING, processed_at TIMESTAMP, source STRING,
          source_file STRING, rows_inserted BIGINT, status STRING, error STRING
        ) USING DELTA
    """)
    run_id = str(uuid.uuid4())


    def record(source, filename, inserted, status, error=None):
        spark.createDataFrame(
            [(run_id, datetime.now(timezone.utc), source, filename, inserted, status, error)],
            schema="run_id STRING, processed_at TIMESTAMP, source STRING, "
                   "source_file STRING, rows_inserted BIGINT, status STRING, error STRING",
        ).write.mode("append").saveAsTable(audit_table)


    def load(source, filename, stage, parameters):
        try:
            result = run_file(Path(__file__).parent / (stage.removeprefix("./") + ".py"), spark, parameters, dbutils, display)
            if int(result.get("num_skipped_corrupt_files", 0)) != 0:
                raise ValueError(f"Corrupt file(s) skipped: {filename}")
            inserted = int(result["num_inserted_rows"])
            record(source, filename, inserted, "LOADED" if inserted else "SKIPPED")
            print(f"{source}: {filename}, inserted={inserted}")
        except Exception as exc:
            record(source, filename, None, "FAILED", str(exc)[:1000])
            raise


    for month in batch.taxi_months:
        filename = f"green_tripdata_{month}.parquet"
        load("green_taxi", filename, "./green_taxi_bronze", {
            "run_month": month,
            "source_dir": config["taxi_source_dir"],
            "catalog": catalog,
            "bronze_schema": bronze_schema,
        })
    for filename in batch.weather_files:
        load("weather", filename, "./weather_bronze", {
            "source_file": filename,
            "weather_source_dir": config["weather_source_dir"],
            "catalog": catalog,
            "bronze_schema": bronze_schema,
        })
    load("taxi_zones", batch.zone_file, "./taxi_zones_bronze", {
        "source_file": batch.zone_file,
        "zones_source_dir": config["zones_source_dir"],
        "catalog": catalog,
        "bronze_schema": bronze_schema,
    })
    print(f"Bronze audit run_id={run_id}; table={audit_table}")
    dbutils.jobs.taskValues.set(key="start_month", value=batch.start_month)
    dbutils.jobs.taskValues.set(key="end_month", value=batch.end_month)
    return {
        "start_month": batch.start_month,
        "end_month": batch.end_month,
        "run_id": run_id,
    }


if __name__ == "__main__":
    execute(run)
