"""Validate landing files before any Bronze table write."""

from source_discovery import validate_weather_hour_stats


def validate_sources(spark, batch, config):
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, StructType

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
