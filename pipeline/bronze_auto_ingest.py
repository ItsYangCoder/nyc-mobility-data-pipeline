"""Discover, validate, and load all Bronze source files with an audit record."""

from pathlib import Path
import re
import uuid
from datetime import datetime, timezone

from bronze_preflight import validate_sources
from runtime import Parameters, execute
from runtime import run_file
from source_discovery import plan_batch


def run(spark, options=None, dbutils=None, display=None):
    params = Parameters(options)
    if display is None:
        display = lambda frame: frame.show(20, truncate=False)
    if dbutils is None:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)

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
    validate_sources(spark, batch, config)

    # Record every attempted file; COPY INTO decides whether it was already loaded.
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
            result = run_file(Path(__file__).parent / f"{stage}.py", spark, parameters, dbutils, display)
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
        load("green_taxi", filename, "green_taxi_bronze", {
            "run_month": month,
            "source_dir": config["taxi_source_dir"],
            "catalog": catalog,
            "bronze_schema": bronze_schema,
        })
    for filename in batch.weather_files:
        load("weather", filename, "weather_bronze", {
            "source_file": filename,
            "weather_source_dir": config["weather_source_dir"],
            "catalog": catalog,
            "bronze_schema": bronze_schema,
        })
    load("taxi_zones", batch.zone_file, "taxi_zones_bronze", {
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
