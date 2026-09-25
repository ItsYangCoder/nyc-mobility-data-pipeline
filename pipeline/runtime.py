"""Small runtime boundary for local tests and Databricks Python script tasks."""

import argparse
import importlib.util
from pathlib import Path
import sys


class Parameters:
    def __init__(self, overrides=None):
        self.overrides = overrides or {}
        self.defaults = {}

    def register(self, name, value, description=None):
        self.defaults[name] = value

    def get(self, name):
        if name not in self.defaults:
            raise ValueError(f"Unknown parameter: {name}")
        return self.overrides.get(name, self.defaults[name])


def parse_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--param", action="append", nargs=2, metavar=("NAME", "VALUE"), default=[])
    args = parser.parse_args(argv)
    return dict(args.param)


def ensure_bronze_metadata_columns(spark, table):
    """Prepare existing Bronze tables for metadata on future file loads."""
    fields = spark.table(table).schema.fields
    if not fields:  # A new schemaless table lets COPY INTO infer every column.
        return
    present = {field.name.lower() for field in fields}
    missing = [definition for name, definition in (
        ("source_file_path", "source_file_path STRING"),
        ("source_ingested_at", "source_ingested_at TIMESTAMP"),
    ) if name not in present]
    if missing:
        spark.sql(f"ALTER TABLE {table} ADD COLUMNS ({', '.join(missing)})")


def check_taxi_month_source(spark, table, month, source_path):
    """Fail closed when a taxi month already contains rows from another file path."""
    if "lpep_pickup_datetime" not in {f.name.lower() for f in spark.table(table).schema.fields}:
        return  # New schemaless table.
    result = spark.sql(f"""
        SELECT COUNT(*) AS existing_rows,
               COUNT_IF(source_file_path IS NULL OR source_file_path <> '{source_path}')
                   AS other_source_rows
        FROM {table}
        WHERE lpep_pickup_datetime >= DATE '{month}-01'
          AND lpep_pickup_datetime < ADD_MONTHS(DATE '{month}-01', 1)
    """).first()
    if result.other_source_rows:
        raise ValueError(
            f"Taxi month {month} already has {result.other_source_rows} rows with unknown or "
            "different source paths; reconcile before loading another path"
        )


def check_weather_file_source(spark, table, source_file, source_path):
    """Block a second weather document covering dates already in Bronze."""
    import re

    match = re.fullmatch(r"weather_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})\.json", source_file)
    if not match:
        raise ValueError("Weather source filename must specify its date range")
    start, end = match.groups()
    if start > end:
        raise ValueError("Weather source date range is reversed")
    if "hourly" not in {f.name.lower() for f in spark.table(table).schema.fields}:
        return
    result = spark.sql(f"""
        SELECT COUNT_IF(source_file_path IS NULL OR source_file_path <> '{source_path}')
                   AS other_source_rows
        FROM {table}
        WHERE TO_DATE(array_min(hourly.time)) <= DATE '{end}'
          AND TO_DATE(array_max(hourly.time)) >= DATE '{start}'
    """).first()
    if result.other_source_rows:
        raise ValueError("Weather date range already has rows with unknown or different "
                         "source paths; reconcile before loading another path")


def check_zones_source(spark, table, source_path):
    """Block loading another zone lookup over an existing unknown source."""
    if "locationid" not in {f.name.lower() for f in spark.table(table).schema.fields}:
        return
    result = spark.sql(f"""
        SELECT COUNT_IF(source_file_path IS NULL OR source_file_path <> '{source_path}')
                   AS other_source_rows
        FROM {table}
    """).first()
    if result.other_source_rows:
        raise ValueError("Taxi zones already have rows with unknown or different "
                         "source paths; reconcile before loading another path")


def run_file(path, spark, options, dbutils=None, display=None):
    path = Path(path).resolve()
    if not path.is_file() or path.suffix != ".py":
        raise ValueError(f"Invalid stage path: {path}")
    spec = importlib.util.spec_from_file_location(f"stage_{path.stem}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.run(spark, options, dbutils, display)


def execute(run):
    options = parse_args()
    root = Path(__file__).resolve().parent.parent
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    from pyspark.dbutils import DBUtils

    result = run(spark, options, DBUtils(spark), lambda frame: frame.show(20, truncate=False))
    if result is not None:
        print(result)
