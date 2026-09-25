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
