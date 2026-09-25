"""Databricks Spark stage; callable directly from Python tests."""

from runtime import Parameters, ensure_bronze_metadata_columns, execute

def run(spark, options=None, dbutils=None, display=None):
    params = Parameters(options)
    if display is None:
        display = lambda frame: frame.show(20, truncate=False)
    if dbutils is None:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)

    # The volume directory is a widget. COPY INTO loads only the CSV file and skips it on a rerun; the metadata JSON is excluded.


    import re

    params.register("zones_source_dir", "", "Taxi Zones directory")
    params.register("source_file", "", "Taxi Zones CSV filename")
    params.register("catalog", "nyc_mobility", "Catalog")
    params.register("bronze_schema", "nyc_bronze", "Bronze schema")

    source_dir = params.get("zones_source_dir").strip().rstrip("/")
    catalog = params.get("catalog").strip()
    bronze_schema = params.get("bronze_schema").strip()
    if not re.fullmatch(r"/Volumes/[A-Za-z0-9_./-]+", source_dir):
        raise ValueError("Use a /Volumes/... directory in zones_source_dir (without dbfs:)")
    for name in (catalog, bronze_schema):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise ValueError(f"Invalid catalog/schema: {name!r}")
    source_file = params.get("source_file").strip()
    if not re.fullmatch(r"[A-Za-z0-9_.-]+\.csv", source_file):
        raise ValueError("Set source_file to a CSV filename in the Taxi Zones directory")
    available = {item.name for item in dbutils.fs.ls(source_dir)}
    if source_file not in available:
        raise FileNotFoundError(f"{source_file} is missing in {source_dir}")
    bronze_table = f"{catalog}.{bronze_schema}.taxi_zones_raw"
    print(f"Source: {source_dir}/{source_file}; target: {bronze_table}")


    spark.sql(f"CREATE TABLE IF NOT EXISTS {bronze_table}")
    ensure_bronze_metadata_columns(spark, bronze_table)
    result = spark.sql(f"""
        COPY INTO {bronze_table}
        FROM (
          SELECT *, _metadata.file_path AS source_file_path,
                 current_timestamp() AS source_ingested_at
          FROM '{source_dir}'
        )
        FILEFORMAT = CSV
        FILES = ('{source_file}')
        FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
        COPY_OPTIONS ('mergeSchema' = 'true')
    """)
    copy_result = result.first().asDict()
    display(spark.createDataFrame([copy_result]))
    display(spark.sql(f"SELECT COUNT(*) AS bronze_rows FROM {bronze_table}"))
    import json
    return copy_result


if __name__ == "__main__":
    execute(run)
