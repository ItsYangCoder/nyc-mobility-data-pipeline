"""Databricks Spark stage; callable directly from Python tests."""

from runtime import Parameters, check_taxi_month_source, ensure_bronze_metadata_columns, execute

def run(spark, options=None, dbutils=None, display=None):
    params = Parameters(options)
    if display is None:
        display = lambda frame: frame.show(20, truncate=False)
    if dbutils is None:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)

    # Pass the source parameters before running. COPY INTO skips a file already loaded into the same table.


    from quality_rules import parse_month
    import re

    params.register("run_month", "", "Run month (YYYY-MM)")
    params.register("source_dir", "", "Green Taxi volume directory")
    params.register("catalog", "nyc_mobility", "Target catalog")
    params.register("bronze_schema", "nyc_bronze", "Bronze schema")

    run_month = params.get("run_month").strip()
    source_dir = params.get("source_dir").strip().rstrip("/")
    catalog = params.get("catalog").strip()
    bronze_schema = params.get("bronze_schema").strip()
    parse_month(run_month)
    if not re.fullmatch(r"/Volumes/[A-Za-z0-9_./-]+", source_dir):
        raise ValueError("Set source_dir to the Green Taxi directory under /Volumes")
    for identifier in (catalog, bronze_schema):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
            raise ValueError(f"Invalid catalog or schema: {identifier!r}")

    bronze_table = f"{catalog}.{bronze_schema}.green_taxi_raw"
    file_name = f"green_tripdata_{run_month}.parquet"
    print(f"Loading {source_dir}/{file_name} into {bronze_table}")


    # Create an empty, schemaless Delta table so COPY INTO can infer the Parquet schema.
    spark.sql(f"CREATE TABLE IF NOT EXISTS {bronze_table}")
    ensure_bronze_metadata_columns(spark, bronze_table)
    check_taxi_month_source(
        spark, bronze_table, run_month, f"dbfs:{source_dir}/{file_name}"
    )
    # The source timestamps use TIMESTAMP_NTZ. Enable this feature BEFORE loading.
    spark.sql(f"""
        ALTER TABLE {bronze_table}
        SET TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')
    """)

    result = spark.sql(f"""
        COPY INTO {bronze_table}
        FROM (
          SELECT *, _metadata.file_path AS source_file_path,
                 current_timestamp() AS source_ingested_at
          FROM '{source_dir}'
        )
        FILEFORMAT = PARQUET
        FILES = ('{file_name}')
        COPY_OPTIONS ('mergeSchema' = 'true')
    """)
    copy_result = result.first().asDict()
    display(spark.createDataFrame([copy_result]))


    display(spark.sql(f"SELECT COUNT(*) AS bronze_rows FROM {bronze_table}"))
    import json
    return copy_result


if __name__ == "__main__":
    execute(run)
