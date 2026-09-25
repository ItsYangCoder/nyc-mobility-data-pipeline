"""Databricks Spark stage; callable directly from Python tests."""

from runtime import Parameters, execute

def run(spark, options=None, dbutils=None, display=None):
    params = Parameters(options)
    if display is None:
        display = lambda frame: frame.show(20, truncate=False)
    if dbutils is None:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)

    # Read-only source profiling. This stage does not create or change a table; the Bronze stage performs the actual load.


    import re
    from pyspark.sql import functions as F

    params.register("zones_source_dir", "", "Taxi Zones directory")
    params.register("source_file", "", "Taxi Zones CSV filename")
    source_dir = params.get("zones_source_dir").strip().rstrip("/")
    source_file = params.get("source_file").strip()
    if not re.fullmatch(r"/Volumes/[A-Za-z0-9_./-]+", source_dir):
        raise ValueError("Use a /Volumes/... directory in zones_source_dir (without dbfs:)")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+\.csv", source_file):
        raise ValueError("Set source_file to a CSV filename in the Taxi Zones directory")
    if source_file not in {item.name for item in dbutils.fs.ls(source_dir)}:
        raise FileNotFoundError(f"{source_file} not found in {source_dir}")
    path = f"{source_dir}/{source_file}"
    print("Inspecting:", path)


    zones_df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    print("Source rows:", zones_df.count())
    zones_df.printSchema()
    display(zones_df.limit(10))


    required = {"LocationID", "Borough", "Zone", "service_zone"}
    if not required.issubset(zones_df.columns):
        raise ValueError(f"Missing Taxi Zones columns: {sorted(required - set(zones_df.columns))}")
    display(zones_df.select(
        F.count("*").alias("total_rows"),
        F.countDistinct("LocationID").alias("unique_location_ids"),
        F.sum(F.col("LocationID").isNull().cast("long")).alias("missing_location_id"),
        F.sum((F.col("Zone").isNull() | (F.trim("Zone") == "")).cast("long")).alias("missing_zone"),
    ))
    display(zones_df.groupBy("LocationID").count().filter(F.col("count") > 1))


if __name__ == "__main__":
    execute(__file__, run)
