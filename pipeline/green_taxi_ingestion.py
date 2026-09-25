"""Databricks Spark stage; callable directly from Python tests."""

from runtime import Parameters, execute

def run(spark, options=None, dbutils=None, display=None):
    params = Parameters(options)
    if display is None:
        display = lambda frame: frame.show(20, truncate=False)
    if dbutils is None:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)

    # Read and inspect the source; this notebook does not write tables.


    from quality_rules import parse_month
    import re
    from pyspark.sql import functions as F

    params.register("run_month", "", "Run month (YYYY-MM)")
    params.register("source_dir", "", "Green Taxi volume directory")

    run_month = params.get("run_month").strip()
    source_dir = params.get("source_dir").strip().rstrip("/")
    parse_month(run_month)
    if not re.fullmatch(r"/Volumes/[A-Za-z0-9_./-]+", source_dir):
        raise ValueError("Set source_dir to the Green Taxi directory under /Volumes")

    file_path = f"{source_dir}/green_tripdata_{run_month}.parquet"
    print(f"Reading: {file_path}")


    taxi_df = spark.read.parquet(file_path)
    print("Source rows:", taxi_df.count())
    taxi_df.printSchema()
    display(taxi_df.limit(5))


    display(taxi_df.select(
        F.min("lpep_pickup_datetime").alias("earliest_pickup"),
        F.max("lpep_pickup_datetime").alias("latest_pickup"),
        F.sum(F.col("lpep_pickup_datetime").isNull().cast("long")).alias("missing_pickup"),
        F.sum((F.date_format("lpep_pickup_datetime", "yyyy-MM") != run_month).cast("long"))
            .alias("outside_run_month"),
    ))
    display(taxi_df.groupBy(
        F.date_format("lpep_pickup_datetime", "yyyy-MM").alias("pickup_month")
    ).count().orderBy("pickup_month"))


if __name__ == "__main__":
    execute(__file__, run)
