"""Databricks Spark stage; callable directly from Python tests."""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


from pipeline.runtime import Parameters, execute

def run(spark, options=None, dbutils=None, display=None):
    params = Parameters(options)
    if display is None:
        display = lambda frame: frame.show(20, truncate=False)
    if dbutils is None:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)

    # Read-only notebook. Inspect actual filenames and fields before choosing the file to load; no assumed weather schema is written to the repo.


    import re

    params.register("weather_source_dir", "", "Weather directory")
    source_dir = params.get("weather_source_dir").strip().rstrip("/")
    if not re.fullmatch(r"/Volumes/[A-Za-z0-9_./-]+", source_dir):
        raise ValueError("Use a /Volumes/... directory in weather_source_dir (without dbfs:)")
    files = [item for item in dbutils.fs.ls(source_dir) if not item.isDir()]
    for item in files:
        print(item.name, item.size, "bytes")


    params.register("source_file", "", "Weather file to inspect")
    source_file = params.get("source_file").strip()
    if source_file:
        if not re.fullmatch(r"[A-Za-z0-9_.-]+\.(?:parquet|csv|json)", source_file):
            raise ValueError("Choose one .parquet, .csv or .json filename listed above")
        if source_file not in {item.name for item in files}:
            raise FileNotFoundError(f"{source_file} not found in {source_dir}")
        path = f"{source_dir}/{source_file}"
        if source_file.endswith(".csv"):
            sample = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
        elif source_file.endswith(".json"):
            sample = spark.read.option("multiLine", "true").json(path)
        else:
            sample = spark.read.parquet(path)
        print("Rows:", sample.count())
        sample.printSchema()
        display(sample.limit(5))
    else:
        print("Set source_file to one of the listed filenames, then rerun this cell.")


if __name__ == "__main__":
    execute(run)
