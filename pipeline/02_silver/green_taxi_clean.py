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

    # The date window comes from parameters. Run this stage after Bronze; rerunning replaces the view with the selected window. Zero-distance rides are retained and flagged.


    from silver_taxi import build_silver_taxi

    params.register("start_month", "", "First month (YYYY-MM)")
    params.register("end_month", "", "Last month (YYYY-MM, inclusive)")
    params.register("catalog", "nyc_mobility", "Target catalog")
    params.register("bronze_schema", "nyc_bronze", "Bronze schema")
    params.register("silver_schema", "nyc_silver", "Silver schema")

    silver_view = build_silver_taxi(
        spark,
        start_month=params.get("start_month"),
        end_month=params.get("end_month"),
        catalog=params.get("catalog").strip(),
        bronze_schema=params.get("bronze_schema").strip(),
        silver_schema=params.get("silver_schema").strip(),
    )
    print(f"Created {silver_view}")


    display(spark.sql(f"""
        SELECT COUNT(*) AS silver_rows,
               COUNT_IF(is_zero_distance) AS zero_distance_rows
        FROM {silver_view}
    """))


if __name__ == "__main__":
    execute(run)
