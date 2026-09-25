"""Exercise Bronze loaders with a stateful COPY INTO stand-in."""

import re
from pathlib import Path
from types import SimpleNamespace as Row

import pytest

from pipeline.runtime import run_file


STAGES = {
    "green_taxi_bronze": ("green_taxi_raw", {"run_month": "2026-03", "source_dir": "/Volumes/c/s/taxi"}, "green_tripdata_2026-03.parquet"),
    "weather_bronze": ("weather_raw", {"weather_source_dir": "/Volumes/c/s/weather", "source_file": "weather_2026-03-01_2026-03-31.json"}, "weather_2026-03-01_2026-03-31.json"),
    "taxi_zones_bronze": ("taxi_zones_raw", {"zones_source_dir": "/Volumes/c/s/zones", "source_file": "taxi_zone_lookup.csv"}, "taxi_zone_lookup.csv"),
}


class BronzeSpark:
    def __init__(self):
        self.rows = []
        self.loaded_files = set()
        self.copy_attempts = 0

    def table(self, name):
        fields = [] if not self.rows else ["lpep_pickup_datetime", "hourly", "LocationID",
                                           "source_file_path", "source_ingested_at"]
        return Row(schema=Row(fields=[Row(name=name) for name in fields]))

    def createDataFrame(self, rows):
        return rows

    def sql(self, query):
        if "AS other_source_rows" in query:
            expected = re.search(r"source_file_path <> '([^']+)'", query).group(1)
            conflicts = sum(row["path"] != expected for row in self.rows)
            return Row(first=lambda: Row(other_source_rows=conflicts, existing_rows=len(self.rows)))
        if query.strip().startswith("COPY INTO"):
            self.copy_attempts += 1
            directory = re.search(r"FROM '([^']+)'", query).group(1)
            filename = re.search(r"FILES = \('([^']+)'\)", query).group(1)
            path = f"dbfs:{directory}/{filename}"
            inserted = int(path not in self.loaded_files)
            if inserted:
                self.rows.append({"path": path, "ingested_at": "first-load-time"})
                self.loaded_files.add(path)
            return Row(first=lambda: Row(asDict=lambda: {"num_inserted_rows": inserted}))
        if "AS bronze_rows" in query:
            return Row()
        return Row()


@pytest.mark.parametrize("stage", STAGES)
def test_bronze_retry_preserves_rows_and_ingestion_time(stage, monkeypatch):
    pipeline_dir = Path(__file__).resolve().parents[1] / "pipeline"
    monkeypatch.syspath_prepend(str(pipeline_dir))
    _, options, filename = STAGES[stage]
    spark = BronzeSpark()
    fs = Row(ls=lambda path: [Row(name=filename)])
    dbutils = Row(fs=fs)
    run = lambda: run_file(pipeline_dir / "01_bronze" / f"{stage}.py", spark, options, dbutils, lambda _: None)

    assert run()["num_inserted_rows"] == 1
    original = list(spark.rows)
    assert run()["num_inserted_rows"] == 0
    assert spark.rows == original
    assert spark.copy_attempts == 2


@pytest.mark.parametrize("stage", STAGES)
@pytest.mark.parametrize("prior_path", [None, "dbfs:/Volumes/another/source/file"])
def test_bronze_conflicting_previous_path_blocks_copy(stage, prior_path, monkeypatch):
    pipeline_dir = Path(__file__).resolve().parents[1] / "pipeline"
    monkeypatch.syspath_prepend(str(pipeline_dir))
    _, options, filename = STAGES[stage]
    spark = BronzeSpark()
    spark.rows.append({"path": prior_path, "ingested_at": None})
    dbutils = Row(fs=Row(ls=lambda path: [Row(name=filename)]))

    with pytest.raises(ValueError, match="unknown or different source paths"):
        run_file(pipeline_dir / "01_bronze" / f"{stage}.py", spark, options, dbutils, lambda _: None)
    assert spark.copy_attempts == 0
