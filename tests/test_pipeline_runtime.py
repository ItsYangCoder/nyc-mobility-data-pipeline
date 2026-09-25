from pathlib import Path

import pytest

from pipeline.runtime import (Parameters, check_taxi_month_source, check_weather_file_source,
                              check_zones_source, ensure_bronze_metadata_columns, parse_args, run_file)


@pytest.mark.parametrize("source,should_fail", [(None, True), ("dbfs:/other.json", True),
                                                  ("dbfs:/same.json", False)])
def test_weather_source_guard(source, should_fail):
    from types import SimpleNamespace

    class FakeSpark:
        def table(self, table):
            return SimpleNamespace(schema=SimpleNamespace(fields=[SimpleNamespace(name="hourly")]))

        def sql(self, query):
            assert "2026-03-01" in query and "2026-03-31" in query
            assert "array_min(hourly.time)" in query and "array_max(hourly.time)" in query
            assert "SUBSTRING(CAST(array_min(hourly.time) AS STRING), 1, 10)" in query
            assert "array_min(hourly.time) IS NULL" in query
            return SimpleNamespace(first=lambda: SimpleNamespace(
                other_source_rows=int(source != "dbfs:/same.json")))

    check = lambda: check_weather_file_source(
        FakeSpark(), "c.s.weather_raw", "weather_2026-03-01_2026-03-31.json", "dbfs:/same.json")
    if should_fail:
        with pytest.raises(ValueError, match="unknown or different"):
            check()
    else:
        check()


def test_zones_source_guard_blocks_untracked_rows():
    from types import SimpleNamespace

    class FakeSpark:
        def table(self, table):
            return SimpleNamespace(schema=SimpleNamespace(fields=[SimpleNamespace(name="LocationID")]))

        def sql(self, query):
            assert "source_file_path IS NULL" in query
            return SimpleNamespace(first=lambda: SimpleNamespace(other_source_rows=265))

    with pytest.raises(ValueError, match="unknown or different"):
        check_zones_source(FakeSpark(), "c.s.taxi_zones_raw", "dbfs:/same.csv")


@pytest.mark.parametrize("source,should_fail", [(None, True), ("dbfs:/different/file.parquet", True),
                                                   ("dbfs:/same/file.parquet", False)])
def test_taxi_month_rejects_different_or_unknown_source(source, should_fail):
    from types import SimpleNamespace

    class FakeSpark:
        def table(self, table):
            return SimpleNamespace(schema=SimpleNamespace(fields=[
                SimpleNamespace(name="lpep_pickup_datetime"),
                SimpleNamespace(name="source_file_path"),
            ]))

        def sql(self, query):
            assert "2026-03-01" in query
            assert "dbfs:/same/file.parquet" in query
            return SimpleNamespace(first=lambda: SimpleNamespace(
                existing_rows=1, other_source_rows=int(source != "dbfs:/same/file.parquet")
            ))

    if should_fail:
        with pytest.raises(ValueError, match="unknown or different source paths"):
            check_taxi_month_source(FakeSpark(), "c.s.t", "2026-03", "dbfs:/same/file.parquet")
    else:
        check_taxi_month_source(FakeSpark(), "c.s.t", "2026-03", "dbfs:/same/file.parquet")


@pytest.mark.parametrize("present,expected", [
    ([], None),
    (["trip_distance"], "source_file_path STRING, source_ingested_at TIMESTAMP"),
    (["trip_distance", "source_file_path"], "source_ingested_at TIMESTAMP"),
    (["trip_distance", "source_file_path", "source_ingested_at"], None),
])
def test_existing_bronze_metadata_schema(present, expected):
    from types import SimpleNamespace

    class FakeSpark:
        def __init__(self):
            self.queries = []

        def table(self, table):
            return SimpleNamespace(schema=SimpleNamespace(fields=[SimpleNamespace(name=name) for name in present]))

        def sql(self, query):
            self.queries.append(query)

    spark = FakeSpark()
    ensure_bronze_metadata_columns(spark, "test_catalog.test_schema.test_table")
    assert spark.queries == ([] if expected is None else [
        f"ALTER TABLE test_catalog.test_schema.test_table ADD COLUMNS ({expected})"
    ])


def test_dynamic_parameters_override_defaults():
    params = Parameters(parse_args(["--param", "start_month", "2027-01"]))
    params.register("start_month", "")
    assert params.get("start_month") == "2027-01"


def test_unregistered_parameter_is_rejected():
    with pytest.raises(ValueError, match="Unknown parameter"):
        Parameters().get("missing")


def test_run_file_invokes_plain_python_stage(tmp_path):
    script = tmp_path / "stage.py"
    script.write_text("def run(spark, options, dbutils, display):\n    return (spark, options, dbutils)\n")
    spark, dbutils = object(), object()
    assert run_file(script, spark, {"month": "2027-01"}, dbutils) == (
        spark, {"month": "2027-01"}, dbutils
    )


def test_run_file_rejects_non_python_path(tmp_path):
    with pytest.raises(ValueError, match="Invalid stage path"):
        run_file(tmp_path / "stage.ipynb", None, {})


@pytest.mark.parametrize("stage", ["01_taxi_demand", "02_weather_comparison", "03_zone_patterns"])
def test_analytics_queries_use_configured_catalog_and_schema(stage, monkeypatch):
    import sys

    pipeline_dir = Path(__file__).resolve().parents[1] / "pipeline"
    monkeypatch.syspath_prepend(str(pipeline_dir))

    class FakeSpark:
        def __init__(self):
            self.queries = []

        def sql(self, query):
            self.queries.append(query)
            return object()

    spark = FakeSpark()
    run_file(pipeline_dir / f"{stage}.py", spark,
             {"catalog": "test_catalog", "gold_schema": "test_gold"},
             dbutils=object(), display=lambda frame: None)
    assert spark.queries
    assert all("test_catalog.test_gold." in query for query in spark.queries)
    assert all("nyc_mobility.nyc_gold." not in query for query in spark.queries)
def test_script_entrypoints_work_without_notebook_file_global():
    """Databricks can launch a workspace script without defining __file__."""
    import ast
    from pathlib import Path

    for path in (Path(__file__).resolve().parents[1] / "pipeline").glob("*.py"):
        tree = ast.parse(path.read_text())
        entrypoints = [node for node in tree.body if isinstance(node, ast.If)
                       and ast.unparse(node.test) == "__name__ == '__main__'"]
        for entrypoint in entrypoints:
            calls = []
            namespace = {"__name__": "__main__", "run": object(),
                         "execute": lambda fn: calls.append(fn)}
            exec(compile(ast.Module(body=[entrypoint], type_ignores=[]), str(path), "exec"), namespace)
            assert calls == [namespace["run"]], path
