from pathlib import Path

import pytest

from pipeline.runtime import Parameters, parse_args, run_file


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
