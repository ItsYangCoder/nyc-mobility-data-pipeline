"""Verify the executable Silver query without starting Databricks compute."""

from unittest.mock import Mock

import pytest

from silver_taxi import build_silver_taxi


@pytest.fixture
def spark():
    return Mock()


def build(spark, **overrides):
    args = dict(start_month="2026-03", end_month="2026-05", catalog="nyc_mobility",
                bronze_schema="nyc_bronze", silver_schema="nyc_silver")
    args.update(overrides)
    return build_silver_taxi(spark, **args)


def test_replaces_view_with_inclusive_month_window(spark):
    assert build(spark) == "nyc_mobility.nyc_silver.vw_green_taxi_clean"
    query = spark.sql.call_args.args[0]
    assert "FROM nyc_mobility.nyc_bronze.green_taxi_raw" in query
    assert "CREATE OR REPLACE VIEW nyc_mobility.nyc_silver.vw_green_taxi_clean" in query
    assert "TIMESTAMP_NTZ '2026-03-01 00:00:00'" in query
    assert "TIMESTAMP_NTZ '2026-06-01 00:00:00'" in query
    assert "lpep_dropoff_datetime >= lpep_pickup_datetime" in query
    assert "trip_distance = 0 AS is_zero_distance" in query


def test_month_and_schema_follow_execution_parameters(spark):
    assert build(spark, start_month="2027-12", end_month="2028-01",
                 catalog="demo", bronze_schema="raw", silver_schema="clean") == "demo.clean.vw_green_taxi_clean"
    query = spark.sql.call_args.args[0]
    assert "FROM demo.raw.green_taxi_raw" in query
    assert "TIMESTAMP_NTZ '2027-12-01 00:00:00'" in query
    assert "TIMESTAMP_NTZ '2028-02-01 00:00:00'" in query


@pytest.mark.parametrize("overrides", [
    {"start_month": "2026-13"},
    {"start_month": "2026-05", "end_month": "2026-03"},
    {"catalog": "db; DROP TABLE taxi"},
    {"silver_schema": "bad-name"},
])
def test_invalid_parameters_do_not_replace_view(spark, overrides):
    with pytest.raises(ValueError):
        build(spark, **overrides)
    spark.sql.assert_not_called()
