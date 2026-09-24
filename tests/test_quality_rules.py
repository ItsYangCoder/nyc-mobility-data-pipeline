from datetime import date, datetime

import duckdb
import pytest

from quality_rules import (
    distance_limit,
    distance_projection_sql,
    distance_summary_sql,
    in_window,
    month_window,
    parse_month,
)


@pytest.fixture
def trip_distances():
    # A valid zero, the exact boundary, one high outlier, a negative, and missing data.
    return [(0.0,), (100.0,), (100.01,), (-1.0,), (None,)]


@pytest.mark.parametrize(
    ("text", "expected"),
    [("2026-03", date(2026, 3, 1)), ("2026-12", date(2026, 12, 1)),
     ("2024-02", date(2024, 2, 1))],
)
def test_parse_month(text, expected):
    assert parse_month(text) == expected


@pytest.mark.parametrize("text", ["2026-00", "2026-13", "2026-3", "2026-02-01", "", "2026/03"])
def test_reject_invalid_month(text):
    with pytest.raises(ValueError):
        parse_month(text)


def test_window_is_inclusive_of_selected_months_and_exclusive_of_next_month():
    start, end = month_window("2026-03", "2026-05")
    assert (start, end) == (date(2026, 3, 1), date(2026, 6, 1))
    assert in_window(datetime(2026, 3, 1, 0, 0), start, end)
    assert in_window(datetime(2026, 5, 31, 23, 59, 59), start, end)
    assert not in_window(datetime(2026, 2, 28, 23, 59), start, end)
    assert not in_window(datetime(2026, 6, 1, 0, 0), start, end)
    assert not in_window(None, start, end)


def test_december_rollover_and_reversed_window():
    assert month_window("2026-12", "2026-12") == (date(2026, 12, 1), date(2027, 1, 1))
    with pytest.raises(ValueError, match="end_month"):
        month_window("2026-05", "2026-03")


@pytest.mark.parametrize("limit", ["0", "-1", "nan", "inf", "-inf", "bad", None, True])
def test_reject_bad_distance_limit(limit):
    with pytest.raises(ValueError):
        distance_limit(limit)


def test_distance_limit_accepts_configured_value():
    assert distance_limit("100") == 100.0
    assert distance_limit(35.5) == 35.5


def test_gold_distance_metrics_preserve_trip_grain(trip_distances):
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE trips (trip_distance DOUBLE)")
        con.executemany("INSERT INTO trips VALUES (?)", trip_distances)
        row = con.execute(
            f"WITH fact AS (SELECT trip_distance, {distance_projection_sql(100)} FROM trips) "
            f"SELECT {distance_summary_sql()}, COUNT(valid_trip_distance) AS valid_trips, "
            "ROUND(AVG(valid_trip_distance), 2) AS avg_distance, "
            "COUNT_IF(is_distance_outlier IS NULL) AS missing_flags FROM fact"
        ).fetchone()
        # All five source trips remain. The 100-mile boundary stays valid; high and
        # negative values are flagged; missing distance has no usable metric.
        assert row == (5, 2, 100.0, 2, 50.0, 1)
        distance_rows = con.execute(
            f"SELECT trip_distance, is_distance_outlier, valid_trip_distance "
            f"FROM (SELECT trip_distance, {distance_projection_sql(100)} FROM trips) "
            "ORDER BY trip_distance NULLS LAST"
        ).fetchall()
        assert distance_rows == [
            (-1.0, True, None), (0.0, False, 0.0),
            (100.0, False, 100.0), (100.01, True, None),
            (None, None, None),
        ]
    finally:
        con.close()


def test_distance_limit_changes_classification_without_code_changes(trip_distances):
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE trips (trip_distance DOUBLE)")
        con.executemany("INSERT INTO trips VALUES (?)", trip_distances)
        result = con.execute(
            f"WITH fact AS (SELECT {distance_projection_sql('50')} FROM trips) "
            f"SELECT {distance_summary_sql()} FROM fact"
        ).fetchone()
        assert result == (5, 3, 0.0)
    finally:
        con.close()
