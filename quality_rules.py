"""Shared month-window and Gold distance rules used by the notebooks."""

from __future__ import annotations

from datetime import date, datetime
import math
import re


def parse_month(value: str) -> date:
    """Return the first day of a strictly formatted YYYY-MM month."""
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}", value.strip()):
        raise ValueError("Month must use YYYY-MM")
    try:
        return datetime.strptime(value.strip(), "%Y-%m").date()
    except ValueError as exc:
        raise ValueError(f"Invalid month: {value!r}") from exc


def month_window(start_month: str, end_month: str) -> tuple[date, date]:
    """Return [start, end) for an inclusive pair of calendar months."""
    start = parse_month(start_month)
    last = parse_month(end_month)
    if last < start:
        raise ValueError("end_month must be >= start_month")
    end = date(last.year + (last.month == 12), last.month % 12 + 1, 1)
    return start, end


def in_window(value: date | datetime | None, start: date, end: date) -> bool:
    """Check membership in a half-open calendar date window."""
    if value is None:
        return False
    if isinstance(value, datetime):
        value = value.date()
    if not isinstance(value, date):
        raise TypeError("value must be a date or datetime")
    return start <= value < end


def distance_limit(value: str | float) -> float:
    """Validate a configurable, finite positive distance limit in miles."""
    if isinstance(value, bool):
        raise ValueError("Distance limit must be a positive finite number")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("Distance limit must be a positive finite number") from exc
    if not math.isfinite(result) or result <= 0:
        raise ValueError("Distance limit must be a positive finite number")
    return result


def distance_projection_sql(limit_miles: str | float) -> str:
    """Gold fact columns: preserve the source distance and expose the valid value."""
    limit = distance_limit(limit_miles)
    return (
        f"(trip_distance < 0 OR trip_distance > {limit!r}) AS is_distance_outlier,\n"
        "           CASE WHEN trip_distance BETWEEN 0 AND "
        f"{limit!r} THEN trip_distance END AS valid_trip_distance"
    )


def distance_summary_sql() -> str:
    """Gold daily metrics; counts include trips with flagged distances."""
    return (
        "COUNT(*) AS trip_count,\n"
        "           COUNT_IF(is_distance_outlier) AS flagged_distance_trips,\n"
        "           SUM(valid_trip_distance) AS total_distance"
    )
