"""Validate and plan complete monthly Bronze batches from volume listings."""

from dataclasses import dataclass
from datetime import date
import calendar
import re

from quality_rules import parse_month

_TAXI = re.compile(r"green_tripdata_([0-9]{4}-[0-9]{2})\.parquet\Z")
_WEATHER = re.compile(
    r"weather_([0-9]{4}-[0-9]{2}-[0-9]{2})_([0-9]{4}-[0-9]{2}-[0-9]{2})\.(json|csv|parquet)\Z"
)


@dataclass(frozen=True)
class SourceBatch:
    start_month: str
    end_month: str
    taxi_months: tuple[str, ...]
    weather_files: tuple[str, ...]
    zone_file: str


def weather_hour_bounds(filename):
    """Return expected local first and last hour from a validated weather filename."""
    match = _WEATHER.fullmatch(filename)
    if not match:
        raise ValueError(f"Unexpected Weather source filename: {filename}")
    first_day, last_day, _format = match.groups()
    return f"{first_day}T00:00", f"{last_day}T23:00"


def validate_weather_hour_stats(stats, filename):
    """Require exactly one observation for every local hour in a source month."""
    first, last = weather_hour_bounds(filename)
    expected = (date.fromisoformat(last[:10]) - date.fromisoformat(first[:10])).days * 24 + 24
    if (
        stats["rows"] != expected
        or stats["unique"] != expected
        or stats["first"] != first
        or stats["last"] != last
    ):
        details = {key: stats[key] for key in ("rows", "unique", "first", "last")}
        raise ValueError(f"Weather hourly coverage/uniqueness failed: {filename}: {details}")


def _files(listing, prefix, metadata_suffix=None):
    """Validate matching file names and sizes before any table write."""
    result = []
    for item in listing:
        name = item.name
        if metadata_suffix and name.endswith(metadata_suffix):
            continue
        if not name.startswith(prefix):
            continue
        if name.endswith("/") or item.size <= 0:
            raise ValueError(f"Missing or empty source file: {name}")
        result.append(name)
    return sorted(result)


def plan_batch(taxi_listing, weather_listing, zones_listing, zone_file="taxi_zone_lookup.csv"):
    taxi_names = _files(taxi_listing, "green_tripdata_")
    weather_names = _files(weather_listing, "weather_", "_metadata.json")
    if not taxi_names or not weather_names:
        raise ValueError("Both Taxi and Weather monthly source files are required")

    taxi_months = {}
    for name in taxi_names:
        match = _TAXI.fullmatch(name)
        if not match:
            raise ValueError(f"Unexpected Taxi source filename: {name}")
        month = match.group(1)
        parse_month(month)
        if month in taxi_months:
            raise ValueError(f"Duplicate Taxi month: {month}")
        taxi_months[month] = name

    weather_months = {}
    for name in weather_names:
        match = _WEATHER.fullmatch(name)
        if not match:
            raise ValueError(f"Unexpected Weather source filename: {name}")
        first, last, _format = match.groups()
        try:
            start_date, last_date = date.fromisoformat(first), date.fromisoformat(last)
        except ValueError as exc:
            raise ValueError(f"Invalid Weather dates: {name}") from exc
        month = first[:7]
        end_day = calendar.monthrange(start_date.year, start_date.month)[1]
        if start_date.day != 1 or last_date != date(start_date.year, start_date.month, end_day):
            raise ValueError(f"Weather file must cover one complete month: {name}")
        if month in weather_months:
            raise ValueError(f"Duplicate Weather month: {month}")
        weather_months[month] = name

    months = sorted(taxi_months)
    if set(taxi_months) != set(weather_months):
        raise ValueError(
            f"Unmatched Taxi/Weather months: Taxi={months}, Weather={sorted(weather_months)}"
        )
    for previous, current in zip(months, months[1:]):
        year, number = map(int, previous.split("-"))
        following = f"{year + (number == 12):04d}-{number % 12 + 1:02d}"
        if current != following:
            raise ValueError(f"Missing calendar month between {previous} and {current}")
    if zone_file not in {
        file.name for file in zones_listing if not file.name.endswith("/") and file.size > 0
    }:
        raise ValueError(f"Missing or empty Taxi Zones lookup: {zone_file}")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+\.csv", zone_file):
        raise ValueError("Zone lookup must be a CSV filename")
    return SourceBatch(
        start_month=months[0],
        end_month=months[-1],
        taxi_months=tuple(months),
        weather_files=tuple(weather_months[month] for month in months),
        zone_file=zone_file,
    )
