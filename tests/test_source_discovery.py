from dataclasses import dataclass

import pytest

from source_discovery import plan_batch


@dataclass(frozen=True)
class File:
    name: str
    size: int = 100


@pytest.fixture
def batch_files():
    taxi = [File(f"green_tripdata_2026-{month:02d}.parquet") for month in (3, 4, 5)]
    weather = [
        File("weather_2026-03-01_2026-03-31.json"),
        File("weather_2026-04-01_2026-04-30.json"),
        File("weather_2026-05-01_2026-05-31.json"),
        File("weather_2026-05-01_2026-05-31_metadata.json"),
    ]
    zones = [File("taxi_zone_lookup.csv"), File("taxi_zone_lookup_metadata.json")]
    return taxi, weather, zones


def test_discovers_complete_months_and_ignores_metadata(batch_files):
    batch = plan_batch(*batch_files)
    assert batch.start_month == "2026-03"
    assert batch.end_month == "2026-05"
    assert batch.taxi_months == ("2026-03", "2026-04", "2026-05")
    assert len(batch.weather_files) == 3
    assert batch.zone_file == "taxi_zone_lookup.csv"


def test_new_month_is_discovered_without_changing_code(batch_files):
    taxi, weather, zones = batch_files
    taxi.append(File("green_tripdata_2026-06.parquet"))
    weather.append(File("weather_2026-06-01_2026-06-30.json"))
    assert plan_batch(taxi, weather, zones).end_month == "2026-06"


@pytest.mark.parametrize(
    ("taxi_name", "weather_name"),
    [
        ("green_tripdata_2026-12.parquet", "weather_2026-12-01_2026-12-31.json"),
        ("green_tripdata_2027-01.parquet", "weather_2027-01-01_2027-01-31.parquet"),
        ("green_tripdata_2024-02.parquet", "weather_2024-02-01_2024-02-29.csv"),
    ],
)
def test_single_month_year_end_and_leap_day(taxi_name, weather_name):
    batch = plan_batch([File(taxi_name)], [File(weather_name)], [File("taxi_zone_lookup.csv")])
    assert len(batch.taxi_months) == 1


@pytest.mark.parametrize(
    ("bad_weather", "match"),
    [
        ("weather_2026-03-02_2026-03-31.json", "complete month"),
        ("weather_2026-03-01_2026-04-30.json", "complete month"),
        ("weather_2026-02-01_2026-02-30.json", "Invalid Weather dates"),
        ("weather_2026-03-01_2026-03-31.json.bak", "Unexpected Weather"),
    ],
)
def test_reject_bad_weather_filename(batch_files, bad_weather, match):
    taxi, weather, zones = batch_files
    weather[0] = File(bad_weather)
    with pytest.raises(ValueError, match=match):
        plan_batch(taxi, weather, zones)


def test_reject_missing_month_before_loading(batch_files):
    taxi, weather, zones = batch_files
    taxi.pop(1)
    weather.pop(1)
    with pytest.raises(ValueError, match="Missing calendar month"):
        plan_batch(taxi, weather, zones)


def test_reject_unmatched_months_before_loading(batch_files):
    taxi, weather, zones = batch_files
    weather.pop(1)
    with pytest.raises(ValueError, match="Unmatched"):
        plan_batch(taxi, weather, zones)


def test_reject_duplicate_weather_month(batch_files):
    taxi, weather, zones = batch_files
    weather.append(File("weather_2026-03-01_2026-03-31.parquet"))
    with pytest.raises(ValueError, match="Duplicate Weather"):
        plan_batch(taxi, weather, zones)


def test_reject_bad_taxi_file_and_empty_zone(batch_files):
    taxi, weather, zones = batch_files
    taxi[0] = File("green_tripdata_2026-3.parquet")
    with pytest.raises(ValueError, match="Unexpected Taxi"):
        plan_batch(taxi, weather, zones)
    taxi[0] = File("green_tripdata_2026-03.parquet")
    with pytest.raises(ValueError, match="empty Taxi Zones"):
        plan_batch(taxi, weather, [File("taxi_zone_lookup.csv", 0)])


def test_reject_empty_source(batch_files):
    taxi, weather, zones = batch_files
    weather[0] = File(weather[0].name, 0)
    with pytest.raises(ValueError, match="empty source"):
        plan_batch(taxi, weather, zones)
