# NYC Mobility data pipeline

Databricks Git folder on the `dev` branch. The catalog `nyc_mobility` and the `nyc_bronze`, `nyc_silver`, `nyc_gold`, and `nyc_quality` schemas already exist. Source files stay in the existing volume; the repository contains Python scripts, not raw data.

## Automated runs

Run the dev Job stages in dependency order, starting with `pipeline/bronze_auto_ingest.py`. Set the source-directory arguments for the environment; see [Dynamic Bronze runs](docs/dynamic_bronze.md) for input contracts, audit results, and schedule readiness. The per-file scripts below remain useful for inspection and manual backfills.

## Local rule tests

From the repository root, run `python3 -m pip install -r requirements-dev.txt` and `python3 -m pytest -q tests/`. CI runs the same tests on pull requests and pushes to `dev`. `quality_rules.py` is imported by the scripts from this Databricks Git folder; run the updated scripts from the Git folder, not detached workspace copies.

## Green Taxi (March–May 2026)

1. `pipeline/green_taxi_ingestion.py` inspects one monthly Parquet file.
2. `pipeline/green_taxi_bronze.py` loads each month using `COPY INTO`; set `run_month` and `source_dir` arguments for each script. Load March, April, and May. Repeating a loaded file does not add rows.
3. `pipeline/green_taxi_clean.py` creates a Silver view. Set `start_month=2026-03` and `end_month=2026-05` to include all three months. Rerun to change the view's window; zero-distance trips are flagged, not discarded.
4. `pipeline/green_taxi_quality.py` checks the results. Previous observed baseline: Bronze 133,367, Silver 133,355, Silver zero-distance 4,592. Confirm these in the current workspace.

For Taxi, set `source_dir` to the Green Taxi directory in the existing volume. A `COPY INTO` rerun skips an unchanged previously loaded file; replacing source bytes at the same path requires a planned backfill.

## Taxi Zones (known CSV source)

1. Run `pipeline/taxi_zones_ingestion.py` to read and profile the CSV without writing a table. Its source directory and CSV filename are arguments.
2. Run `pipeline/taxi_zones_bronze.py`. Its `zones_source_dir` and `source_file` arguments use the supplied Volume directory and CSV filename; both can be changed for another valid source. The script loads **only** `taxi_zone_lookup.csv`, with CSV headers and inferred types. It does not ingest the sibling metadata JSON. A repeated `COPY INTO` skips the already loaded file.
3. Run `pipeline/taxi_zones_clean.py`. It checks that `LocationID` is unique and non-null and `Zone` is present, then creates `nyc_mobility.nyc_silver.vw_taxi_zones_clean` with trimmed labels. Do not publish this view if the source fails those checks.
4. Previously observed CSV baseline: **265 rows, 265 distinct IDs, no missing ID or Zone**. Confirm the new Bronze and Silver outputs in Databricks.

## Weather (inspect actual source first)

1. Run `pipeline/weather_inventory.py` to list actual weather filenames. Set its `source_file` parameter to one listed `.parquet`, `.csv`, or `.json` and rerun the inspection cell to see count, schema, and sample. No write occurs.
2. Run `pipeline/weather_bronze.py` for **each** approved weather file, passing the exact filename in `source_file`. The `weather_source_dir` parameter points to the group's landing volume by default. `COPY INTO` is idempotent per file and loads only that filename; CSV expects headers, JSON expects a multiline document, and Parquet uses the embedded schema. Files for the same Bronze table must have a compatible schema.
3. Run `pipeline/weather_clean.py` with `start_month=2026-03`, `end_month=2026-05`. It accepts a flat hourly file containing `temperature_2m` and a recognized time field (`time`, `timestamp`, `datetime`, `observation_time`, `date`), or nested Open-Meteo `hourly` arrays of `time` and `temperature_2m`. For another flat time name set `time_column`. Optional recognized arrays/columns are `precipitation`, `wind_speed_10m`, `rain`, and `snowfall`; available `timezone`, `latitude`, and `longitude` fields are retained for join validation. Unsupported fields and invalid timestamps fail clearly rather than creating a misleading view. The view includes `observation_hour` and available weather metrics.
4. Run `pipeline/zones_weather_quality.py`. Check the zone count/key, weather coverage, temperature nulls, and duplicate hours **before** any weather-to-taxi join. The weather data's source timezone and hourly grain must be confirmed from the source metadata; the script cannot infer them reliably from a file name. See [Weather time quality](docs/weather_time_quality.md) for the observed March 8 daylight-saving anomaly and the checks required before a Gold join.

`catalog`, `bronze_schema`, and `silver_schema` are arguments with current project defaults. Source directories are configurable widgets. A `CREATE OR REPLACE VIEW` uses the window from the latest run; changing parameters alone does not change an existing view until that script is run again.

## Gold and analytics (after Silver and quality checks)

1. Confirm all three Silver views cover March–May 2026 and run existing Silver quality scripts. Previously observed in Databricks: Taxi Silver 133,355 rows (133,355 distinct projected rows), Zones 265 unique IDs, Weather 2,208 unique hours. The Taxi-to-Zones-and-Weather hour check returned zero missing matches; verify this after any source changes.
2. Run `pipeline/gold_marts.py` with the default catalog/schema widgets, then run `pipeline/gold_quality.py`. The Gold script first verifies source grains, keys, timezone and joins. It creates the single-fact star as views: `fact_taxi_trip`, `dim_date`, `dim_hour`, `dim_zone`, `dim_weather_hour`, plus derived daily output `vw_mobility_daily`. Taxi has one row per selected Silver trip; the weather dimension has one observation per local hour, linked via `fact_taxi_trip.weather_hour = dim_weather_hour.observation_hour`. On a successful Gold run, the obsolete `fact_weather_hourly` **view** from the earlier model is retired. The Gold quality script checks key uniqueness, dimensional and weather joins, no row multiplication, and Silver-to-Gold counts. See [Distance quality](docs/distance_quality.md) for the distance rule and its verified baseline.
3. Run the three Python analytics scripts under `pipeline/` in order: `01_taxi_demand` (highest pickup demand by date/hour/zone), `02_weather_comparison` (wet versus dry pickup-hour demand and trip measures), and `03_zone_patterns` (pickup and dropoff role by area, time, and pickup-hour weather). Each reads the Gold views; the optional DOT advisories question is outside the three-source project scope.

Gold is rerunnable because these objects are views and read the latest Silver window. The Taxi `trip_key` is a SHA-256 string fingerprint of the ten current Silver columns, not a source-provided trip ID. The observed 133,355 Silver rows have distinct projected values; a future genuine duplicate with identical values needs a source-record identifier. The Weather dimension uses the source's local `observation_hour` as its unique key. Date, Hour and Zone each serve both pickup and dropoff roles; Weather joins by pickup hour. Weather reflects one source observation location, not a measurement for each Taxi Zone. March 8, 2026 includes an anomalous local 02:00 weather hour; see [Weather time quality](docs/weather_time_quality.md).

The analytics are comparisons of observed Taxi activity and Weather, not evidence that Weather causes demand changes. Q2 aggregates Taxi at pickup hour before joining Weather; it never sums an hourly precipitation value once per trip. Q3 counts each trip separately in pickup and dropoff roles, so counts across both roles must not be summed as unique trips. Negative Taxi fares are excluded only from explicitly named nonnegative-fare measures. Derived peak-hour and temperature-band labels require agreed definitions and are omitted. Actual source ingestion time and source-file lineage are unavailable in the existing Bronze rows, so Gold does not fabricate them. The diagram's `source_ingested_at` fields are planned: capture real load time and source file at Bronze for future loads, propagate through Silver, then add them to Gold with a deliberate backfill policy for older rows. `current_timestamp()` in a view would mean query time, not ingestion time.

## Rerun after updating `dev`

Pull `dev` in the Databricks Git folder, confirm the three Silver views, then run `pipeline/gold_marts.py`, `pipeline/gold_quality.py`, and the three Python analytics scripts in that order. Check Gold quality before using analytics.
