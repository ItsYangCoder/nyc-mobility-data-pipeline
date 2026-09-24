# NYC Mobility data pipeline

Databricks Git folder on the `dev` branch. The catalog `nyc_mobility` and the `nyc_bronze`, `nyc_silver`, `nyc_gold`, and `nyc_quality` schemas already exist. Source files stay in the existing volume; the repository contains notebooks, not raw data.

## Green Taxi (March–May 2026)

1. `notebooks/01_ingestion/green_taxi_ingestion.ipynb` inspects one monthly Parquet file.
2. `notebooks/02_bronze/green_taxi_bronze.ipynb` loads each month using `COPY INTO`; set `run_month` and `source_dir` widgets independently in each notebook. Load March, April, and May. Repeating a loaded file does not add rows.
3. `notebooks/03_silver/green_taxi_clean.ipynb` creates a Silver view. Set `start_month=2026-03` and `end_month=2026-05` to include all three months. Rerun to change the view's window; zero-distance trips are flagged, not discarded.
4. `notebooks/04_quality/green_taxi_quality.ipynb` checks the results. Previous observed baseline: Bronze 133,367, Silver 133,355, Silver zero-distance 4,592. Confirm these in the current workspace.

For Taxi, set `source_dir` to the Green Taxi directory in the existing volume. A `COPY INTO` rerun skips an unchanged previously loaded file; replacing source bytes at the same path requires a planned backfill.

## Taxi Zones (known CSV source)

1. Run `notebooks/01_ingestion/taxi_zones_ingestion.ipynb` to read and profile the CSV without writing a table. Its source directory and CSV filename are widgets.
2. Run `notebooks/02_bronze/taxi_zones_bronze.ipynb`. Its `zones_source_dir` and `source_file` widgets default to the current group's landing directory and `taxi_zone_lookup.csv`; both can be changed for another valid source. The notebook loads **only** `taxi_zone_lookup.csv`, with CSV headers and inferred types. It does not ingest the sibling metadata JSON. A repeated `COPY INTO` skips the already loaded file.
3. Run `notebooks/03_silver/taxi_zones_clean.ipynb`. It checks that `LocationID` is unique and non-null and `Zone` is present, then creates `nyc_mobility.nyc_silver.vw_taxi_zones_clean` with trimmed labels. Do not publish this view if the source fails those checks.
4. Previously observed CSV baseline: **265 rows, 265 distinct IDs, no missing ID or Zone**. Confirm the new Bronze and Silver outputs in Databricks.

## Weather (inspect actual source first)

1. Run `notebooks/01_ingestion/weather_inventory.ipynb` to list actual weather filenames. Set its `source_file` widget to one listed `.parquet`, `.csv`, or `.json` and rerun the inspection cell to see count, schema, and sample. No write occurs.
2. Run `notebooks/02_bronze/weather_bronze.ipynb` for **each** approved weather file, passing the exact filename in `source_file`. The `weather_source_dir` widget points to the group's landing volume by default. `COPY INTO` is idempotent per file and loads only that filename; CSV expects headers, JSON expects a multiline document, and Parquet uses the embedded schema. Files for the same Bronze table must have a compatible schema.
3. Run `notebooks/03_silver/weather_clean.ipynb` with `start_month=2026-03`, `end_month=2026-05`. It accepts a flat hourly file containing `temperature_2m` and a recognized time field (`time`, `timestamp`, `datetime`, `observation_time`, `date`), or nested Open-Meteo `hourly` arrays of `time` and `temperature_2m`. For another flat time name set `time_column`. Optional recognized arrays/columns are `precipitation`, `wind_speed_10m`, `rain`, and `snowfall`; available `timezone`, `latitude`, and `longitude` fields are retained for join validation. Unsupported fields and invalid timestamps fail clearly rather than creating a misleading view. The view includes `observation_hour` and available weather metrics.
4. Run `notebooks/04_quality/zones_weather_quality.ipynb`. Check the zone count/key, weather coverage, temperature nulls, and duplicate hours **before** any weather-to-taxi join. The weather data's source timezone and hourly grain must be confirmed from the source metadata; the notebook cannot infer them reliably from a file name.

`catalog`, `bronze_schema`, and `silver_schema` are widgets with current project defaults. Source directories are configurable widgets. A `CREATE OR REPLACE VIEW` uses the window from the latest run; changing widgets alone does not change an existing view until that notebook is run again.

## Current scope

Green Taxi, Taxi Zones and Weather notebooks are stored in Git. The weather files and their schema could not be read from Git, so live Weather execution and expected Weather row counts remain to be verified in Databricks. Gold joins, orchestration, and end-to-end runtime validation are separate follow-up tasks.
