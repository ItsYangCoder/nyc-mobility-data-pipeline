# NYC Mobility Job: manual Databricks UI setup

Create one **unscheduled** Databricks Job with Python script tasks from the `refactor/silver-taxi-python-script` Git folder. Keep max concurrent runs at **1**. This uses no bundle deployment. Use the same workspace source for every task and review the selected paths before saving.

## Job parameters

Set `catalog`, `bronze_schema`, `silver_schema`, `gold_schema`, `quality_schema`, `taxi_source_dir`, `weather_source_dir`, `zones_source_dir`, `zone_file`, and `max_valid_distance_miles` as Job parameters. Use actual existing Volume directories for the three source directories. Defaults for this dev workspace: `nyc_mobility`, `nyc_bronze`, `nyc_silver`, `nyc_gold`, `nyc_quality`, `taxi_zone_lookup.csv`, and `100`. Keep source directories outside the code.

## Tasks

Choose **Python script** / **Workspace** for each file. Each parameter below means three consecutive script arguments: `--param`, its name, and its value. Use `{{job.parameters.NAME}}` for named Job parameters.

| Task key | Python file | Depends on | Parameters |
| --- | --- | --- | --- |
| `bronze` | `pipeline/bronze_auto_ingest.py` | — | `taxi_source_dir`, `weather_source_dir`, `zones_source_dir`, `zone_file`, `catalog`, `bronze_schema`, `quality_schema` |
| `silver_taxi` | `pipeline/green_taxi_clean.py` | `bronze` | `start_month={{tasks.bronze.values.start_month}}`, `end_month={{tasks.bronze.values.end_month}}`, `catalog`, `bronze_schema`, `silver_schema` |
| `silver_weather` | `pipeline/weather_clean.py` | `silver_taxi` | Same month references, `catalog`, `bronze_schema`, `silver_schema` |
| `silver_zones` | `pipeline/taxi_zones_clean.py` | `silver_weather` | `catalog`, `bronze_schema`, `silver_schema` |
| `taxi_quality` | `pipeline/green_taxi_quality.py` | `silver_zones` | `catalog`, `bronze_schema`, `silver_schema` |
| `zones_weather_quality` | `pipeline/zones_weather_quality.py` | `taxi_quality` | `catalog`, `silver_schema` |
| `gold` | `pipeline/gold_marts.py` | `zones_weather_quality` | `catalog`, `silver_schema`, `gold_schema`, `max_valid_distance_miles` |
| `gold_quality` | `pipeline/gold_quality.py` | `gold` | `catalog`, `silver_schema`, `gold_schema` |

For `bronze`, an example of the first three argument tokens is `["--param", "taxi_source_dir", "{{job.parameters.taxi_source_dir}}"]`; append each other parameter the same way. The `bronze` task publishes `start_month` and `end_month` task values for Silver. Do not substitute fixed month strings. Leave retries and schedule disabled during validation.

## Before the first run

Creating the Job does not validate its data. Existing taxi, weather, and zone Bronze rows lack file provenance. The guards intentionally stop a reload of those historical sources from another path; do not force COPY INTO, reset tables, or backfill fabricated timestamps to bypass them. Resolve the legacy lineage first, then test one controlled run and review the Bronze audit and Gold quality output before enabling a schedule or merging to `dev`.
