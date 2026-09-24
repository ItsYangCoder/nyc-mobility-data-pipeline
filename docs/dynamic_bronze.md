# Dynamic monthly Bronze runs

Run `notebooks/00_pipeline/run_pipeline.ipynb` as the Databricks Job notebook task. Configure the three Volume directories and catalog/schema widgets for the environment. The default directories point to the current group's landing folders; months and weather filenames are discovered from the Volume on every run. The controller runs Bronze, Silver, the existing quality notebooks, Gold, and Gold quality in order. A failure stops downstream tasks.

Before writing, `bronze_auto_ingest.ipynb` requires nonempty, contiguous monthly Taxi Parquet and Weather JSON/CSV/Parquet files covering the **same months**, plus a nonempty Taxi Zones CSV. Filenames must match `green_tripdata_YYYY-MM.parquet` and `weather_YYYY-MM-01_YYYY-MM-lastday.<format>`; metadata JSON files are ignored. Preflight checks required schemas, weather timezone (`America/New_York`), complete hourly date boundaries, and duplicate hours. It fails on missing/extra months or malformed files. The Silver start/end widgets receive the discovered window, including a new month without a code change.

Each file is passed to the existing `COPY INTO` loader. Retries of **unchanged paths** should return zero new rows. Every attempt is appended to `nyc_quality.bronze_file_runs` with `run_id`, processing time, filename, inserted rows, and status. This timestamp records **processing**, not the original source ingestion time. New Bronze rows also store `source_file_path` and a stable `source_ingested_at` captured during `COPY INTO`. Existing rows loaded before this change remain `NULL` for these columns; do not assign them an invented historical load time. Gold views do not yet expose the row-level ingestion timestamp. For example:

```sql
SELECT run_id, processed_at, source, source_file, rows_inserted, status, error
FROM nyc_mobility.nyc_quality.bronze_file_runs
ORDER BY processed_at DESC;
```

The audit write and `COPY INTO` are separate operations. If the audit write fails after a successful load, a retry can report `SKIPPED`; reconcile the Bronze table and Databricks file-load history before interpreting audit counts. Replacing bytes at an existing path, removing months, or replaying historical sources needs an explicit backfill procedure. Configure the Job to prevent overlapping runs against the same Bronze tables. Run the notebook once manually and check Gold quality before enabling a schedule.
