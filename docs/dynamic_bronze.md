# Dynamic monthly Bronze runs

The [manual Job setup](job_ui_setup.md) uses eight Python script tasks from this refactor branch. Bronze passes discovered months to Silver as task values. The Job stays unscheduled and allows one concurrent run while historical Bronze lineage is reconciled. Local Python tests do not execute Databricks tasks.

Before writing, `bronze_auto_ingest.py` requires nonempty, contiguous monthly Taxi Parquet and Weather JSON/CSV/Parquet files covering the **same months**, plus a nonempty Taxi Zones CSV. Filenames must match `green_tripdata_YYYY-MM.parquet` and `weather_YYYY-MM-01_YYYY-MM-lastday.<format>`; metadata JSON files are ignored. Preflight checks required schemas, weather timezone (`America/New_York`), complete hourly date boundaries, and duplicate hours. It fails on missing/extra months or malformed files. The Silver tasks receive the discovered window, including a new month without a code change.

Each file is passed to a `COPY INTO` loader. Retries of **unchanged paths** should return zero new rows. The local idempotency tests simulate the first load and the retry for all three loaders, and check that conflicting/unknown paths stop before COPY. They do not run Databricks `COPY INTO`. Every attempt is appended to `nyc_quality.bronze_file_runs` with `run_id`, processing time, filename, inserted rows, and status. This timestamp records **processing**, not the original source ingestion time. New Bronze rows also store `source_file_path` and a stable `source_ingested_at` captured during `COPY INTO`. Existing rows loaded before this change remain `NULL` for these columns; do not assign them an invented historical load time. Gold views do not yet expose the row-level ingestion timestamp. For example:

```sql
SELECT run_id, processed_at, source, source_file, rows_inserted, status, error
FROM nyc_mobility.nyc_quality.bronze_file_runs
ORDER BY processed_at DESC;
```

The audit write and `COPY INTO` are separate operations. If the audit write fails after a successful load, a retry can report `SKIPPED`; reconcile the Bronze table and Databricks file-load history before interpreting audit counts. Replacing bytes at an existing path, removing months, or replaying historical sources needs an explicit backfill procedure. Configure the Job to prevent overlapping runs against the same Bronze tables. Run the dev task once and check Gold quality before enabling a schedule.
