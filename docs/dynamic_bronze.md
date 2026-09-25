# Dynamic monthly Bronze runs

`pipeline/01_bronze/bronze_auto_ingest.py` discovers source months before loading. The [Job guide](job_ui_setup.md) defines the task order and parameters.

Before writing, `bronze_auto_ingest.py` requires nonempty, contiguous monthly Taxi Parquet and Weather JSON/CSV/Parquet files covering the **same months**, plus a nonempty Taxi Zones CSV. Filenames must match `green_tripdata_YYYY-MM.parquet` and `weather_YYYY-MM-01_YYYY-MM-lastday.<format>`; metadata JSON files are ignored. Preflight checks required schemas, weather timezone (`America/New_York`), complete hourly date boundaries, and duplicate hours. It fails on missing/extra months or malformed files. The Silver tasks receive the discovered window, including a new month without a code change.

Each file is passed to a `COPY INTO` loader. Retries of **unchanged paths** should return zero new rows. Local tests simulate the first load, retry, and conflicting paths; they do not run Databricks `COPY INTO`. Every attempt is appended to `nyc_quality.bronze_file_runs` with `run_id`, processing time, filename, inserted rows, and status. New Bronze rows store `source_file_path` and `source_ingested_at`; legacy rows may have `NULL` in both columns. `processed_at` is the audit record time, not the original source ingestion time. Gold views do not yet expose row-level ingestion timestamps.

To inspect file attempts:

```sql
SELECT run_id, processed_at, source, source_file, rows_inserted, status, error
FROM nyc_mobility.nyc_quality.bronze_file_runs
ORDER BY processed_at DESC;
```

Existing rows with no source path can conflict with a new load. The lineage guards stop that load; do not reset tables or fabricate timestamps to bypass them. Reconcile historical rows before the first full run.

The audit write and `COPY INTO` are separate operations. If the audit write fails after a successful load, a retry can report `SKIPPED`; reconcile Bronze and Databricks file-load history before interpreting audit counts. Replacing bytes at an existing path or replaying historical sources needs an explicit backfill procedure.
