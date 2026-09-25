# NYC Mobility data pipeline

Python scripts for Green Taxi, weather, and Taxi Zones. Source files live in Databricks Volumes.

| Folder | Purpose |
| --- | --- |
| `pipeline/00_source_inspection/` | Inspect input files. |
| `pipeline/01_bronze/` | Discover sources and load Bronze. |
| `pipeline/02_silver/` | Clean Taxi, weather, and zones. |
| `pipeline/03_quality/` | Check Silver views. |
| `pipeline/04_gold/` | Build and check Gold views. |
| `pipeline/05_analytics/` | Answer demand, weather, and zone questions. |

`pipeline/runtime.py` and `pipeline/bronze_preflight.py` are shared helpers, not Job tasks. See [Job setup](docs/job_ui_setup.md) for task order and parameters, and [Bronze ingestion](docs/dynamic_bronze.md) for retries and audit records.

For local checks in VS Code, from the repository root:

```bash
python3 -m pip install -r requirements-dev.txt
python3 -m pytest -q tests/
python3 -m compileall -q pipeline
```

Local tests do not query Databricks. After pulling the branch, update any existing Job task paths to match the numbered folders before running the Job.

Gold has one retained trip per `fact_taxi_trip` row and date, hour, zone, and weather-hour dimensions. Its objects are SQL views, and the file audit remains in `nyc_quality.bronze_file_runs`; Gold views do not currently expose row-level ingestion timestamps. `max_valid_distance_miles` defaults to 100: outlier distances remain flagged in the fact and excluded from distance sums and averages, while trips remain in counts. The March 2026 weather source includes a local `02:00` label on the daylight-saving transition day; hourly uniqueness checks do not establish UTC correctness. Recheck source-time policy before relying on UTC event times.

The complete Job has not yet been validated end to end in a fresh Databricks workspace.
