# Pipeline scripts

These are Python script tasks. Run them through a Databricks Job with the parameters in [the Job setup guide](../docs/job_ui_setup.md). The file paths stay at this level because existing Job tasks point to them.

| Step | Scripts | Purpose |
| --- | --- | --- |
| Bronze | `bronze_auto_ingest.py` | Discover complete source months, validate files, load them, and write file run audit records. Start here for a full run. |
| Bronze single file | `green_taxi_bronze.py`, `weather_bronze.py`, `taxi_zones_bronze.py` | Load one requested source file. Each validates source lineage before `COPY INTO`. |
| Silver | `green_taxi_clean.py`, `taxi_zones_clean.py`, `weather_clean.py` | Build clean views from Bronze. |
| Silver checks | `green_taxi_quality.py`, `zones_weather_quality.py` | Validate counts, keys, coverage, and joins. |
| Gold | `gold_marts.py`, `gold_quality.py` | Build and check the star schema views. |
| Analytics | `01_taxi_demand.py`, `02_weather_comparison.py`, `03_zone_patterns.py` | Query the Gold views. |
| Source inspection | `green_taxi_ingestion.py`, `taxi_zones_ingestion.py`, `weather_inventory.py` | Inspect source files without loading Bronze. |

`bronze_preflight.py` checks source schemas and weather hours; `runtime.py` handles CLI parameters, source lineage guards, and Spark startup. These are helper modules, not Job tasks.

Supply Volume paths, catalog, schema, and month parameters through the Job. `COPY INTO` skips a file it has already loaded into a table. For files already present with unknown or different source paths, the single-file loaders stop before copying; reconcile historical rows separately. Replacing the bytes at an existing path requires a planned backfill.

Run `python3 -m pytest -q tests/` from the repository root for local tests, including repeat-load checks. Those tests use a Spark stand-in; a real `COPY INTO` run still requires Databricks compute.
