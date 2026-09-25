# Python pipeline

The numbered folders follow the run order. Every Job entry point is a `.py` file. Use [the Job setup guide](../docs/job_ui_setup.md) for task arguments and dependencies.

| Folder | What is inside |
| --- | --- |
| `00_source_inspection/` | Optional read-only source profiling. |
| `01_bronze/` | Full source discovery and Bronze ingestion; `bronze_auto_ingest.py` starts the full run. |
| `02_silver/` | Taxi, weather, and zone cleaning views. |
| `03_quality/` | Silver data quality checks. |
| `04_gold/` | Gold marts and Gold quality checks. |
| `05_analytics/` | Taxi demand, weather comparison, and zone pattern queries. |

`runtime.py` and `bronze_preflight.py` are shared helpers. They are not Job tasks. The three Bronze loaders in `01_bronze/` are also available for controlled single-file runs.

Source directories, catalog, schemas, and months come from Job parameters and discovered filenames. An unchanged file is skipped by `COPY INTO` on retry. A new path that conflicts with historical rows is blocked; replacing source bytes at the same path needs a deliberate backfill.

After pulling this branch into Databricks, update any **existing** Job task paths from `pipeline/<file>.py` to the corresponding numbered folder before running that Job. The included `databricks.yml` and [manual Job guide](../docs/job_ui_setup.md) use the new paths.
