# Pipeline folders

| Folder | Purpose |
| --- | --- |
| `00_source_inspection/` | Read-only inspection of the three sources. |
| `01_bronze/` | Source discovery and Bronze loaders; start a full run with `bronze_auto_ingest.py`. |
| `02_silver/` | Clean Taxi, weather, and zone views. |
| `03_quality/` | Silver quality checks. |
| `04_gold/` | Gold marts and Gold quality checks. |
| `05_analytics/` | Demand, weather comparison, and zone pattern queries. |

`runtime.py` and `bronze_preflight.py` are shared helpers, not Job tasks. Use the [Job guide](../docs/job_ui_setup.md) for task paths and parameters, and the [Bronze guide](../docs/dynamic_bronze.md) for source and retry behavior.
