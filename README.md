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

New teammate? Read the [project walkthrough](docs/project_walkthrough.md) for what we built, historical results, and the next validation step.

For local checks in VS Code, from the repository root:

```bash
python3 -m pip install -r requirements-dev.txt
python3 -m pytest -q tests/
python3 -m compileall -q pipeline
```

Local tests do not query Databricks. After pulling this branch, update existing Job paths to the numbered folders before running. The complete Job still needs validation in the new workspace.
