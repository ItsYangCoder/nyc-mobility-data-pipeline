# NYC Mobility data pipeline

Python script tasks for the Green Taxi, weather, and Taxi Zones Bronze → Silver → Gold pipeline. Source files live in the existing Databricks Volume; no raw data is stored in this repository.

## Find the code

| Folder or file | Purpose |
| --- | --- |
| [`pipeline/`](pipeline/README.md) | Job entry points by stage, inspection scripts, and small runtime helpers. |
| [`tests/`](tests) | Local rule, runtime, and Bronze repeat-load tests. |
| [`docs/job_ui_setup.md`](docs/job_ui_setup.md) | Job tasks, dependencies, and parameter setup. |
| [`docs/dynamic_bronze.md`](docs/dynamic_bronze.md) | Source discovery, ingestion checks, and audit log. |
| [`docs/`](docs) | Data quality and modeling notes. |

Start a full run with `pipeline/01_bronze/bronze_auto_ingest.py`, followed by Silver checks, Gold, and analytics as described in [the script index](pipeline/README.md). Set Volume directories in Job parameters; the run discovers its months from source filenames and passes the selected range downstream. Existing Jobs that use the former flat paths need their Python file paths updated to the numbered folders before the next run.

## Check locally in VS Code

```bash
python3 -m pip install -r requirements-dev.txt
python3 -m pytest -q tests/
python3 -m compileall -q pipeline
```

The tests check source rules and simulate repeated `COPY INTO` calls without using Databricks compute. They cannot validate live Unity Catalog permissions, the current contents of the Volume, or actual Databricks `COPY INTO` history. Check a full run in Databricks after compute is available.

Bronze stores a source file path and ingestion timestamp for **new** rows. Existing legacy rows may lack both. Do not reload them at a new path just to populate lineage: the Bronze guards stop when historical rows conflict with the requested source. See [Dynamic Bronze runs](docs/dynamic_bronze.md) for reconciliation and audit behavior.
