# NYC Mobility data pipeline

Python scripts for Green Taxi, weather, and Taxi Zones: Bronze → Silver → Gold → analytics. The source files stay in Databricks Volumes.

- [Pipeline folders](pipeline/README.md): find scripts by stage.
- [Databricks Job setup](docs/job_ui_setup.md): task paths, dependencies, and parameters.
- [Bronze ingestion](docs/dynamic_bronze.md): source rules, retries, and file audit.
- [Gold model](docs/gold_star_schema.md): grain, dimensions, and diagram.
- [Weather time finding](docs/weather_time_quality.md) and [distance policy](docs/distance_quality.md): observed quality results.

For local checks in VS Code, from the repository root:

```bash
python3 -m pip install -r requirements-dev.txt
python3 -m pytest -q tests/
python3 -m compileall -q pipeline
```

Local tests do not query Databricks. After pulling the branch, update any existing Job task paths to match the numbered folders before running the Job.
