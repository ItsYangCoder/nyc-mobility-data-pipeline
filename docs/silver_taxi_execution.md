# Green Taxi Silver execution

`silver_taxi.py` owns the Silver query. `pipeline/green_taxi_clean.py` calls it from a Python script task.

- Local validation: `python3 -m pytest -q tests/`. This does not connect to Databricks.
- Databricks: run `pipeline/green_taxi_clean.py` as a Python script task. The Job passes discovered months and catalog/schema arguments.
- The Silver stage needs Databricks Spark and access to the Bronze table. Unit tests exercise its SQL builder without starting a Spark session.

Only the Silver view definition is replaced. A run with a new month window changes the view's range; no Silver rows are appended.
