# Green Taxi Silver execution

`silver_taxi.py` owns the Silver query. The existing Databricks notebook calls it, so the current Job keeps working without a task change.

- Local validation: `python3 -m pytest -q tests/`. This does not connect to Databricks.
- Databricks: run `notebooks/03_silver/green_taxi_clean.ipynb` with the month and catalog/schema widgets. The Job supplies discovered months to those widgets.
- To use a Python script task later, point it to `silver_taxi.py` in the Git folder and pass `--start-month`, `--end-month`, `--catalog`, `--bronze-schema`, and `--silver-schema`. The script needs Databricks Spark and access to the Bronze table.

Only the Silver view definition is replaced. A run with a new month window changes the view's range; no Silver rows are appended.
