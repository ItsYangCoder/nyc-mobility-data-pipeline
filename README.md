# NYC Mobility data pipeline

Databricks notebooks for the Green Taxi monthly pipeline. Use the Git folder on the `dev` branch and run **all cells** in order. The catalog and schemas already exist; no local installation is required.

| Order | Notebook | Purpose |
| --- | --- | --- |
| 01 | `notebooks/01_ingestion/green_taxi_ingestion.ipynb` | Read and inspect one source Parquet file; no writes |
| 02 | `notebooks/02_bronze/green_taxi_bronze.ipynb` | Load that month into the Bronze Delta table |
| 03 | `notebooks/03_silver/green_taxi_clean.ipynb` | Recreate the clean Silver view for the chosen month window |
| 04 | `notebooks/04_quality/green_taxi_quality.ipynb` | Verify counts and data quality |

## Widgets to set in Databricks

- For 01 and 02, set `run_month` to `2026-03`, `2026-04`, or `2026-05` (one month at a time). Set `source_dir` to `/Volumes/nyc_mobility/nyc_bronze/ftw-b12-de-r2/groups/week-08/group-c/landing/green_taxi`. These values are supplied at run time; the notebooks do not depend on the last value another notebook used.
- Run 02 once per month. `COPY INTO` skips the same file on a rerun, so `num_inserted_rows` should be zero for a repeated month.
- For 03, set `start_month=2026-03` and `end_month=2026-05` to cover the assignment window. The end month is inclusive. Each rerun updates the view definition; change these widgets to select another window. Run 04 after 03.
- `catalog`, `bronze_schema`, and `silver_schema` default to `nyc_mobility`, `nyc_bronze`, and `nyc_silver`. Change them in widgets if deploying to another environment.

The Silver view excludes pickups outside the selected window and trips whose dropoff is before pickup or missing. Zero-distance rides remain with `is_zero_distance=true`. For the current March–May Bronze data, the expected totals are 133,367 Bronze rows and 133,355 Silver rows, including 4,592 zero-distance rides. The 11 out-of-window pickups and one reversed-time trip account for the difference. No source Parquet files are stored in this repository.

**Note:** `COPY INTO` deduplicates by source file in the same Delta table. If the upstream file contents change at the same path, plan a separate backfill; a regular rerun will not replace rows already loaded from that file.
