# NYC Mobility: project walkthrough

This is the story of the current `refactor/silver-taxi-python-script` branch, from the first Databricks checks to the Python Job design. It is written for teammates onboarding to the project. [Job setup](job_ui_setup.md) has the exact task paths and parameters; [Bronze ingestion](dynamic_bronze.md) explains retries and audit behavior.

## 1. Start with the sources

We inspected three source types in Databricks Volumes: monthly Green Taxi Parquet, monthly Weather JSON, and a Taxi Zones CSV. The source inspection scripts live in `pipeline/00_source_inspection/`. We checked filenames, schemas, date coverage, and the monthly window before loading. March–May 2026 is the **historical validation window**, not a month range fixed in the pipeline.

The source files remain in Volumes. A different Databricks workspace needs its own accessible source directories and catalog/schema setup; pulling this repository does not move data.

**Proof to add:** screenshot of the three Volume directories and one source inspection output. Save it as `docs/img/01_sources.png`, then add `![Source files and inspection](img/01_sources.png)` here.

## 2. Build Bronze and track what was loaded

We first loaded the sources through Databricks notebooks. Bronze held raw Taxi rows, three monthly Weather JSON records (each contains hourly arrays), and the Zones lookup. After converting to scripts, `pipeline/01_bronze/bronze_auto_ingest.py` discovers matching Taxi and Weather months, validates the input files before writing, and calls the three per-file loaders. The loaders use `COPY INTO`. A new row records `source_file_path` and `source_ingested_at`; each file attempt writes an entry to `nyc_quality.bronze_file_runs` with its run ID, processing time, rows inserted, and status.

The Bronze guard stops a reload when existing rows for the requested period have an unknown or different source path. We saw why that matters: a separate March Taxi load added 44,208 rows to a 133,367-row Bronze table, reaching 177,575. We removed **only those 44,208 rows identified by that new source path** and confirmed the total returned to 133,367, with zero rows carrying that path. Do not use that cleanup as a general rerun step; inspect file history and lineage first. Older rows can have null lineage, so the new guard may intentionally fail against legacy tables.

| Historical Bronze check | Result |
| --- | ---: |
| Green Taxi raw, after cleanup | 133,367 rows |
| Weather raw | 3 monthly records |
| Taxi Zones raw | 265 rows |

**Proof to add:** Bronze row counts and audit table output. Save screenshots as `docs/img/02_bronze_counts.png` and `docs/img/03_bronze_audit.png`, then add image links here. The values above came from earlier Databricks runs, not from the new workspace.

## 3. Clean Silver and run quality checks

`pipeline/02_silver/` creates clean Taxi, hourly Weather, and Zones views. Bronze publishes discovered `start_month` and `end_month` task values; both Taxi and Weather use that same window. `pipeline/03_quality/` checks the Silver outputs before Gold runs.

Earlier validation of the March–May source window produced **133,355 Silver Taxi rows** (including **4,592 zero-distance trips**), **2,208 distinct Weather hours**, and **265 unique Zones**. The Weather source labels `2026-03-08 02:00` as a local hour despite the New York daylight-saving clock change. Hourly counts and unique keys alone do not prove the source time is UTC-correct; we kept this finding visible rather than silently changing source rows.

**Proof to add:** Silver Taxi, Weather, and Zones quality task outputs, as `docs/img/04_silver_quality.png`.

## 4. Build one Gold fact and check the joins

`pipeline/04_gold/gold_marts.py` creates SQL views: `fact_taxi_trip`, `dim_date`, `dim_hour`, `dim_zone`, `dim_weather_hour`, and the daily mobility view. The fact has one retained trip per row. `gold_quality.py` checks key uniqueness, Taxi and Weather reconciliation with Silver, and missing or multiplied dimension joins. A distance threshold is a Job parameter (default 100 miles): flagged trips stay in counts, while invalid distances are excluded from distance sums and averages.

An earlier Gold validation reconciled **133,355 trips**, **265 Zones**, and **2,208 Weather hours**, with **31 flagged distances**. These are evidence from the old workspace and source window, not fixed expected outputs for future data. The Bronze file audit is separate; the current Gold views do not expose row-level ingestion timestamps.

**Proof to add:** Gold quality task output as `docs/img/05_gold_quality.png`.

## 5. Move from notebooks to testable Python

The Job stages now live under numbered `pipeline/` folders: `00_source_inspection`, `01_bronze`, `02_silver`, `03_quality`, `04_gold`, and `05_analytics`. `pipeline/runtime.py` handles script parameters and Spark startup; `pipeline/bronze_preflight.py` contains source checks. `quality_rules.py` and `source_discovery.py` are shared Python modules. The numbered folders indicate processing order; the Job dependencies are defined in `databricks.yml` or the manual Job configuration.

Local `pytest` runs do **not** process Databricks data. The focused Bronze tests simulate first load, unchanged-file retry (zero additional rows, original timestamp retained), and rejection of conflicting or missing lineage. Other tests cover runtime, paths, and parameter handling. Rhea reported **69 passing local tests** after pulling the refactor branch; that count is a local run report, not a live end-to-end Job result.

**Proof to add:** terminal screenshot of the test run as `docs/img/06_pytest.png`.

## 6. Current Job status and the next handoff

We ran several **individual** Python-script validation tasks in Databricks and reviewed their output. We have not yet verified a complete eight-task Job from Bronze through Gold quality in a fresh workspace. Earlier one-off jobs are test runs; they are not proof that the final Job succeeds together. The PR stays a draft and is not merged into `dev`.

For the next workspace: connect the Git folder to this branch; prepare accessible Volume sources and the catalog/schemas; use the eight task paths and parameters in [Job setup](job_ui_setup.md); keep the Job unscheduled with one concurrent run; run once; inspect the Bronze audit and Gold quality output; then check a retry for duplicate rows. Compare results only for the same source files and parameters. Save a screenshot of the complete task graph and successful run as `docs/img/07_full_job.png` **after it actually passes**.

## Screenshots

The `docs/img/` filenames above are placeholders for the team's real screenshots. No screenshot is embedded yet, and no image in this guide represents a new successful run. Add a file only after matching its source, window, workspace, and run ID to the accompanying result.
