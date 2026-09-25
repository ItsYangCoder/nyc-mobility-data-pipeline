# Python source layout

Pipeline stages now use `.py` files. Databricks recognizes the `# Databricks notebook source` header as a Python source notebook, so existing widgets, `dbutils.notebook.run`, and Job task values continue to work. The SQL analytics cells call `spark.sql` and `display`.

Edit and run `python3 -m pytest -q tests/` in VS Code. `python3 -m compileall -q notebooks` checks stage syntax locally; a pipeline stage still needs Databricks Spark, Unity Catalog, and `dbutils` to execute. `databricks.yml` points to the `.py` source files, but a Git pull does not change any already deployed Job. Do not run the full pipeline solely to validate this file format change; check the individual stages in the dev Git folder first.
