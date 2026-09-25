# Python source layout

`pipeline/` contains ordinary Python modules with a `run(spark, options, dbutils, display)` function. The dev Job uses Python script tasks, passing source paths, schemas and the months discovered by Bronze as arguments. There are no notebook tasks or notebook imports. `pipeline/runtime.py` handles command-line arguments and Spark startup.

From VS Code, run `python3 -m pytest -q tests/` and `python3 -m compileall -q pipeline`. These run without Databricks. To execute transformations locally, supply a Spark connection and the required Volume sources; syntax and unit tests do not verify Unity Catalog or COPY INTO. A Git pull does not update an existing deployed Job.
