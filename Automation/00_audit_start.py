# Databricks notebook source
"""Register the beginning of a pipeline execution."""

import sys

NOTEBOOK_PATH = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook().getContext().notebookPath().get()
)
PROJECT_ROOT = "/Workspace/" + NOTEBOOK_PATH.strip("/").rsplit("/", 2)[0]
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config.project_config import AUDIT_TABLES, CATALOG  # noqa: E402
from observability.audit import start_pipeline_run  # noqa: E402

spark.sql(f"USE CATALOG `{CATALOG}`")
RUN_ID = start_pipeline_run(spark, dbutils, AUDIT_TABLES["pipeline_runs"])
dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
print("Auditoria iniciada para run_id:", RUN_ID)
