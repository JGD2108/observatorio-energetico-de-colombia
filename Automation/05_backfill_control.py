# Databricks notebook source
"""Validate and register an optional historical backfill request."""

import sys
from datetime import date

NOTEBOOK_PATH = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
PROJECT_ROOT = "/Workspace/" + NOTEBOOK_PATH.strip("/").rsplit("/", 2)[0]
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from backfill.runtime import _parameter, chunk_days, resolve_window  # noqa: E402
from config.project_config import CATALOG, ENVIRONMENT  # noqa: E402

mode_text = _parameter(dbutils, "execution_mode", "AUTO").upper()
start_text = _parameter(dbutils, "backfill_start_date")
end_text = _parameter(dbutils, "backfill_end_date")
if mode_text == "BACKFILL":
    start, end, mode = resolve_window(dbutils, date.today(), date.today(), "BACKFILL")
else:
    start, end, mode = None, None, mode_text

backfill_id = _parameter(dbutils, "backfill_id") or _parameter(dbutils, "run_id")
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{CATALOG}`.`audit`.`backfill_runs` (
  backfill_id STRING NOT NULL,
  job_run_id STRING,
  environment STRING,
  execution_mode STRING NOT NULL,
  start_date DATE,
  end_date DATE,
  chunk_days INT,
  status STRING NOT NULL,
  started_at TIMESTAMP NOT NULL,
  completed_at TIMESTAMP,
  message STRING
) USING DELTA
""")
spark.sql(f"""
MERGE INTO `{CATALOG}`.`audit`.`backfill_runs` target
USING (SELECT '{backfill_id}' backfill_id, '{_parameter(dbutils, 'run_id')}' job_run_id,
              '{ENVIRONMENT}' environment, '{mode}' execution_mode,
              {f"DATE '{start}'" if start else 'CAST(NULL AS DATE)'} start_date,
              {f"DATE '{end}'" if end else 'CAST(NULL AS DATE)'} end_date,
              {chunk_days(dbutils)} chunk_days) source
ON target.backfill_id = source.backfill_id
WHEN NOT MATCHED THEN INSERT (backfill_id, job_run_id, environment, execution_mode, start_date, end_date, chunk_days, status, started_at)
VALUES (source.backfill_id, source.job_run_id, source.environment, source.execution_mode, source.start_date, source.end_date, source.chunk_days, 'RUNNING', current_timestamp())
""")
dbutils.jobs.taskValues.set(key="backfill_id", value=backfill_id)
print("Control Fase 5 validado:", backfill_id, mode, start, end)
