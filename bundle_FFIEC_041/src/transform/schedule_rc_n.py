# Databricks notebook source
# TRANSFORM: ffiec041_schedules.schedule_rc_n  |  upstream: fact_loans_conformed  |  aggregate_schedule

# COMMAND ----------
dbutils.widgets.text("catalog",     "bfsi_regulatory_catalog")
dbutils.widgets.text("report_date", "2026-03-31")
dbutils.widgets.text("table_name",  "schedule_rc_n")
dbutils.widgets.text("schema_name", "ffiec041_schedules")

CATALOG     = dbutils.widgets.get("catalog")
REPORT_DATE = dbutils.widgets.get("report_date")
TABLE_NAME  = dbutils.widgets.get("table_name")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
TARGET      = f"`{CATALOG}`.`ffiec041_schedules`.`schedule_rc_n`"

from pyspark.sql import functions as F
print(f"Transform: {TARGET}")

# COMMAND ----------
# Transform: aggregate_schedule
# Upstream tables were already created by bootstrap and populated by prior tasks
    src = spark.table('`{CATALOG}`.`conformed`.`fact_loans_conformed`')
    df = (src

    )
    _cols = [c for c in ['rcon_code', 'line_item', 'column_ref', 'amount_usd_000', 'report_date', 'validated'] if c in src.columns]
    df = df.select(_cols) if _cols else src

# COMMAND ----------
# DQ — manifest rules via shared dq_runner
%run ../dq/dq_runner
df = run_dq(df, TABLE_NAME, raise_on_critical=True)

# COMMAND ----------
# Write into existing table (schema created by bootstrap)
(df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET))

count = spark.table(TARGET).count()
print(f"  [OK] {count} rows in {TARGET}")
dbutils.jobs.taskValues.set(key="schedule_rc_n_count", value=count)

