# Databricks notebook source
# TRANSFORM: ffiec041_schedules.schedule_rc  |  upstream: fact_gl_positions  |  aggregate_schedule

# COMMAND ----------
dbutils.widgets.text("catalog",     "bfsi_regulatory_catalog")
dbutils.widgets.text("report_date", "2026-03-31")
dbutils.widgets.text("table_name",  "schedule_rc")
dbutils.widgets.text("schema_name", "ffiec041_schedules")

CATALOG     = dbutils.widgets.get("catalog")
REPORT_DATE = dbutils.widgets.get("report_date")
TABLE_NAME  = dbutils.widgets.get("table_name")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
TARGET      = f"`{CATALOG}`.`ffiec041_schedules`.`schedule_rc`"

from pyspark.sql import functions as F
print(f"Transform: {TARGET}")

# COMMAND ----------
# Transform: aggregate_schedule
# Upstream tables were already created by bootstrap and populated by prior tasks
    src = spark.table('`{CATALOG}`.`conformed`.`fact_gl_positions`')
    df = (src
        .groupBy(['line_item_ref', 'report_date', 'rcon_code', 'schedule_ref'])
        .agg(F.sum('ytd_amount_usd').alias('ytd_amount_usd'),
             F.lit(True).alias('validated')))
    _cols = [c for c in ['rcon_code', 'line_item', 'description', 'amount_usd_000', 'report_date', 'validated'] if c in df.columns]
    df = df.select(_cols) if _cols else df

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
dbutils.jobs.taskValues.set(key="schedule_rc_count", value=count)

