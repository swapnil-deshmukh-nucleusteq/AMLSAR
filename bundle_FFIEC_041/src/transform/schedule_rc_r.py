# Databricks notebook source
# TRANSFORM: ffiec041_schedules.schedule_rc_r  |  upstream: capital_positions  |  aggregate_capital

# COMMAND ----------
dbutils.widgets.text("catalog",     "bfsi_regulatory_catalog")
dbutils.widgets.text("report_date", "2026-03-31")
dbutils.widgets.text("table_name",  "schedule_rc_r")
dbutils.widgets.text("schema_name", "ffiec041_schedules")

CATALOG     = dbutils.widgets.get("catalog")
REPORT_DATE = dbutils.widgets.get("report_date")
TABLE_NAME  = dbutils.widgets.get("table_name")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
TARGET      = f"`{CATALOG}`.`ffiec041_schedules`.`schedule_rc_r`"

from pyspark.sql import functions as F
print(f"Transform: {TARGET}")

# COMMAND ----------
# Transform: aggregate_capital
# Upstream tables were already created by bootstrap and populated by prior tasks
    src = spark.table('`{CATALOG}`.`raw_sources`.`capital_positions`')
    df = (src
        .groupBy(['component_type', 'rcoa_code', 'balance_date'])
        .agg(F.sum('rwa_amount').alias('rwa_amount'),
             F.lit(True).alias('validated')))
    _cols = [c for c in ['rcoa_code', 'line_item', 'description', 'amount_usd_000', 'ratio_pct', 'cblr_election', 'report_date', 'validated'] if c in df.columns]
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
dbutils.jobs.taskValues.set(key="schedule_rc_r_count", value=count)

