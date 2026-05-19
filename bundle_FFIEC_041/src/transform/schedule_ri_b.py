# Databricks notebook source
# TRANSFORM: ffiec041_schedules.schedule_ri_b  |  upstream: allowance_positions  |  roll_forward_acl

# COMMAND ----------
dbutils.widgets.text("catalog",     "bfsi_regulatory_catalog")
dbutils.widgets.text("report_date", "2026-03-31")
dbutils.widgets.text("table_name",  "schedule_ri_b")
dbutils.widgets.text("schema_name", "ffiec041_schedules")

CATALOG     = dbutils.widgets.get("catalog")
REPORT_DATE = dbutils.widgets.get("report_date")
TABLE_NAME  = dbutils.widgets.get("table_name")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
TARGET      = f"`{CATALOG}`.`ffiec041_schedules`.`schedule_ri_b`"

from pyspark.sql import functions as F
print(f"Transform: {TARGET}")

# COMMAND ----------
# Transform: roll_forward_acl
# Upstream tables were already created by bootstrap and populated by prior tasks
    src = spark.table('`{CATALOG}`.`raw_sources`.`allowance_positions`')
    df = (src
        .groupBy(['pool_id', 'asset_class', 'balance_date'])
        .agg(F.sum('balance_date').alias('balance_date'),
             F.lit(True).alias('validated')))
    _cols = [c for c in ['riad_code', 'line_item', 'column_ref', 'ytd_amount_usd_000', 'report_date', 'validated'] if c in df.columns]
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
dbutils.jobs.taskValues.set(key="schedule_ri_b_count", value=count)

