# Databricks notebook source
# TRANSFORM: conformed.fact_gl_positions  |  upstream: gl_balances  |  cleanse_map_rcon

# COMMAND ----------
dbutils.widgets.text("catalog",     "bfsi_regulatory_catalog")
dbutils.widgets.text("report_date", "2026-03-31")
dbutils.widgets.text("table_name",  "fact_gl_positions")
dbutils.widgets.text("schema_name", "conformed")

CATALOG     = dbutils.widgets.get("catalog")
REPORT_DATE = dbutils.widgets.get("report_date")
TABLE_NAME  = dbutils.widgets.get("table_name")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
TARGET      = f"`{CATALOG}`.`conformed`.`fact_gl_positions`"

from pyspark.sql import functions as F
print(f"Transform: {TARGET}")

# COMMAND ----------
# Transform: cleanse_map_rcon
# Upstream tables were already created by bootstrap and populated by prior tasks
    src = spark.table('`{CATALOG}`.`raw_sources`.`gl_balances`')
    df = (src
    .withColumn('schedule_ref',
        F.when(F.col('account_type').isin('INCOME','EXPENSE'), F.lit('RI'))
         .otherwise(F.lit('RC')))
    .withColumn('line_item_ref', F.col('rcon_code'))
    .withColumn('report_date', F.lit(REPORT_DATE).cast('date'))
    )
    _cols = [c for c in ['gl_sk', 'gl_account_id', 'rcon_code', 'schedule_ref', 'line_item_ref', 'balance_usd', 'ytd_amount_usd', 'report_date', 'dq_passed', 'dq_flags'] if c in src.columns]
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
dbutils.jobs.taskValues.set(key="fact_gl_positions_count", value=count)

