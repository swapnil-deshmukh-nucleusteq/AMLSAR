# Databricks notebook source
# TRANSFORM: conformed.dim_customer  |  upstream: crm_customer_profile, watchlist_screening  |  conform_scd2, enrich_flag

# COMMAND ----------
dbutils.widgets.text("catalog",     "bfsi_regulatory_catalog")
dbutils.widgets.text("report_date", "2026-03-31")
dbutils.widgets.text("table_name",  "dim_customer")
dbutils.widgets.text("schema_name", "conformed")

CATALOG     = dbutils.widgets.get("catalog")
REPORT_DATE = dbutils.widgets.get("report_date")
TABLE_NAME  = dbutils.widgets.get("table_name")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
TARGET      = f"`{CATALOG}`.`conformed`.`dim_customer`"

from pyspark.sql import functions as F
print(f"Transform: {TARGET}")

# COMMAND ----------
# Transform: conform_scd2, enrich_flag
# Upstream tables were already created by bootstrap and populated by prior tasks
    df = spark.table('`{CATALOG}`.`raw_sources`.`crm_customer_profile`')
    _j1 = spark.table('`{CATALOG}`.`raw_sources`.`watchlist_screening`')
    df = df.join(_j1, on='customer_id', how='left')
    if 'composite_risk_score' in df.columns and 'alert_score' in df.columns:
        df = df.withColumn('composite_risk_score',
            F.col('alert_score') * 0.5 +
            F.when(F.col('risk_rating')=='HIGH',  F.lit(200.0))
             .when(F.col('risk_rating')=='PEP',   F.lit(300.0))
             .when(F.col('risk_rating')=='MEDIUM', F.lit(100.0))
             .otherwise(F.lit(0.0)))
    if 'sar_recommended' in df.columns:
        df = df.withColumn('sar_recommended', F.col('composite_risk_score') >= 750)
    if 'recommendation_reason' in df.columns:
        df = df.withColumn('recommendation_reason',
            F.when(F.col('sar_recommended'), F.lit('COMPOSITE_SCORE_THRESHOLD'))
             .otherwise(F.lit(None).cast('string')))
    _cols = [c for c in ['customer_sk', 'customer_id', 'full_name_masked', 'risk_rating', 'is_pep', 'is_sanctioned', 'kyc_current', 'effective_from', 'effective_to', 'is_current'] if c in df.columns]
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
dbutils.jobs.taskValues.set(key="dim_customer_count", value=count)
