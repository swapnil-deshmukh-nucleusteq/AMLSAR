# Databricks notebook source
# TRANSFORM: aml_features.alert_enriched  |  upstream: tms_alerts, dim_customer, customer_txn_behavior  |  enrich_join

# COMMAND ----------
dbutils.widgets.text("catalog",     "bfsi_regulatory_catalog")
dbutils.widgets.text("report_date", "2026-03-31")
dbutils.widgets.text("table_name",  "alert_enriched")
dbutils.widgets.text("schema_name", "aml_features")

CATALOG     = dbutils.widgets.get("catalog")
REPORT_DATE = dbutils.widgets.get("report_date")
TABLE_NAME  = dbutils.widgets.get("table_name")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
TARGET      = f"`{CATALOG}`.`aml_features`.`alert_enriched`"

from pyspark.sql import functions as F
print(f"Transform: {TARGET}")

# COMMAND ----------
# Transform: enrich_join
# Upstream tables were already created by bootstrap and populated by prior tasks
    df = spark.table('`{CATALOG}`.`raw_sources`.`tms_alerts`')
    _j1 = spark.table('`{CATALOG}`.`conformed`.`dim_customer`')
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
    _cols = [c for c in ['alert_id', 'customer_sk', 'alert_score', 'composite_risk_score', 'is_pep', 'is_sanctioned', 'structuring_flag', 'sar_recommended', 'recommendation_reason'] if c in df.columns]
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
dbutils.jobs.taskValues.set(key="alert_enriched_count", value=count)
