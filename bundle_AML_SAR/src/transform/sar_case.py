# Databricks notebook source
# TRANSFORM: sar_reporting.sar_case  |  upstream: alert_enriched  |  case_creation

# COMMAND ----------
dbutils.widgets.text("catalog",     "bfsi_regulatory_catalog")
dbutils.widgets.text("report_date", "2026-03-31")
dbutils.widgets.text("table_name",  "sar_case")
dbutils.widgets.text("schema_name", "sar_reporting")

CATALOG     = dbutils.widgets.get("catalog")
REPORT_DATE = dbutils.widgets.get("report_date")
TABLE_NAME  = dbutils.widgets.get("table_name")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
TARGET      = f"`{CATALOG}`.`sar_reporting`.`sar_case`"

from pyspark.sql import functions as F
print(f"Transform: {TARGET}")

# COMMAND ----------
# Transform: case_creation
# Upstream tables were already created by bootstrap and populated by prior tasks
    src = spark.table('`{CATALOG}`.`aml_features`.`alert_enriched`')
    df = (src
        .filter(F.col('sar_recommended') == True)
        .withColumn('sar_case_id', F.concat(F.lit('SAR-'), F.col('alert_id')))
        .withColumn('sar_type', F.lit('STRUCTURING_OR_OTHER'))
        .withColumn('suspicious_activity_begin', F.date_sub(F.lit(REPORT_DATE).cast('date'), 90))
        .withColumn('suspicious_activity_end', F.lit(REPORT_DATE).cast('date'))
        .withColumn('total_amount_usd', F.lit(0.0).cast('decimal(18,4)'))
        .withColumn('narrative_text',
            F.concat(F.lit('Suspicious activity - score: '), F.col('alert_score').cast('string')))
        .withColumn('filing_status', F.lit('DRAFT'))
        .withColumn('fincen_bsa_id', F.lit(None).cast('string'))
        .withColumn('submitted_ts',  F.lit(None).cast('timestamp'))
        .withColumn('deadline_ts', F.date_add(F.lit(REPORT_DATE).cast('date'), 30).cast('timestamp'))
    )
    _cols = [c for c in ['sar_case_id', 'alert_id', 'customer_sk', 'sar_type', 'suspicious_activity_begin', 'suspicious_activity_end', 'total_amount_usd', 'narrative_text', 'filing_status', 'fincen_bsa_id', 'submitted_ts', 'deadline_ts'] if c in df.columns]
    df = df.select(_cols)

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
dbutils.jobs.taskValues.set(key="sar_case_count", value=count)
