# Databricks notebook source
# TRANSFORM: sar_reporting.sar_submission_payload  |  upstream: sar_case  |  payload_render

# COMMAND ----------
dbutils.widgets.text("catalog",     "bfsi_regulatory_catalog")
dbutils.widgets.text("report_date", "2026-03-31")
dbutils.widgets.text("table_name",  "sar_submission_payload")
dbutils.widgets.text("schema_name", "sar_reporting")

CATALOG     = dbutils.widgets.get("catalog")
REPORT_DATE = dbutils.widgets.get("report_date")
TABLE_NAME  = dbutils.widgets.get("table_name")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
TARGET      = f"`{CATALOG}`.`sar_reporting`.`sar_submission_payload`"

from pyspark.sql import functions as F
print(f"Transform: {TARGET}")

# COMMAND ----------
# Transform: payload_render
# Upstream tables were already created by bootstrap and populated by prior tasks
    src = spark.table('`{CATALOG}`.`sar_reporting`.`sar_case`')
    df = (src
        .filter(F.col('filing_status').isin('DRAFT','APPROVED'))
        .withColumn('filing_institution', F.lit('BFSI_BANK_001'))
        .withColumn('subject_json', F.to_json(F.struct('sar_case_id','sar_type')))
        .withColumn('activity_json', F.to_json(F.struct('total_amount_usd','suspicious_activity_begin','suspicious_activity_end')))
        .withColumn('narrative_final',  F.col('narrative_text'))
        .withColumn('xml_payload',      F.lit(None).cast('string'))
        .withColumn('payload_hash',     F.sha2(F.col('narrative_text'), 256))
        .withColumn('generated_ts',     F.current_timestamp())
    )
    _cols = [c for c in ['sar_case_id', 'filing_institution', 'subject_json', 'activity_json', 'narrative_final', 'xml_payload', 'payload_hash', 'generated_ts'] if c in df.columns]
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
dbutils.jobs.taskValues.set(key="sar_submission_payload_count", value=count)
