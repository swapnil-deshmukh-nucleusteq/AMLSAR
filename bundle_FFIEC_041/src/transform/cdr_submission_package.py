# Databricks notebook source
# TRANSFORM: ffiec041_submission.cdr_submission_package  |  upstream: schedule_rc, schedule_ri, schedule_rc_c, schedule_rc_r, schedule_rc_n, schedule_ri_b  |  flatten_submission

# COMMAND ----------
dbutils.widgets.text("catalog",     "bfsi_regulatory_catalog")
dbutils.widgets.text("report_date", "2026-03-31")
dbutils.widgets.text("table_name",  "cdr_submission_package")
dbutils.widgets.text("schema_name", "ffiec041_submission")

CATALOG     = dbutils.widgets.get("catalog")
REPORT_DATE = dbutils.widgets.get("report_date")
TABLE_NAME  = dbutils.widgets.get("table_name")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
TARGET      = f"`{CATALOG}`.`ffiec041_submission`.`cdr_submission_package`"

from pyspark.sql import functions as F
print(f"Transform: {TARGET}")

# COMMAND ----------
# Transform: flatten_submission
# Upstream tables were already created by bootstrap and populated by prior tasks
    _sql = '''SELECT `report_date`, `rcon_code`, CAST(NULL AS STRING) AS `submission_id`, CAST(NULL AS STRING) AS `fdic_cert_number`, CAST(NULL AS STRING) AS `lei`, CAST(NULL AS STRING) AS `amount_value`, CAST(NULL AS STRING) AS `schedule_ref`, CAST(NULL AS STRING) AS `filing_status`, CAST(NULL AS STRING) AS `submission_hash`, CAST(NULL AS STRING) AS `cdr_bsa_id`, CAST(NULL AS STRING) AS `submitted_ts`, CAST(NULL AS STRING) AS `due_date` FROM `{CATALOG}`.`ffiec041_schedules`.`schedule_rc`\nUNION ALL\nSELECT `report_date`, CAST(NULL AS STRING) AS `submission_id`, CAST(NULL AS STRING) AS `fdic_cert_number`, CAST(NULL AS STRING) AS `lei`, CAST(NULL AS STRING) AS `rcon_code`, CAST(NULL AS STRING) AS `amount_value`, CAST(NULL AS STRING) AS `schedule_ref`, CAST(NULL AS STRING) AS `filing_status`, CAST(NULL AS STRING) AS `submission_hash`, CAST(NULL AS STRING) AS `cdr_bsa_id`, CAST(NULL AS STRING) AS `submitted_ts`, CAST(NULL AS STRING) AS `due_date` FROM `{CATALOG}`.`ffiec041_schedules`.`schedule_ri`\nUNION ALL\nSELECT `report_date`, `rcon_code`, CAST(NULL AS STRING) AS `submission_id`, CAST(NULL AS STRING) AS `fdic_cert_number`, CAST(NULL AS STRING) AS `lei`, CAST(NULL AS STRING) AS `amount_value`, CAST(NULL AS STRING) AS `schedule_ref`, CAST(NULL AS STRING) AS `filing_status`, CAST(NULL AS STRING) AS `submission_hash`, CAST(NULL AS STRING) AS `cdr_bsa_id`, CAST(NULL AS STRING) AS `submitted_ts`, CAST(NULL AS STRING) AS `due_date` FROM `{CATALOG}`.`ffiec041_schedules`.`schedule_rc_c`\nUNION ALL\nSELECT `report_date`, CAST(NULL AS STRING) AS `submission_id`, CAST(NULL AS STRING) AS `fdic_cert_number`, CAST(NULL AS STRING) AS `lei`, CAST(NULL AS STRING) AS `rcon_code`, CAST(NULL AS STRING) AS `amount_value`, CAST(NULL AS STRING) AS `schedule_ref`, CAST(NULL AS STRING) AS `filing_status`, CAST(NULL AS STRING) AS `submission_hash`, CAST(NULL AS STRING) AS `cdr_bsa_id`, CAST(NULL AS STRING) AS `submitted_ts`, CAST(NULL AS STRING) AS `due_date` FROM `{CATALOG}`.`ffiec041_schedules`.`schedule_rc_r`\nUNION ALL\nSELECT `report_date`, `rcon_code`, CAST(NULL AS STRING) AS `submission_id`, CAST(NULL AS STRING) AS `fdic_cert_number`, CAST(NULL AS STRING) AS `lei`, CAST(NULL AS STRING) AS `amount_value`, CAST(NULL AS STRING) AS `schedule_ref`, CAST(NULL AS STRING) AS `filing_status`, CAST(NULL AS STRING) AS `submission_hash`, CAST(NULL AS STRING) AS `cdr_bsa_id`, CAST(NULL AS STRING) AS `submitted_ts`, CAST(NULL AS STRING) AS `due_date` FROM `{CATALOG}`.`ffiec041_schedules`.`schedule_rc_n`\nUNION ALL\nSELECT `report_date`, CAST(NULL AS STRING) AS `submission_id`, CAST(NULL AS STRING) AS `fdic_cert_number`, CAST(NULL AS STRING) AS `lei`, CAST(NULL AS STRING) AS `rcon_code`, CAST(NULL AS STRING) AS `amount_value`, CAST(NULL AS STRING) AS `schedule_ref`, CAST(NULL AS STRING) AS `filing_status`, CAST(NULL AS STRING) AS `submission_hash`, CAST(NULL AS STRING) AS `cdr_bsa_id`, CAST(NULL AS STRING) AS `submitted_ts`, CAST(NULL AS STRING) AS `due_date` FROM `{CATALOG}`.`ffiec041_schedules`.`schedule_ri_b`'''
    df = spark.sql(_sql)
    df = (df
        .withColumn('filing_status', F.lit('DRAFT'))
        .withColumn('due_date', F.date_add(F.lit(REPORT_DATE).cast('date'), 30))
    )

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
dbutils.jobs.taskValues.set(key="cdr_submission_package_count", value=count)

