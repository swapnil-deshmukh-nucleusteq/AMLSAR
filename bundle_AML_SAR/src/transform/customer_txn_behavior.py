# Databricks notebook source
# TRANSFORM: aml_features.customer_txn_behavior  |  upstream: fact_transactions  |  feature_aggregation

# COMMAND ----------
dbutils.widgets.text("catalog",     "bfsi_regulatory_catalog")
dbutils.widgets.text("report_date", "2026-03-31")
dbutils.widgets.text("table_name",  "customer_txn_behavior")
dbutils.widgets.text("schema_name", "aml_features")

CATALOG     = dbutils.widgets.get("catalog")
REPORT_DATE = dbutils.widgets.get("report_date")
TABLE_NAME  = dbutils.widgets.get("table_name")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
TARGET      = f"`{CATALOG}`.`aml_features`.`customer_txn_behavior`"

from pyspark.sql import functions as F
print(f"Transform: {TARGET}")

# COMMAND ----------
# Transform: feature_aggregation
# Upstream tables were already created by bootstrap and populated by prior tasks
    src       = spark.table('`{CATALOG}`.`conformed`.`fact_transactions`')
    report_dt = F.lit(REPORT_DATE).cast('date')
    df = (src
        .groupBy('customer_id')
        .agg(
            F.sum(F.when(F.datediff(report_dt, F.to_date('txn_date')) <= 30,
                  F.col('txn_amount_usd'))).alias('txn_volume_usd_30d'),
            F.count(F.when(F.datediff(report_dt, F.to_date('txn_date')) <= 30,
                  F.lit(1))).alias('txn_count_30d'),
            F.sum(F.when(F.datediff(report_dt, F.to_date('txn_date')) <= 90,
                  F.col('txn_amount_usd'))).alias('txn_volume_usd_90d'),
            F.count(F.when(F.datediff(report_dt, F.to_date('txn_date')) <= 90,
                  F.lit(1))).alias('txn_count_90d'),
            F.count(F.when(F.col('is_cash') &
                  (F.datediff(report_dt, F.to_date('txn_date')) <= 30),
                  F.lit(1))).alias('cash_txn_count_30d'),
            F.countDistinct(F.when(
                  F.datediff(report_dt, F.to_date('txn_date')) <= 30,
                  F.col('counterparty_id'))).alias('unique_counterparties_30d'),
        )
        .withColumn('feature_date', report_dt)
        .withColumn('structuring_flag',
            (F.col('cash_txn_count_30d') >= 3) &
            (F.col('txn_volume_usd_30d') / F.col('txn_count_30d').cast('double') < 10000))
        .withColumn('rapid_movement_flag', F.lit(False))
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
dbutils.jobs.taskValues.set(key="customer_txn_behavior_count", value=count)
