# Databricks notebook source
# TRANSFORM: conformed.fact_transactions  |  upstream: core_banking_transactions  |  cleanse_normalize

# COMMAND ----------
dbutils.widgets.text("catalog",     "bfsi_regulatory_catalog")
dbutils.widgets.text("report_date", "2026-03-31")
dbutils.widgets.text("table_name",  "fact_transactions")
dbutils.widgets.text("schema_name", "conformed")

CATALOG     = dbutils.widgets.get("catalog")
REPORT_DATE = dbutils.widgets.get("report_date")
TABLE_NAME  = dbutils.widgets.get("table_name")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
TARGET      = f"`{CATALOG}`.`conformed`.`fact_transactions`"

from pyspark.sql import functions as F
print(f"Transform: {TARGET}")

# COMMAND ----------
# Transform: cleanse_normalize
# Upstream tables were already created by bootstrap and populated by prior tasks
src = spark.table(f'`{CATALOG}`.`raw_sources`.`core_banking_transactions`')
df = (src
    .withColumn('txn_amount_usd',
        F.when(F.col('txn_currency')=='USD', F.col('txn_amount'))
         .otherwise(F.col('txn_amount')))
    .withColumn('is_cash', F.col('txn_type').isin('CASH_DEPOSIT','CASH_WITHDRAWAL'))
    .withColumn('ctr_eligible', F.col('is_cash') & (F.col('txn_amount_usd') >= 10000))
    .withColumn('report_date', F.lit(REPORT_DATE).cast('date'))
)
_cols = [c for c in ['txn_sk', 'txn_id', 'customer_id', 'customer_sk', 'txn_date', 'txn_amount_usd', 'txn_type', 'is_cash', 'ctr_eligible', 'counterparty_id', 'dq_passed', 'dq_flags'] if c in df.columns]
df = df.select(_cols) if _cols else df

# COMMAND ----------
# Write into existing table (schema created by bootstrap)
(df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET))

count = spark.table(TARGET).count()
print(f"  [OK] {count} rows in {TARGET}")
dbutils.jobs.taskValues.set(key="fact_transactions_count", value=count)

