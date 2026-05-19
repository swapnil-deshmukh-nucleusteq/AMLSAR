# Databricks notebook source
# INGEST: raw_sources.allowance_positions  |  4 DQ rules  |  source_mapping driven

# COMMAND ----------
dbutils.widgets.text("catalog",     "bfsi_regulatory_catalog")
dbutils.widgets.text("report_date", "2026-03-31")
dbutils.widgets.text("table_name",  "allowance_positions")
dbutils.widgets.text("schema_name", "raw_sources")

CATALOG     = dbutils.widgets.get("catalog")
REPORT_DATE = dbutils.widgets.get("report_date")
TABLE_NAME  = dbutils.widgets.get("table_name")
SCHEMA_NAME = dbutils.widgets.get("schema_name")

from pyspark.sql import functions as F

# COMMAND ----------
# Read source mapping — single source of truth for connection details
rows = (
    spark.table(f"`{CATALOG}`.`audit_control`.`source_mapping`")
    .filter(F.col("active_flag") == True)
    .filter(F.col("source_table") == TABLE_NAME)
    .orderBy(F.col("mapped_ts").desc())
    .limit(1).collect()
)
if not rows:
    raise Exception(
        f"No active mapping in source_mapping for source_table='{TABLE_NAME}'. "
        "Run register_source_mapping.py first."
    )

row          = rows[0]
CONN_TYPE    = row["connection_type"]
CONN_STR     = row["connection_string"]
TARGET_FQN   = row["target_fqn"]
LOAD_MODE    = row["load_mode"]
INCR_COL     = row["incremental_col"]
SECRET_SCOPE = row["secret_scope"]

print(f"  source  : {CONN_STR}")
print(f"  target  : {TARGET_FQN}")
print(f"  mode    : {LOAD_MODE}")

# COMMAND ----------
# Read
if CONN_TYPE == "volume":
    df = (spark.read
        .option("header",      "true")
        .option("inferSchema", "true")
        .option("nullValue",   "")
        .csv(CONN_STR))
elif LOAD_MODE == "INCREMENTAL":
    _wm = spark.table(TARGET_FQN).agg(F.max(INCR_COL)).collect()[0][0]
    df = (spark.read.format("jdbc")
        .option("url",      dbutils.secrets.get(SECRET_SCOPE, "jdbc_url"))
        .option("user",     dbutils.secrets.get(SECRET_SCOPE, "jdbc_user"))
        .option("password", dbutils.secrets.get(SECRET_SCOPE, "jdbc_password"))
        .option("dbtable",  f"(SELECT * FROM {schema}.{TABLE_NAME} WHERE {INCR_COL} > '{_wm}') t")
        .load())
else:
    df = (spark.read.format("jdbc")
        .option("url",      dbutils.secrets.get(SECRET_SCOPE, "jdbc_url"))
        .option("user",     dbutils.secrets.get(SECRET_SCOPE, "jdbc_user"))
        .option("password", dbutils.secrets.get(SECRET_SCOPE, "jdbc_password"))
        .option("dbtable",  "raw_sources.allowance_positions")
        .option("fetchsize", "10000")
        .load())

print(f"  read: {df.count()} rows")

# COMMAND ----------
# Type casts
df = (df
    .withColumn("acl_balance_open", F.col("acl_balance_open").cast("decimal(18,4)"))
    .withColumn("acl_balance_close", F.col("acl_balance_close").cast("decimal(18,4)"))
    .withColumn("provision_amount", F.col("provision_amount").cast("decimal(18,4)"))
    .withColumn("charge_offs", F.col("charge_offs").cast("decimal(18,4)"))
    .withColumn("recoveries", F.col("recoveries").cast("decimal(18,4)"))
    .withColumn("balance_date", F.col("balance_date").cast("date"))
    .withColumn("src_load_ts", F.col("src_load_ts").cast("timestamp"))
)

# COMMAND ----------
# DQ — rules from manifest via shared dq_runner
%run ../dq/dq_runner
df = run_dq(df, TABLE_NAME, raise_on_critical=True)

# COMMAND ----------
# Write into the shell table created by bootstrap
write_mode = "append" if LOAD_MODE == "INCREMENTAL" else "overwrite"
(df.write
    .mode(write_mode)
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_FQN))

count = spark.table(TARGET_FQN).count()
print(f"  [OK] {count} rows in {TARGET_FQN}")
dbutils.jobs.taskValues.set(key="allowance_positions_count", value=count)

