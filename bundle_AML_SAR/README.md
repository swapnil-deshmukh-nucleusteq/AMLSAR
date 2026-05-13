# AML_SAR — Databricks Asset Bundle

## Prerequisites (lineage-first — already done)
1. `generic_bootstrap_notebook.py` — created all UC objects from manifest
2. `register_source_mapping.py`    — populated audit_control.source_mapping
3. `generic_lineage_materializer.py` — registered lineage in UC

## Deploy via workspace web terminal (no local CLI, no Git)

```bash
# 1. Upload bundle zip via UI: Data -> Add Data -> upload bundle_AML_SAR.zip
# 2. Open web terminal: top-right menu -> Web Terminal
unzip /dbfs/FileStore/bundle_AML_SAR.zip -d /tmp/
cd /tmp/bundle_AML_SAR
databricks bundle deploy --target dev
databricks bundle run aml_sar_pipeline
```

## Flow

```
audit_control.source_mapping   <- entry point (connection_string, load_mode)
        |
        v  (parallel bronze tasks)
  raw_sources.core_banking_transactions
  raw_sources.crm_customer_profile
  raw_sources.tms_alerts
  raw_sources.watchlist_screening
        |
        v  (manifest edges define transforms)
  silver tables  ->  gold tables  ->  submission
```

## DQ
31 rules from manifest embedded in src/dq/dq_runner.py.
Critical failures stop the pipeline task.
High severity failures log warnings and continue.
Every output row has dq_passed + dq_flags columns.

## Files
```
databricks.yml
resources/job.yml          10 tasks, serverless, dependency-ordered
src/
  dq/dq_runner.py          Shared DQ library (31 rules)
  ingest/                  4 bronze notebooks (source_mapping driven)
  transform/               6 silver/gold notebooks (manifest edge driven)
```
