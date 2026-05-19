# FFIEC_041 — Databricks Asset Bundle

## Prerequisites (lineage-first — already done)
1. `generic_bootstrap_notebook.py` — created all UC objects from manifest
2. `register_source_mapping.py`    — populated audit_control.source_mapping
3. `generic_lineage_materializer.py` — registered lineage in UC

## Deploy via workspace web terminal (no local CLI, no Git)

```bash
# 1. Upload bundle zip via UI: Data -> Add Data -> upload bundle_FFIEC_041.zip
# 2. Open web terminal: top-right menu -> Web Terminal
unzip /dbfs/FileStore/bundle_FFIEC_041.zip -d /tmp/
cd /tmp/bundle_FFIEC_041
databricks bundle deploy --target dev
databricks bundle run ffiec_041_pipeline
```

## Flow

```
audit_control.source_mapping   <- entry point (connection_string, load_mode)
        |
        v  (parallel bronze tasks)
  raw_sources.gl_balances
  raw_sources.loan_positions
  raw_sources.securities_positions
  raw_sources.deposit_accounts
  raw_sources.allowance_positions
  raw_sources.derivative_positions
  raw_sources.capital_positions
        |
        v  (manifest edges define transforms)
  silver tables  ->  gold tables  ->  submission
```

## DQ
59 rules from manifest embedded in src/dq/dq_runner.py.
Critical failures stop the pipeline task.
High severity failures log warnings and continue.
Every output row has dq_passed + dq_flags columns.

## Files
```
databricks.yml
resources/job.yml          16 tasks, serverless, dependency-ordered
src/
  dq/dq_runner.py          Shared DQ library (59 rules)
  ingest/                  7 bronze notebooks (source_mapping driven)
  transform/               9 silver/gold notebooks (manifest edge driven)
```
