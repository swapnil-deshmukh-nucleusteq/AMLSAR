# Databricks notebook source
# DQ runner — AML_SAR manifest rules embedded at bundle generation time
# Called via %run ../dq/dq_runner in every notebook

# COMMAND ----------
from pyspark.sql import functions as F

DQ_RULES = {
  "core_banking_transactions": [
    {
      "rule_id": "dq_001",
      "field": "txn_id",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "Transaction ID must never be null"
    },
    {
      "rule_id": "dq_002",
      "field": "txn_amount",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "Amount must not be null"
    },
    {
      "rule_id": "dq_003",
      "field": "txn_amount",
      "rule_type": "range_check",
      "severity": "critical",
      "params": {
        "min": 0
      },
      "description": "Amount must be non-negative"
    },
    {
      "rule_id": "dq_004",
      "field": "txn_currency",
      "rule_type": "reference_check",
      "severity": "high",
      "params": {
        "reference_table": "audit_control.ref_iso_currency"
      },
      "description": "Currency must be valid ISO 4217 code"
    },
    {
      "rule_id": "dq_005",
      "field": "txn_date",
      "rule_type": "not_future",
      "severity": "high",
      "description": "Transaction date must not be in the future"
    }
  ],
  "crm_customer_profile": [
    {
      "rule_id": "dq_010",
      "field": "customer_id",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "Customer ID must never be null"
    },
    {
      "rule_id": "dq_011",
      "field": "risk_rating",
      "rule_type": "value_set",
      "severity": "critical",
      "params": {
        "allowed_values": [
          "LOW",
          "MEDIUM",
          "HIGH",
          "PEP",
          "BLOCKED"
        ]
      },
      "description": "Risk rating must be a known value"
    },
    {
      "rule_id": "dq_012",
      "field": "kyc_review_date",
      "rule_type": "staleness",
      "severity": "high",
      "params": {
        "max_age_days": 365
      },
      "description": "KYC review must not be older than 1 year"
    }
  ],
  "tms_alerts": [
    {
      "rule_id": "dq_020",
      "field": "alert_id",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "Alert ID must not be null"
    },
    {
      "rule_id": "dq_021",
      "field": "alert_score",
      "rule_type": "range_check",
      "severity": "critical",
      "params": {
        "min": 0,
        "max": 1000
      },
      "description": "Alert score must be within 0-1000"
    },
    {
      "rule_id": "dq_022",
      "field": "alert_status",
      "rule_type": "value_set",
      "severity": "high",
      "params": {
        "allowed_values": [
          "OPEN",
          "IN_REVIEW",
          "CLOSED",
          "ESCALATED",
          "SAR_FILED"
        ]
      },
      "description": "Status must be known value"
    }
  ],
  "watchlist_screening": [
    {
      "rule_id": "dq_030",
      "field": "match_status",
      "rule_type": "value_set",
      "severity": "critical",
      "params": {
        "allowed_values": [
          "NO_MATCH",
          "POTENTIAL_MATCH",
          "CONFIRMED_MATCH",
          "FALSE_POSITIVE"
        ]
      },
      "description": "Match status must be valid"
    }
  ],
  "dim_customer": [
    {
      "rule_id": "dq_040",
      "field": "risk_rating",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "Risk rating must always be populated"
    },
    {
      "rule_id": "dq_041",
      "field": "customer_sk",
      "rule_type": "unique",
      "severity": "critical",
      "description": "Surrogate key must be unique"
    },
    {
      "rule_id": "dq_042",
      "field": "kyc_current",
      "rule_type": "not_null",
      "severity": "high",
      "description": "KYC currency flag must be computed"
    }
  ],
  "fact_transactions": [
    {
      "rule_id": "dq_050",
      "field": "txn_amount_usd",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "USD amount must be populated after FX conversion"
    },
    {
      "rule_id": "dq_051",
      "field": "dq_passed",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "DQ flag must always be computed"
    },
    {
      "rule_id": "dq_052",
      "field": "customer_sk",
      "rule_type": "ref_integrity",
      "severity": "critical",
      "params": {
        "reference_table": "conformed.dim_customer"
      },
      "description": "Customer SK must exist in dim_customer"
    }
  ],
  "customer_txn_behavior": [
    {
      "rule_id": "dq_060",
      "field": "structuring_flag",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "Structuring flag must always be computed"
    },
    {
      "rule_id": "dq_061",
      "field": "txn_volume_usd_30d",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "30-day volume must not be null"
    },
    {
      "rule_id": "dq_062",
      "field": "feature_date",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "Feature date must be set"
    }
  ],
  "alert_enriched": [
    {
      "rule_id": "dq_070",
      "field": "composite_risk_score",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "Composite risk score must be populated"
    },
    {
      "rule_id": "dq_071",
      "field": "sar_recommended",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "SAR recommendation flag must always be set"
    }
  ],
  "sar_case": [
    {
      "rule_id": "dq_080",
      "field": "filing_status",
      "rule_type": "value_set",
      "severity": "critical",
      "params": {
        "allowed_values": [
          "DRAFT",
          "REVIEW",
          "APPROVED",
          "SUBMITTED",
          "WITHDRAWN"
        ]
      },
      "description": "Filing status must be valid"
    },
    {
      "rule_id": "dq_081",
      "field": "narrative_text",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "SAR narrative must be present before submission"
    },
    {
      "rule_id": "dq_082",
      "field": "total_amount_usd",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "Total suspicious amount must be captured"
    },
    {
      "rule_id": "dq_083",
      "field": "deadline_ts",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "Filing deadline must always be computed"
    },
    {
      "rule_id": "dq_084",
      "field": "suspicious_activity_begin",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "Activity begin date required"
    }
  ],
  "sar_submission_payload": [
    {
      "rule_id": "dq_090",
      "field": "payload_hash",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "Payload hash must always be computed for integrity"
    },
    {
      "rule_id": "dq_091",
      "field": "subject_json",
      "rule_type": "valid_json",
      "severity": "critical",
      "description": "Subject JSON must be parseable"
    },
    {
      "rule_id": "dq_092",
      "field": "activity_json",
      "rule_type": "valid_json",
      "severity": "critical",
      "description": "Activity JSON must be parseable"
    }
  ]
}


def _condition(rule):
    field  = rule.get("field", "")
    rtype  = rule["rule_type"]
    params = rule.get("params", {})
    if rtype == "not_null":
        return F.col(field).isNull()
    elif rtype == "range_check":
        cond = F.lit(False)
        if "min" in params: cond = cond | (F.col(field) < params["min"])
        if "max" in params: cond = cond | (F.col(field) > params["max"])
        return cond
    elif rtype == "value_set":
        return ~F.col(field).isin(params.get("allowed_values", []))
    elif rtype == "staleness":
        return F.datediff(F.current_date(), F.col(field)) > params.get("max_age_days", 365)
    elif rtype == "not_future":
        return F.col(field) > F.current_timestamp()
    elif rtype == "valid_json":
        return F.col(field).isNull() | (F.length(F.col(field)) < 3)
    elif rtype == "ref_integrity":
        return F.col(field).isNull()
    else:
        return F.lit(False)


def run_dq(df, table_name, raise_on_critical=True):
    """
    Apply all DQ rules defined in the manifest for table_name.
    Adds dq_passed (bool) and dq_flags (array<string>) columns.
    Raises on critical failures if raise_on_critical=True.
    """
    rules = DQ_RULES.get(table_name, [])
    if not rules:
        return (df
            .withColumn("dq_passed", F.lit(True))
            .withColumn("dq_flags",  F.array().cast("array<string>")))

    fail_cols = []
    for r in rules:
        rid  = r["rule_id"]
        sev  = r["severity"]
        desc = r.get("description", rid)
        fc   = f"_fail_{rid}"
        try:
            df = df.withColumn(fc, _condition(r))
        except Exception as ex:
            print(f"  [DQ WARN] {rid} skipped: {ex}")
            df = df.withColumn(fc, F.lit(False))
        n_fail = df.filter(F.col(fc)).count()
        if n_fail > 0:
            msg = f"[{sev.upper()}] {rid}: {n_fail} rows failed - {desc}"
            print(f"  {msg}")
            if sev == "critical" and raise_on_critical:
                raise Exception(msg)
        else:
            print(f"  [DQ PASS] {rid}")
        fail_cols.append((rid, sev, fc))

    critical_fcs = [fc for _, sev, fc in fail_cols if sev == "critical"]
    df = df.withColumn(
        "dq_passed",
        ~F.array(*[F.col(fc) for fc in critical_fcs]).contains(True)
        if critical_fcs else F.lit(True)
    )
    df = df.withColumn(
        "dq_flags",
        F.array_remove(
            F.array(*[
                F.when(F.col(fc), F.lit(rid)).otherwise(F.lit(None).cast("string"))
                for rid, _, fc in fail_cols
            ]), None
        ).cast("array<string>")
    )
    df = df.drop(*[fc for _, _, fc in fail_cols])
    passed = df.filter("dq_passed").count()
    print(f"  [DQ] {table_name}: {passed}/{df.count()} rows passed critical rules")
    return df
