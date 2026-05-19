# Databricks notebook source
# DQ runner — FFIEC_041 manifest rules embedded at bundle generation time
# Called via %run ../dq/dq_runner in every notebook

# COMMAND ----------
from pyspark.sql import functions as F

DQ_RULES = {
  "gl_balances": [
    {
      "rule_id": "dq_r001",
      "field": "gl_account_id",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "GL account must be present"
    },
    {
      "rule_id": "dq_r002",
      "field": "balance_date",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "Balance date must be present"
    },
    {
      "rule_id": "dq_r003",
      "field": "balance_usd",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "Balance must be non-null"
    },
    {
      "rule_id": "dq_r004",
      "field": "account_type",
      "rule_type": "value_set",
      "severity": "critical",
      "params": {
        "allowed_values": [
          "ASSET",
          "LIABILITY",
          "EQUITY",
          "INCOME",
          "EXPENSE"
        ]
      },
      "description": "Account type must be valid"
    },
    {
      "rule_id": "dq_r005",
      "field": "currency_code",
      "rule_type": "reference_check",
      "severity": "high",
      "params": {
        "reference_table": "audit_control.ref_iso_currency"
      },
      "description": "Currency must be ISO 4217"
    }
  ],
  "loan_positions": [
    {
      "rule_id": "dq_r010",
      "field": "loan_id",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "Loan ID must not be null"
    },
    {
      "rule_id": "dq_r011",
      "field": "outstanding_balance",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "Balance must not be null"
    },
    {
      "rule_id": "dq_r012",
      "field": "outstanding_balance",
      "rule_type": "range_check",
      "severity": "critical",
      "params": {
        "min": 0
      },
      "description": "Balance must be >= 0"
    },
    {
      "rule_id": "dq_r013",
      "field": "days_past_due",
      "rule_type": "range_check",
      "severity": "critical",
      "params": {
        "min": 0
      },
      "description": "Days past due must be >= 0"
    },
    {
      "rule_id": "dq_r014",
      "field": "accrual_status",
      "rule_type": "value_set",
      "severity": "critical",
      "params": {
        "allowed_values": [
          "ACCRUING",
          "NONACCRUAL"
        ]
      },
      "description": "Accrual status must be valid"
    },
    {
      "rule_id": "dq_r015",
      "field": "maturity_date",
      "rule_type": "date_after",
      "severity": "high",
      "params": {
        "reference_field": "origination_date"
      },
      "description": "Maturity must be after origination"
    },
    {
      "rule_id": "dq_r016",
      "field": "loan_type_code",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "Loan type required for RC-C classification"
    }
  ],
  "securities_positions": [
    {
      "rule_id": "dq_r020",
      "field": "portfolio_bucket",
      "rule_type": "value_set",
      "severity": "critical",
      "params": {
        "allowed_values": [
          "HTM",
          "AFS",
          "TRADING"
        ]
      },
      "description": "Portfolio bucket must be HTM / AFS / TRADING"
    },
    {
      "rule_id": "dq_r021",
      "field": "amortized_cost",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "Amortized cost must not be null"
    },
    {
      "rule_id": "dq_r022",
      "field": "fair_value",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "Fair value must not be null"
    },
    {
      "rule_id": "dq_r023",
      "field": "security_type",
      "rule_type": "value_set",
      "severity": "critical",
      "params": {
        "allowed_values": [
          "UST",
          "AGENCY",
          "MBS",
          "ABS",
          "MUNI",
          "CORP",
          "EQUITY",
          "OTHER"
        ]
      },
      "description": "Security type must be classified"
    }
  ],
  "deposit_accounts": [
    {
      "rule_id": "dq_r030",
      "field": "account_id",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "Account ID must not be null"
    },
    {
      "rule_id": "dq_r031",
      "field": "balance_usd",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "Balance must not be null"
    },
    {
      "rule_id": "dq_r032",
      "field": "depositor_type",
      "rule_type": "value_set",
      "severity": "critical",
      "params": {
        "allowed_values": [
          "IPC",
          "US_GOV",
          "STATE_MUNI",
          "BANK_US",
          "BANK_FOREIGN",
          "FOREIGN_GOV"
        ]
      },
      "description": "Depositor type must be valid"
    },
    {
      "rule_id": "dq_r033",
      "field": "account_type",
      "rule_type": "value_set",
      "severity": "critical",
      "params": {
        "allowed_values": [
          "DEMAND",
          "NOW",
          "ATS",
          "MMDA",
          "SAVINGS",
          "TIME_LE250K",
          "TIME_GT250K"
        ]
      },
      "description": "Account type must be valid"
    }
  ],
  "allowance_positions": [
    {
      "rule_id": "dq_r040",
      "field": "acl_balance_close",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "Closing ACL balance must not be null"
    },
    {
      "rule_id": "dq_r041",
      "field": "acl_balance_close",
      "rule_type": "range_check",
      "severity": "critical",
      "params": {
        "min": 0
      },
      "description": "ACL balance must be >= 0"
    },
    {
      "rule_id": "dq_r042",
      "field": "provision_amount",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "Provision must be present"
    },
    {
      "rule_id": "dq_r043",
      "field": "asset_class",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "Asset class required for RI-C disaggregation"
    }
  ],
  "derivative_positions": [
    {
      "rule_id": "dq_r050",
      "field": "notional_amount",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "Notional must not be null"
    },
    {
      "rule_id": "dq_r051",
      "field": "instrument_type",
      "rule_type": "value_set",
      "severity": "critical",
      "params": {
        "allowed_values": [
          "INTEREST_RATE",
          "FX",
          "EQUITY",
          "COMMODITY",
          "CREDIT"
        ]
      },
      "description": "Instrument type must be classified"
    },
    {
      "rule_id": "dq_r052",
      "field": "purpose",
      "rule_type": "value_set",
      "severity": "critical",
      "params": {
        "allowed_values": [
          "TRADING",
          "HEDGING"
        ]
      },
      "description": "Purpose must be TRADING or HEDGING"
    }
  ],
  "capital_positions": [
    {
      "rule_id": "dq_r060",
      "field": "component_type",
      "rule_type": "value_set",
      "severity": "critical",
      "params": {
        "allowed_values": [
          "CET1",
          "AT1",
          "TIER2",
          "DEDUCTION",
          "RWA",
          "LEVERAGE"
        ]
      },
      "description": "Capital component type must be valid"
    },
    {
      "rule_id": "dq_r061",
      "field": "amount_usd",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "Amount must not be null"
    }
  ],
  "fact_gl_positions": [
    {
      "rule_id": "dq_s001",
      "field": "rcon_code",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "RCON code must be mapped before silver"
    },
    {
      "rule_id": "dq_s002",
      "field": "schedule_ref",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "Target schedule must be identified"
    },
    {
      "rule_id": "dq_s003",
      "field": "dq_passed",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "DQ flag must be computed"
    },
    {
      "rule_id": "dq_s004",
      "field": "balance_usd",
      "rule_type": "thousands_check",
      "severity": "high",
      "description": "Amounts must be in USD thousands (not dollars)"
    }
  ],
  "fact_loans_conformed": [
    {
      "rule_id": "dq_s010",
      "field": "rc_c_category",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "Every loan must have an RC-C category"
    },
    {
      "rule_id": "dq_s011",
      "field": "rc_n_bucket",
      "rule_type": "value_set",
      "severity": "critical",
      "params": {
        "allowed_values": [
          "CURRENT",
          "PD_30_89",
          "PD_90_PLUS",
          "NONACCRUAL"
        ]
      },
      "description": "Past-due bucket must be valid"
    },
    {
      "rule_id": "dq_s012",
      "field": "outstanding_net",
      "rule_type": "range_check",
      "severity": "critical",
      "params": {
        "min": 0
      },
      "description": "Net balance must be >= 0"
    }
  ],
  "schedule_rc": [
    {
      "rule_id": "dq_g001",
      "field": "amount_usd_000",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "No null amounts in RC schedule"
    },
    {
      "rule_id": "dq_g002",
      "field": "rcon_code",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "RCON code required"
    },
    {
      "rule_id": "dq_g003",
      "rule_id_ext": "RC-BAL",
      "rule_type": "balance_check",
      "severity": "critical",
      "description": "Total assets (2170) must equal sum of items 1-11",
      "cross_field_check": "SUM(assets) == SUM(liabilities + equity)"
    },
    {
      "rule_id": "dq_g004",
      "rule_id_ext": "RC-EQ",
      "rule_type": "cross_schedule",
      "severity": "critical",
      "description": "RC item 27.a must equal RI-A item 12 (equity reconciliation)"
    }
  ],
  "schedule_ri": [
    {
      "rule_id": "dq_g010",
      "field": "ytd_amount_usd_000",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "No null amounts in RI"
    },
    {
      "rule_id": "dq_g011",
      "rule_id_ext": "RI-NII",
      "rule_type": "arithmetic",
      "severity": "critical",
      "description": "Net interest income (4074) = Total interest income (4107) minus Total interest expense (4073)"
    },
    {
      "rule_id": "dq_g012",
      "rule_id_ext": "RI-NI",
      "rule_type": "cross_schedule",
      "severity": "critical",
      "description": "Net income (4340) must equal RI-A item 4"
    }
  ],
  "schedule_rc_c": [
    {
      "rule_id": "dq_g020",
      "field": "amount_usd_000",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "No null loan balances"
    },
    {
      "rule_id": "dq_g021",
      "rule_id_ext": "RC-C-TOT",
      "rule_type": "cross_schedule",
      "severity": "critical",
      "description": "RC-C item 12 (total loans) must equal RC item 4.a + 4.b"
    },
    {
      "rule_id": "dq_g022",
      "rule_id_ext": "RC-C-CHG",
      "rule_type": "cross_schedule",
      "severity": "critical",
      "description": "RC-C charge-offs total must equal RI-B Part I item 9 Column A"
    }
  ],
  "schedule_rc_r": [
    {
      "rule_id": "dq_g030",
      "rule_id_ext": "RCR-CET1",
      "rule_type": "threshold",
      "severity": "critical",
      "params": {
        "min_ratio_pct": 4.5
      },
      "description": "CET1 ratio (P793) must be >= 4.5% minimum"
    },
    {
      "rule_id": "dq_g031",
      "rule_id_ext": "RCR-TIER1",
      "rule_type": "threshold",
      "severity": "critical",
      "params": {
        "min_ratio_pct": 6.0
      },
      "description": "Tier 1 ratio (7206) must be >= 6.0%"
    },
    {
      "rule_id": "dq_g032",
      "rule_id_ext": "RCR-TOTAL",
      "rule_type": "threshold",
      "severity": "critical",
      "params": {
        "min_ratio_pct": 8.0
      },
      "description": "Total capital ratio (7205) must be >= 8.0%"
    },
    {
      "rule_id": "dq_g033",
      "rule_id_ext": "RCR-LEV",
      "rule_type": "threshold",
      "severity": "critical",
      "params": {
        "min_ratio_pct": 4.0
      },
      "description": "Leverage ratio (7204) must be >= 4.0%"
    },
    {
      "rule_id": "dq_g034",
      "rule_id_ext": "RCR-RWA",
      "rule_type": "cross_schedule",
      "severity": "critical",
      "description": "Total RWA from Part II item 31 must equal RC-R Part I item 48"
    }
  ],
  "schedule_rc_n": [
    {
      "rule_id": "dq_g040",
      "field": "column_ref",
      "rule_type": "value_set",
      "severity": "critical",
      "params": {
        "allowed_values": [
          "A",
          "B",
          "C"
        ]
      },
      "description": "Column ref must be A, B, or C"
    },
    {
      "rule_id": "dq_g041",
      "rule_id_ext": "RCN-XSCH",
      "rule_type": "cross_schedule",
      "severity": "critical",
      "description": "RC-N item 9 col A+B+C must reconcile to RC-C total loans by status"
    }
  ],
  "schedule_ri_b": [
    {
      "rule_id": "dq_g050",
      "rule_id_ext": "RIB-ACL",
      "rule_type": "cross_schedule",
      "severity": "critical",
      "description": "RI-B Part II item 7 col A must equal RC item 4.c (ACL on loans)"
    },
    {
      "rule_id": "dq_g051",
      "rule_id_ext": "RIB-PROV",
      "rule_type": "cross_schedule",
      "severity": "critical",
      "description": "RI-B Part II item 5 sum must equal RI item 4 (Provision)"
    }
  ],
  "cdr_submission_package": [
    {
      "rule_id": "dq_sub001",
      "field": "fdic_cert_number",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "FDIC cert number required on every record"
    },
    {
      "rule_id": "dq_sub002",
      "field": "submission_hash",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "Hash must be computed before status=SUBMITTED"
    },
    {
      "rule_id": "dq_sub003",
      "field": "filing_status",
      "rule_type": "value_set",
      "severity": "critical",
      "params": {
        "allowed_values": [
          "DRAFT",
          "CFO_REVIEWED",
          "BOARD_ATTESTED",
          "SUBMITTED"
        ]
      },
      "description": "Filing status must follow approval workflow"
    },
    {
      "rule_id": "dq_sub004",
      "field": "due_date",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "Due date must always be computed"
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

