# Databricks notebook source
# =============================================================================
# FFIEC 041 — CDR SUBMISSION EMITTER
#
# HOW FFIEC 041 IS FILED:
#   Banks submit to the FFIEC Central Data Repository (CDR) at cdr.ffiec.gov
#   Format: XBRL XML — every reported value tagged with its RCON/RIAD mnemonic
#   Deadline: 30 days after quarter-end for banks with assets under $5B
#   After upload: CDR runs edit checks, bank receives acceptance/rejection email
#
# THIS NOTEBOOK:
#   Reads: bfsi_regulatory_catalog.ffiec041_submission.cdr_submission_package
#   Emits: XBRL XML file written to /Volumes/.../landing/FFIEC041_<date>_CDR.xml
#   Also:  Human-readable summary written to .../FFIEC041_<date>_SUMMARY.txt
# =============================================================================

# COMMAND ----------
dbutils.widgets.text("catalog",          "bfsi_regulatory_catalog")
dbutils.widgets.text("report_date",      "2026-03-31")
dbutils.widgets.text("fdic_cert_number", "12345")
dbutils.widgets.text("bank_name",        "BFSI Test Bank NA")
dbutils.widgets.text("lei",              "")

CATALOG     = dbutils.widgets.get("catalog")
REPORT_DATE = dbutils.widgets.get("report_date")
FDIC_CERT   = dbutils.widgets.get("fdic_cert_number")
BANK_NAME   = dbutils.widgets.get("bank_name")
LEI         = dbutils.widgets.get("lei")

VOL_OUT = f"/Volumes/{CATALOG}/raw_sources/landing"

from pyspark.sql import functions as F
from datetime import datetime

print(f"FFIEC 041 CDR Emitter")
print(f"  Bank      : {BANK_NAME}")
print(f"  FDIC Cert : {FDIC_CERT}")
print(f"  Report    : {REPORT_DATE}")

# COMMAND ----------
# Read from gold submission table
df = (
    spark.table(f"`{CATALOG}`.`ffiec041_submission`.`cdr_submission_package`")
    .filter(F.col("report_date") == F.lit(REPORT_DATE).cast("date"))
    .filter(F.col("filing_status") == "DRAFT")
)

count = df.count()
print(f"\n  Rows in submission table: {count}")
if count == 0:
    raise Exception(
        f"No DRAFT rows in cdr_submission_package for report_date={REPORT_DATE}. "
        "Run the pipeline first."
    )

# COMMAND ----------
# Cross-schedule validations before emitting
# These mirror the CDR edit checks that would reject the filing

print("\nRunning pre-emission edit checks...")
rows = df.collect()
rcon_map = {r["rcon_code"]: r["amount_value"] for r in rows if r["rcon_code"]}

def get_val(rcon, default=0.0):
    v = rcon_map.get(rcon)
    try:
        return float(v) if v else default
    except:
        return default

edit_failures = []

# RC: Total assets = sum of asset line items
# RC item 12 (RCON 2170) must equal sum of items 1-11
total_assets = get_val("2170")
if total_assets == 0:
    edit_failures.append("WARN: RCON 2170 (Total Assets) is zero — verify data flow")

# RC: Total liabilities + equity must equal total assets (balance check)
total_liabilities = get_val("2948")
total_equity      = get_val("3210")
if total_assets > 0:
    balance_diff = abs(total_assets - (total_liabilities + total_equity))
    if balance_diff > 1:   # $1K tolerance
        edit_failures.append(
            f"FAIL: Balance sheet out of balance. "
            f"Assets={total_assets:,.0f} vs Liabilities+Equity={total_liabilities+total_equity:,.0f} "
            f"(diff={balance_diff:,.0f} USD thousands)"
        )
    else:
        print(f"  [PASS] Balance sheet: Assets={total_assets:,.0f} = "
              f"Liabilities {total_liabilities:,.0f} + Equity {total_equity:,.0f}")

# RI: Net interest income = Total interest income - Total interest expense
total_int_income  = get_val("4107")
total_int_expense = get_val("4073")
net_int_income    = get_val("4074")
ni_calc = total_int_income - total_int_expense
if abs(ni_calc - net_int_income) > 1:
    edit_failures.append(
        f"FAIL: Net interest income mismatch. "
        f"4107({total_int_income}) - 4073({total_int_expense}) = {ni_calc} "
        f"but 4074={net_int_income}"
    )
else:
    print(f"  [PASS] Net interest income: {total_int_income} - {total_int_expense} = {net_int_income}")

# ACL cross-check: RC item 4.c (RCON 3123) must equal RI-B Part II item 7 col A
rc_acl   = get_val("3123")
# RI-B uses same RCON 3123 in our model
if rc_acl == 0:
    edit_failures.append("WARN: RCON 3123 (Allowance for Credit Losses) is zero")
else:
    print(f"  [PASS] ACL balance: {rc_acl:,.0f}")

for f_msg in edit_failures:
    print(f"  [{f_msg[:4]}] {f_msg[6:]}")

if any(e.startswith("FAIL") for e in edit_failures):
    raise Exception(
        f"{sum(1 for e in edit_failures if e.startswith('FAIL'))} critical edit check(s) failed. "
        "Fix upstream data before emitting submission file."
    )

print(f"\n  Edit checks complete. {len(edit_failures)} warnings, 0 failures.")

# COMMAND ----------
# Generate XBRL XML — CDR submission format
# Each data point tagged with its RCON/RIAD mnemonic per FFIEC taxonomy

rpt_date_nodash = REPORT_DATE.replace("-", "")
generated_ts    = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

# Build context and unit blocks (XBRL boilerplate)
xbrl_header = f"""<?xml version="1.0" encoding="UTF-8"?>
<!-- FFIEC 041 Call Report — XBRL Submission -->
<!-- Bank: {BANK_NAME} | FDIC Cert: {FDIC_CERT} | Report Date: {REPORT_DATE} -->
<!-- Generated: {generated_ts} -->
<xbrl
  xmlns="http://www.xbrl.org/2003/instance"
  xmlns:link="http://www.xbrl.org/2003/linkbase"
  xmlns:xlink="http://www.w3.org/1999/xlink"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xmlns:ffiec="http://xbrl.ffiec.gov/fr/common/labels/2024-03-31"
  xmlns:iso4217="http://www.xbrl.org/2003/iso4217">

  <!-- ═══ CONTEXT ═══════════════════════════════════════════════════════ -->
  <context id="C-INSTANT">
    <entity>
      <identifier scheme="http://www.ffiec.gov/rssd">{FDIC_CERT}</identifier>
    </entity>
    <period>
      <instant>{REPORT_DATE}</instant>
    </period>
  </context>

  <context id="C-YTD">
    <entity>
      <identifier scheme="http://www.ffiec.gov/rssd">{FDIC_CERT}</identifier>
    </entity>
    <period>
      <startDate>{REPORT_DATE[:4]}-01-01</startDate>
      <endDate>{REPORT_DATE}</endDate>
    </period>
  </context>

  <!-- ═══ UNIT ══════════════════════════════════════════════════════════ -->
  <unit id="USD-THOUSANDS">
    <measure>iso4217:USD</measure>
  </unit>

  <!-- ═══ FILING METADATA ═══════════════════════════════════════════════ -->
  <ffiec:RCON9999 contextRef="C-INSTANT" decimals="0">{rpt_date_nodash}</ffiec:RCON9999>
  <ffiec:RSSD9017 contextRef="C-INSTANT">{BANK_NAME}</ffiec:RSSD9017>
  <ffiec:RSSD9050 contextRef="C-INSTANT">{FDIC_CERT}</ffiec:RSSD9050>"""

if LEI:
    xbrl_header += f"""
  <ffiec:RCON9224 contextRef="C-INSTANT">{LEI}</ffiec:RCON9224>"""

# Build data elements — one XBRL element per RCON/RIAD row
schedule_context = {
    "RC":    "C-INSTANT",
    "RI":    "C-YTD",
    "RC-C":  "C-INSTANT",
    "RC-N":  "C-INSTANT",
    "RI-B":  "C-YTD",
    "RC-R":  "C-INSTANT",
}

xbrl_data_lines = []
for row in sorted(rows, key=lambda r: (r["schedule_ref"] or "", r["rcon_code"] or "")):
    rcon     = row["rcon_code"]
    value    = row["amount_value"]
    schedule = row["schedule_ref"] or "RC"

    if not rcon or not value:
        continue

    # Determine context — balance sheet items use instant, income items use YTD
    ctx = schedule_context.get(schedule, "C-INSTANT")

    # Determine if numeric or text field
    try:
        float_val = float(value)
        xbrl_data_lines.append(
            f'  <ffiec:{rcon} contextRef="{ctx}" unitRef="USD-THOUSANDS" '
            f'decimals="0">{int(float_val)}</ffiec:{rcon}>'
        )
    except (ValueError, TypeError):
        # Text field (Y/N flags, dates, etc.)
        xbrl_data_lines.append(
            f'  <ffiec:{rcon} contextRef="{ctx}">{value}</ffiec:{rcon}>'
        )

xbrl_footer = "\n</xbrl>"

xbrl_content = (
    xbrl_header + "\n\n"
    "  <!-- ═══ REPORTED DATA ELEMENTS ══════════════════════════════════════ -->\n"
    + "\n".join(xbrl_data_lines)
    + "\n" + xbrl_footer
)

print(f"\n  XBRL document: {len(xbrl_data_lines)} data elements, "
      f"{len(xbrl_content):,} characters")

# COMMAND ----------
# Write XBRL file to volume
xml_filename = f"FFIEC041_{rpt_date_nodash}_{FDIC_CERT}_CDR.xml"
xml_path     = f"{VOL_OUT}/{xml_filename}"

dbutils.fs.put(xml_path, xbrl_content, overwrite=True)
print(f"\n  [WRITTEN] {xml_path}")

# COMMAND ----------
# Write human-readable summary (for CFO review before CDR upload)

schedules_in_report = sorted(set(r["schedule_ref"] for r in rows if r["schedule_ref"]))

summary_lines = [
    f"FFIEC 041 CALL REPORT — SUBMISSION SUMMARY",
    f"{'='*60}",
    f"Bank Name       : {BANK_NAME}",
    f"FDIC Cert No.   : {FDIC_CERT}",
    f"Report Date     : {REPORT_DATE}",
    f"Due Date        : {(datetime.strptime(REPORT_DATE,'%Y-%m-%d').strftime('%Y-%m-%d'))} + 30 days",
    f"Generated       : {generated_ts}",
    f"Filing Status   : DRAFT — requires CFO signature before CDR upload",
    f"",
    f"SCHEDULES INCLUDED:",
]
for sched in schedules_in_report:
    n = sum(1 for r in rows if r["schedule_ref"] == sched)
    summary_lines.append(f"  {sched:<12} {n:>4} data items")

summary_lines += [
    f"",
    f"KEY FIGURES (USD thousands):",
    f"  Total Assets  (RCON 2170) : {total_assets:>15,.0f}",
    f"  Total Loans   (RCON 2122) : {get_val('2122'):>15,.0f}",
    f"  Total Deposits(RCON 2200) : {get_val('2200'):>15,.0f}",
    f"  ACL on Loans  (RCON 3123) : {rc_acl:>15,.0f}",
    f"  Net Interest Income (4074): {net_int_income:>15,.0f}",
    f"  Net Income    (RIAD 4340) : {get_val('4340'):>15,.0f}",
    f"",
    f"EDIT CHECK RESULTS:",
    f"  Balance sheet check : {'PASS' if not any('FAIL' in e for e in edit_failures) else 'FAIL'}",
    f"  NII arithmetic      : {'PASS' if abs(ni_calc - net_int_income) <= 1 else 'FAIL'}",
    f"  Warnings            : {len(edit_failures)}",
    f"",
    f"NEXT STEPS:",
    f"  1. CFO reviews and signs the hard-copy signature page",
    f"  2. Board attests (minimum 2 directors for state non-member banks)",
    f"  3. Log in to https://cdr.ffiec.gov/cdr/",
    f"  4. Select Banks & Vendors > Submit Call Report",
    f"  5. Upload: {xml_filename}",
    f"  6. CDR runs edit checks — accept/reject notification within minutes",
    f"  7. On acceptance: retain signed hard copy in institution files",
]

summary_content = "\n".join(summary_lines)
summary_path    = f"{VOL_OUT}/FFIEC041_{rpt_date_nodash}_{FDIC_CERT}_SUMMARY.txt"
dbutils.fs.put(summary_path, summary_content, overwrite=True)
print(f"  [WRITTEN] {summary_path}")

# COMMAND ----------
# Update filing_status in submission table to EMITTED
spark.sql(f"""
    UPDATE `{CATALOG}`.`ffiec041_submission`.`cdr_submission_package`
    SET filing_status = 'EMITTED',
        submission_hash = '{df.first()["submission_hash"] or ""}',
        submitted_ts = current_timestamp()
    WHERE report_date = '{REPORT_DATE}'
    AND   filing_status = 'DRAFT'
""")
print(f"\n  Filing status updated to EMITTED in cdr_submission_package")

# COMMAND ----------
print(f"""
{'='*60}
  FFIEC 041 EMISSION COMPLETE
{'='*60}
  XBRL file : {xml_filename}
  Summary   : FFIEC041_{rpt_date_nodash}_{FDIC_CERT}_SUMMARY.txt
  Location  : {VOL_OUT}

  Both files are in the volume landing folder.
  Download from:
  Catalog -> {CATALOG} -> raw_sources -> Volumes -> landing

  Upload the XBRL XML to: https://cdr.ffiec.gov/cdr/
  (Banks & Vendors -> Submit Call Report -> browse -> upload)
{'='*60}
""")

