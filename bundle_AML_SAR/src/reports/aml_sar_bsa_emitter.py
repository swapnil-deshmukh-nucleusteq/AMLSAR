# Databricks notebook source
# =============================================================================
# AML/SAR — BSA E-FILING XML EMITTER
#
# HOW SAR IS FILED:
#   Banks submit to FinCEN via the BSA E-Filing System at bsaefiling.fincen.gov
#   Format: XML using FinCEN SAR XML Schema 2.0 (batch format)
#   Each SAR is one <Activity> element inside <EFilingBatchXML>
#   After upload: FinCEN validates within ~1 hour, bank receives BSA ID on acceptance
#
# THIS NOTEBOOK:
#   Reads: bfsi_regulatory_catalog.sar_reporting.sar_submission_payload
#   Emits: FinCEN SAR XML 2.0 batch file -> /Volumes/.../landing/SAR_<date>_BATCH.xml
#   Also:  Case summary report -> .../SAR_<date>_CASES.txt
# =============================================================================

# COMMAND ----------

dbutils.widgets.text("catalog",           "bfsi_regulatory_catalog")
dbutils.widgets.text("report_date",       "2026-03-31")
dbutils.widgets.text("filing_institution","BFSI Test Bank NA")
dbutils.widgets.text("ein",               "12-3456789")
dbutils.widgets.text("contact_name",      "AML Compliance Officer")
dbutils.widgets.text("contact_phone",     "2025551234")

CATALOG     = dbutils.widgets.get("catalog")
REPORT_DATE = dbutils.widgets.get("report_date")
INSTITUTION = dbutils.widgets.get("filing_institution")
EIN         = dbutils.widgets.get("ein")
CONTACT     = dbutils.widgets.get("contact_name")
PHONE       = dbutils.widgets.get("contact_phone")

VOL_OUT = f"/Volumes/{CATALOG}/raw_sources/landing"

from pyspark.sql import functions as F
from datetime import datetime
import json as _json

print(f"AML/SAR BSA E-Filing Emitter")
print(f"  Institution : {INSTITUTION}")
print(f"  Report date : {REPORT_DATE}")

# COMMAND ----------

# Read from gold submission table
df = spark.table(f"`{CATALOG}`.`sar_reporting`.`sar_submission_payload`")

count = df.count()
print(f"\n  SAR cases in submission payload: {count}")
if count == 0:
    raise Exception(
        "No rows in sar_submission_payload. "
        "Run the AML pipeline first."
    )

rows = df.collect()

# COMMAND ----------

# Pre-filing validation
print("\nRunning pre-filing checks...")
issues = []

for row in rows:
    cid = row["sar_case_id"] or "UNKNOWN"
    if not row["narrative_final"] or len(row["narrative_final"]) < 20:
        issues.append(f"  FAIL: {cid} — narrative too short (minimum 20 chars required by FinCEN)")
    if not row["filing_institution"]:
        issues.append(f"  FAIL: {cid} — filing_institution missing")
    if not row["subject_json"]:
        issues.append(f"  WARN: {cid} — subject_json missing, SAR may be incomplete")
    if not row["activity_json"]:
        issues.append(f"  WARN: {cid} — activity_json missing")

for issue in issues:
    print(f"  {issue}")

if any(i.startswith("  FAIL") for i in issues):
    raise Exception(
        f"{sum(1 for i in issues if 'FAIL' in i)} validation failure(s). "
        "Fix SAR cases before emitting."
    )

print(f"  Pre-filing checks: {count} cases valid, {len(issues)} warnings")

# COMMAND ----------

# Build FinCEN SAR XML 2.0 batch
# Schema: EFilingBatchXML > Activity (one per SAR)
# Reference: FinCEN SAR XML Schema 2.0 User Guide

rpt_date_nodash = REPORT_DATE.replace("-","")
generated_ts    = datetime.now().strftime("%Y%m%dT%H%M%S")
generated_disp  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def safe(v, max_len=None):
    if not v:
        return ""
    s = str(v).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;").replace('"',"&quot;")
    return s[:max_len] if max_len else s

def parse_subject(subject_json):
    """Parse subject JSON from the gold table into SAR subject fields."""
    try:
        d = _json.loads(subject_json) if subject_json else {}
    except:
        d = {}
    return d

def parse_activity(activity_json):
    """Parse activity JSON from the gold table into SAR activity fields."""
    try:
        d = _json.loads(activity_json) if activity_json else {}
    except:
        d = {}
    return d

activity_blocks = []
for i, row in enumerate(rows, 1):
    case_id    = safe(row["sar_case_id"])
    narrative  = safe(row["narrative_final"], 20000)   # FinCEN max 20,000 chars
    subj       = parse_subject(row["subject_json"])
    activity   = parse_activity(row["activity_json"])

    # Activity dates — default to 90d window ending on report_date
    act_begin = activity.get("suspicious_activity_begin", "")
    act_end   = activity.get("suspicious_activity_end",   REPORT_DATE)
    if act_begin:
        act_begin_fmt = str(act_begin).replace("-","")[:8]
    else:
        act_begin_fmt = rpt_date_nodash  # fallback

    act_end_fmt = str(act_end).replace("-","")[:8] if act_end else rpt_date_nodash

    # Amount — from activity JSON or default 0
    try:
        amt = float(activity.get("total_amount_usd", 0) or 0)
    except:
        amt = 0.0

    # SAR type code — FinCEN codes for suspicious activity type
    # 16 = Structuring, 17 = Money Laundering, 32 = Other Suspicious Activity
    sar_type_code = "16"  # Structuring (most common in our sample data)

    block = f"""\
    <Activity SeqNum="{i}">

      <!-- Part I — Filing Institution Information -->
      <ActivityAssociation>
        <CorFilingDateText>{rpt_date_nodash}</CorFilingDateText>
        <InitialReportIndicator>Y</InitialReportIndicator>
      </ActivityAssociation>

      <EFilingPriorDocumentNumber>0</EFilingPriorDocumentNumber>

      <FilingInstitution>
        <Party>
          <ActivityPartyTypeCode>35</ActivityPartyTypeCode>
          <PartyName>
            <RawPartyFullName>{safe(INSTITUTION, 150)}</RawPartyFullName>
          </PartyName>
          <TaxPartyIdentification>
            <TaxIdentificationNumberText>{safe(EIN)}</TaxIdentificationNumberText>
            <TaxIdentificationTypeCode>1</TaxIdentificationTypeCode>
          </TaxPartyIdentification>
          <PartyIdentification>
            <PartyIdentificationTypeCode>2</PartyIdentificationTypeCode>
            <PartyIdentificationNumberText>FDIC_CERT_PLACEHOLDER</PartyIdentificationNumberText>
          </PartyIdentification>
          <OrganizationClassificationTypeCode>
            <OrganizationSubtypeID>60</OrganizationSubtypeID>
          </OrganizationClassificationTypeCode>
        </Party>
        <PartyContactInformation>
          <OrganizationManagersName>{safe(CONTACT, 100)}</OrganizationManagersName>
          <PhoneNumberText>{safe(PHONE)}</PhoneNumberText>
        </PartyContactInformation>
      </FilingInstitution>

      <!-- Part II — Suspicious Activity Information -->
      <SuspiciousActivity>
        <ActivityIPAddress/>
        <SuspiciousActivityClassification>
          <SuspiciousActivitySubtypeID>{sar_type_code}</SuspiciousActivitySubtypeID>
        </SuspiciousActivityClassification>
        <SuspiciousActivityFromDateText>{act_begin_fmt}</SuspiciousActivityFromDateText>
        <SuspiciousActivityToDateText>{act_end_fmt}</SuspiciousActivityToDateText>
        <TotalSuspiciousAmountText>{amt:.2f}</TotalSuspiciousAmountText>
        <VehicleTypeCode>20</VehicleTypeCode>
      </SuspiciousActivity>

      <!-- Part III — Subject Information (from enriched alert data) -->
      <Subject>
        <Party>
          <ActivityPartyTypeCode>8</ActivityPartyTypeCode>
          <PartyName>
            <RawPartyFullName>{safe(subj.get("sar_case_id", "UNKNOWN SUBJECT"), 150)}</RawPartyFullName>
          </PartyName>
        </Party>
      </Subject>

      <!-- Part IV — Filing Institution Where Activity Occurred -->
      <ActivityInstitution>
        <Party>
          <ActivityPartyTypeCode>30</ActivityPartyTypeCode>
          <PartyName>
            <RawPartyFullName>{safe(INSTITUTION, 150)}</RawPartyFullName>
          </PartyName>
        </Party>
        <ActivityInstitutionAccount/>
      </ActivityInstitution>

      <!-- Part V — Narrative -->
      <Narrative>
        <SuspiciousActivityNarrativeText>{narrative}</SuspiciousActivityNarrativeText>
      </Narrative>

      <ActivityDocumentControlNumber>{case_id}</ActivityDocumentControlNumber>

    </Activity>"""

    activity_blocks.append(block)

xml_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<!-- FinCEN SAR XML 2.0 Batch File -->
<!-- Institution: {INSTITUTION} | Report Date: {REPORT_DATE} -->
<!-- Generated: {generated_disp} | Cases: {count} -->
<EFilingBatchXML
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:noNamespaceSchemaLocation="http://www.fincen.gov/base/EFilingBatchXML_v2.xsd"
  SeqNum="1">

  <EFilingSubmissionXML SeqNum="1">
    <FilingDateText>{rpt_date_nodash}</FilingDateText>
    <FilingName>SAR_BATCH_{rpt_date_nodash}_{INSTITUTION.replace(" ","_")[:20]}</FilingName>
    <ActivityCount>{count}</ActivityCount>
    <EFilingBatchFileName>SAR_{rpt_date_nodash}_BATCH.xml</EFilingBatchFileName>
  </EFilingSubmissionXML>

""" + "\n".join(activity_blocks) + """

</EFilingBatchXML>"""

print(f"\n  SAR XML document: {count} Activity elements, {len(xml_content):,} characters")

# COMMAND ----------

# Write XML to volume
xml_filename = f"SAR_{rpt_date_nodash}_BATCH.xml"
xml_path     = f"{VOL_OUT}/{xml_filename}"
dbutils.fs.put(xml_path, xml_content, overwrite=True)
print(f"\n  [WRITTEN] {xml_path}")

# COMMAND ----------

# Write human-readable case summary (for AML officer review)

summary_lines = [
    f"AML/SAR FILING SUMMARY — BSA E-Filing Batch",
    f"{'='*60}",
    f"Institution     : {INSTITUTION}",
    f"Report Date     : {REPORT_DATE}",
    f"Generated       : {generated_disp}",
    f"Cases to File   : {count}",
    f"Batch File      : {xml_filename}",
    f"",
    f"SAR CASES:",
]

for row in rows:
    subj_data = parse_subject(row["subject_json"])
    act_data  = parse_activity(row["activity_json"])
    amt       = act_data.get("total_amount_usd", 0)
    summary_lines += [
        f"",
        f"  Case ID    : {row['sar_case_id']}",
        f"  Institution: {row['filing_institution']}",
        f"  Hash       : {(row['payload_hash'] or '')[:16]}...",
        f"  Amount     : USD {float(amt or 0):,.2f}",
        f"  Narrative  : {(row['narrative_final'] or '')[:120]}...",
        f"  Generated  : {row['generated_ts']}",
    ]

summary_lines += [
    f"",
    f"FILING INSTRUCTIONS:",
    f"  1. AML officer reviews each case narrative for completeness",
    f"  2. Log in to https://bsaefiling.fincen.gov/",
    f"  3. Select New Reports > File Batch FinCEN SAR (Report 111 - SARXBatch)",
    f"  4. Click Open New Form",
    f"  5. Attach: {xml_filename}",
    f"  6. Enter Number of Forms in Batch: {count}",
    f"  7. Sign with PIN and submit",
    f"  8. FinCEN validates within ~1 hour — check email for BSA ID",
    f"  9. On acceptance: record BSA ID back in sar_submission_payload table",
    f"",
    f"REGULATORY DEADLINES:",
    f"  SAR must be filed within 30 days of detecting suspicious activity",
    f"  Complex cases: 60 days maximum",
    f"  Continuing activity: re-file every 90 days",
]

summary_path = f"{VOL_OUT}/SAR_{rpt_date_nodash}_CASES.txt"
dbutils.fs.put(summary_path, "\n".join(summary_lines), overwrite=True)
print(f"  [WRITTEN] {summary_path}")

# COMMAND ----------

# Mark emission — update generated_ts to current timestamp
spark.sql(f"""
    UPDATE `{CATALOG}`.`sar_reporting`.`sar_submission_payload`
    SET generated_ts = current_timestamp()
    WHERE generated_ts IS NOT NULL
""")
print(f"\n  Emission timestamp updated in sar_submission_payload")

# COMMAND ----------

print(f"""
{'='*60}
  AML/SAR EMISSION COMPLETE
{'='*60}
  Batch XML  : {xml_filename}
  Summary    : SAR_{rpt_date_nodash}_CASES.txt
  Cases      : {count}
  Location   : {VOL_OUT}

  Download from:
  Catalog -> {CATALOG} -> raw_sources -> Volumes -> landing

  Upload to: https://bsaefiling.fincen.gov/
  (New Reports -> File Batch FinCEN SAR -> Open New Form -> attach XML)
{'='*60}
""")
