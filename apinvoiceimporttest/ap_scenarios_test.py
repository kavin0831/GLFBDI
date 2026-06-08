"""
AP Invoice FBDI -- 6-Scenario Live Integration Test
====================================================
Runs 6 data scenarios against Oracle Fusion and reports actual import results
including rejection details from BIP XML / ESS logs.

Scenarios
---------
  1  VALID_USD        -- 2 clean USD invoices (ABC Consulting / ABC US1)
                        Expected: SUCCEEDED, 2 invoices created
  2  DUPLICATE        -- same invoice numbers as scenario 1 re-submitted
                        Expected: WARNING/SUCCEEDED but invoices REJECTED (duplicate)
  3  INVALID_RATE     -- EUR invoice with Conversion Rate = -1 (invalid negative)
                        Expected: Oracle rejects on bad conversion rate
  4  BAD_SUPPLIER     -- completely made-up supplier name & site
                        Expected: Oracle rejects (supplier not found)
  5  CLOSED_PERIOD    -- accounting date in a closed GL period (Jan-2018) and a
                        non-existent future period (Dec-2099)
                        Expected: Oracle rejects (accounting period not open)
  6  BAD_DIST         -- fake GL account segments + non-existent distribution set name
                        Expected: Oracle rejects (invalid distribution combination/set)

Environment variables (required)
---------------------------------
  FUSION_URL      https://fa-xxxx-saasfademo1.oracledemos.com
  FUSION_USER     Kavin.Sasikumar   (or whichever Fusion user)
  FUSION_PASSWORD ********
  AP_BU_ID        300000046987012   (Business Unit ID)
  AP_LEDGER_ID    300000046975971   (Ledger ID)

Optional overrides (have safe defaults from the sample data)
-------------------------------------------------------------
  VALID_SUPPLIER_NAME    ABC Consulting
  VALID_SUPPLIER_NUM     1288
  VALID_SUPPLIER_SITE    ABC US1
  VALID_BU_NAME          US1 Business Unit
  VALID_LEGAL_ENTITY     US1 Legal Entity
  VALID_DIST_SET       Advertising (Full)
  ACCOUNTING_DATE        2026/03/02

Run
---
  python ap_scenarios_test.py
  python ap_scenarios_test.py --dry-run      # build ZIPs, skip Oracle submission
  python ap_scenarios_test.py --cases 1,3    # run only selected case numbers
"""

from __future__ import annotations

import argparse
import base64
import csv
import io
import json
import os
import sys
import time
import uuid
import zipfile
from datetime import datetime
from pathlib import Path
from typing import NamedTuple

try:
    import httpx
except ImportError:
    print("ERROR: httpx not installed.  Run: pip install httpx")
    sys.exit(1)

# ?? Credentials & Config (from env) ??????????????????????????????????????????

FUSION_URL   = os.environ.get("FUSION_URL", "").rstrip("/")
USERNAME     = os.environ.get("FUSION_USER", "")
PASSWORD     = os.environ.get("FUSION_PASSWORD", "")
AP_BU_ID     = os.environ.get("AP_BU_ID", "")
AP_LEDGER_ID = os.environ.get("AP_LEDGER_ID", "")

# Defaults extracted from the project's sample ApInvoicesInterface.csv
VALID_SUPPLIER_NAME  = os.environ.get("VALID_SUPPLIER_NAME",  "ABC Consulting")
VALID_SUPPLIER_NUM   = os.environ.get("VALID_SUPPLIER_NUM",   "1288")
VALID_SUPPLIER_SITE  = os.environ.get("VALID_SUPPLIER_SITE",  "ABC US1")
VALID_BU_NAME        = os.environ.get("VALID_BU_NAME",        "US1 Business Unit")
VALID_LEGAL_ENTITY   = os.environ.get("VALID_LEGAL_ENTITY",   "US1 Legal Entity")
VALID_DIST_SET       = os.environ.get("VALID_DIST_SET",       "Advertising (Full)")
ACCOUNTING_DATE      = os.environ.get("ACCOUNTING_DATE",      "2026/03/02")   # YYYY/MM/DD

AP_SOURCE   = "External"
AP_JOB_NAME = "oracle/apps/ess/financials/payables/invoices/transactions,APXIIMPT"
AP_DOC_ACCT = "fin$/payables$/import$"
ERPI        = "/fscmRestApi/resources/11.13.18.05/erpintegrations"

OUT_DIR = Path(__file__).parent

# Unique run token -- shared across scenarios so invoice numbers don't collide between runs
RUN_TOKEN = uuid.uuid4().hex[:8].upper()


# ?? Column helpers ????????????????????????????????????????????????????????????
# Import column lists from the app generator so we never get out of sync.
APP_ROOT = Path(__file__).resolve().parent.parent
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

try:
    from utils.ap_fbdi_generator import HDR_DATA_COLS, LINE_DATA_COLS
except ImportError:
    print("WARNING: Could not import ap_fbdi_generator -- using embedded column lists.")
    # Minimal fallback (exact column names from the sample CSV header row)
    HDR_DATA_COLS = None   # will be built from sample file
    LINE_DATA_COLS = None


def _hdr_empty() -> dict:
    return {c: "" for c in HDR_DATA_COLS}


def _line_empty() -> dict:
    return {c: "" for c in LINE_DATA_COLS}


def _fmt(d: str) -> str:
    """Ensure YYYY/MM/DD -- handles YYYY-MM-DD and DD-MM-YYYY too."""
    d = (d or "").strip()
    for fmt in ("%Y/%m/%d", "%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y",
                "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(d, fmt).strftime("%Y/%m/%d")
        except ValueError:
            pass
    return d


# ?? FBDI row builders ?????????????????????????????????????????????????????????

def _make_header(inv_id: str, inv_num: str, amount: str, inv_date: str,
                 supplier: str, sup_num: str, site: str,
                 currency: str = "USD",
                 import_set: str = "",
                 conv_rate_type: str = "",
                 conv_rate: str = "",
                 description: str = "",
                 acct_date: str = "",       # override ACCOUNTING_DATE per-invoice
                 inv_type: str = "STANDARD",
                 payment_terms: str = "Immediate") -> dict:
    h = _hdr_empty()
    h["*Invoice ID"]        = inv_id
    h["*Business Unit"]     = VALID_BU_NAME
    h["*Source"]            = AP_SOURCE
    h["*Invoice Number"]    = inv_num
    h["*Invoice Amount"]    = amount
    h["*Invoice Date"]      = _fmt(inv_date)
    h["**Supplier Name"]    = supplier
    h["**Supplier Number"]  = sup_num
    h["*Supplier Site"]     = site
    h["Invoice Currency"]   = currency
    h["Payment Currency"]   = currency
    h["Description"]        = description
    h["Import Set"]         = import_set
    h["*Invoice Type"]      = inv_type
    h["Legal Entity"]       = VALID_LEGAL_ENTITY
    h["*Payment Terms"]     = payment_terms
    h["Terms Date"]         = _fmt(inv_date)
    h["Accounting Date"]    = _fmt(acct_date or ACCOUNTING_DATE)
    h["Payment Method"]     = "CHECK"
    h["Pay Group"]          = "Standard"
    h["Calculate Tax During Import"] = "N"
    if currency not in ("USD", ""):
        h["Conversion Rate Type"] = conv_rate_type or "Corporate"
        h["Conversion Date"]      = _fmt(acct_date or ACCOUNTING_DATE)
        h["Conversion Rate"]      = conv_rate
    return h


def _make_line(inv_id: str, line_num: int, amount: str,
               dist_combo: str = "", dist_set: str = "",
               description: str = "",
               acct_date: str = "") -> dict:    # override ACCOUNTING_DATE per-line
    ln = _line_empty()
    ln["*Invoice ID"]    = inv_id
    ln["Line Number"]    = str(line_num)
    ln["*Line Type"]     = "ITEM"
    ln["*Amount"]        = amount
    ln["Description"]    = description
    ln["Final Match"]    = "N"
    ln["Distribution Combination"] = dist_combo
    ln["Distribution Set"]         = dist_set
    ln["Accounting Date"]          = _fmt(acct_date or ACCOUNTING_DATE)
    ln["Prorate Across All Item Lines"] = "N"
    return ln


# ?? CSV/ZIP builder ???????????????????????????????????????????????????????????

def _build_zip(headers: list[dict], lines: list[dict],
               zip_path: Path) -> None:
    """Write headerless FBDI CSVs into a ZIP, exactly as Oracle expects."""
    hdr_buf = io.StringIO()
    hw = csv.writer(hdr_buf, quoting=csv.QUOTE_MINIMAL)
    for h in headers:
        row = [h.get(c, "") for c in HDR_DATA_COLS]
        row.append("END")
        hw.writerow(row)

    line_buf = io.StringIO()
    lw = csv.writer(line_buf, quoting=csv.QUOTE_MINIMAL)
    for ln in lines:
        row = [ln.get(c, "") for c in LINE_DATA_COLS]
        row.append("END")
        lw.writerow(row)

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("ApInvoicesInterface.csv",     hdr_buf.getvalue())
        zf.writestr("ApInvoiceLinesInterface.csv", line_buf.getvalue())


# ?? Oracle submission & polling ???????????????????????????????????????????????

def submit(zip_path: Path, invoice_group: str, acct_date: str) -> dict:
    b64 = base64.b64encode(zip_path.read_bytes()).decode()
    param_list = (
        f"#NULL,{AP_BU_ID},N,{acct_date},#NULL,#NULL,1000,"
        f"{AP_SOURCE},{invoice_group},N,N,{AP_LEDGER_ID},#NULL,1"
    )
    payload = {
        "OperationName":    "importBulkData",
        "DocumentContent":  b64,
        "ContentType":      "zip",
        "FileName":         zip_path.name,
        "DocumentAccount":  AP_DOC_ACCT,
        "JobName":          AP_JOB_NAME,
        "ParameterList":    param_list,
        "CallbackURL":      "#NULL",
        "NotificationCode": "10",
        # PurgeOption=N keeps rejected rows available for BIP report inspection
        "JobOptions": "InterfaceDetails=1,ImportOption=Y,PurgeOption=N,ExtractFileType=ALL",
    }
    r = httpx.post(
        FUSION_URL + ERPI, json=payload,
        auth=(USERNAME, PASSWORD), timeout=120,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    r.raise_for_status()
    return r.json()


def poll_status(req_id: str, max_polls: int = 30, interval: int = 15) -> str:
    """Poll ESSJobStatusRF until a terminal status is reached."""
    auth = (USERNAME, PASSWORD)
    terminal = {"SUCCEEDED", "ERROR", "WARNING", "CANCELLED", "BLOCKED"}
    for i in range(1, max_polls + 1):
        r = httpx.get(
            FUSION_URL + ERPI,
            params={"finder": f"ESSJobStatusRF;requestId={req_id}"},
            auth=auth, timeout=30,
            headers={"Accept": "application/json"},
        )
        items = r.json().get("items", [])
        st = (items[0].get("RequestStatus") or "PENDING").upper() if items else "PENDING"
        print(f"        poll {i:2d}: {st}")
        if st in terminal:
            return st
        time.sleep(interval)
    return "TIMEOUT"


def get_child_jobs(parent_id: str) -> list[dict]:
    """Fetch child ESS jobs from ESSExecutionDetailsRF."""
    r = httpx.get(
        FUSION_URL + ERPI,
        params={"finder": f"ESSExecutionDetailsRF;requestId={parent_id}"},
        auth=(USERNAME, PASSWORD), timeout=30,
        headers={"Accept": "application/json"},
    )
    items = r.json().get("items", [])
    if not items:
        return []
    raw = items[0].get("RequestStatus") or ""
    try:
        parsed = json.loads(raw)
    except Exception:
        return []

    flat: list[dict] = []

    def walk(node):
        if isinstance(node, dict):
            if node.get("JOBNAME") or node.get("REQUESTID"):
                flat.append({
                    "name":       node.get("JOBNAME", ""),
                    "request_id": str(node.get("REQUESTID", "")),
                    "status":     (node.get("STATUS") or "").upper(),
                })
            for key in ("CHILD", "JOBS"):
                if key in node:
                    walk(node[key])
        elif isinstance(node, list):
            for x in node:
                walk(x)

    walk(parsed)
    return flat


def download_log_text(req_id: str) -> str:
    """Try to download ESS log ZIP and extract all text content."""
    import zipfile as _zf, io as _io
    url = f"{FUSION_URL}/ess/rest/scheduler/v1/requests/{req_id}/log"
    try:
        r = httpx.get(url, auth=(USERNAME, PASSWORD), timeout=60,
                       headers={"Accept": "application/zip, */*"})
        if r.status_code != 200:
            return ""
        try:
            with _zf.ZipFile(_io.BytesIO(r.content)) as z:
                parts = []
                for name in z.namelist():
                    try:
                        parts.append(f"--- {name} ---\n{z.read(name).decode('utf-8','replace')}")
                    except Exception:
                        pass
                return "\n\n".join(parts)
        except Exception:
            return r.content.decode("utf-8", "replace")
    except Exception:
        return ""


BIP_REPORT_PATH = "/Financials/Payables/Invoices/ImportPayablesInvoices.xdo"
BIP_SOAP_URL    = "/xmlpserver/services/v2/ReportService"


def fetch_bip_xml_direct(req_id: str) -> bytes:
    """
    Direct BIP SOAP runReport call (XML output format) for the AP Invoice Import report.
    Does NOT go through the app's service layer.
    Returns decoded XML bytes, or b"" on failure.
    """
    import re, base64 as _b64
    envelope = f"""<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
               xmlns:v2="http://xmlns.oracle.com/oxp/service/v2">
  <soap:Body>
    <v2:runReport>
      <v2:reportRequest>
        <v2:attributeFormat>xml</v2:attributeFormat>
        <v2:reportAbsolutePath>{BIP_REPORT_PATH}</v2:reportAbsolutePath>
        <v2:parameterNameValues>
          <v2:listOfParamNameValues>
            <v2:item>
              <v2:name>P_REQUEST_ID</v2:name>
              <v2:values><v2:item>{req_id}</v2:item></v2:values>
            </v2:item>
          </v2:listOfParamNameValues>
        </v2:parameterNameValues>
        <v2:sizeOfDataChunkDownload>-1</v2:sizeOfDataChunkDownload>
      </v2:reportRequest>
      <v2:userID>{USERNAME}</v2:userID>
      <v2:password>{PASSWORD}</v2:password>
    </v2:runReport>
  </soap:Body>
</soap:Envelope>"""
    url = FUSION_URL + BIP_SOAP_URL
    try:
        r = httpx.post(url, content=envelope.encode("utf-8"),
                       auth=(USERNAME, PASSWORD), timeout=120,
                       headers={"Content-Type": "text/xml;charset=UTF-8",
                                "SOAPAction": ""})
        if r.status_code != 200:
            print(f"    BIP SOAP HTTP {r.status_code}: {r.text[:120]}")
            return b""
        m = re.search(r"<(?:\w+:)?reportBytes>([^<]+)</(?:\w+:)?reportBytes>", r.text)
        if not m:
            print(f"    BIP SOAP: no reportBytes in response: {r.text[:120]}")
            return b""
        raw = _b64.b64decode(m.group(1))
        return raw
    except Exception as exc:
        print(f"    BIP SOAP error: {exc}")
        return b""


def extract_xml_from_log_zip(log_bytes: bytes) -> bytes:
    """
    Oracle stores the BIP data XML inside the ESS log ZIP.
    Extract the first .xml file found; skip log/out files.
    """
    if not log_bytes:
        return b""
    try:
        import zipfile as _zf, io as _io
        with _zf.ZipFile(_io.BytesIO(log_bytes)) as z:
            for name in z.namelist():
                if name.lower().endswith(".xml"):
                    data = z.read(name)
                    # Only return XML that looks like the report (not a SOAP envelope)
                    if b"APXIIMPT" in data or b"G_REJECTIONS" in data or b"INVOICES" in data:
                        return data
    except Exception:
        pass
    return b""


def download_log_zip_bytes(req_id: str) -> bytes:
    """Download raw ESS log ZIP bytes."""
    url = f"{FUSION_URL}/ess/rest/scheduler/v1/requests/{req_id}/log"
    try:
        r = httpx.get(url, auth=(USERNAME, PASSWORD), timeout=60,
                       headers={"Accept": "application/zip, */*"})
        if r.status_code == 200:
            return r.content
    except Exception:
        pass
    return b""


def parse_bip_xml(xml_bytes: bytes) -> dict:
    """Parse BIP XML for invoice counts + rejection details."""
    if not xml_bytes:
        return {}
    try:
        from services.fusion_service import analyze_ap_bip_xml
        return analyze_ap_bip_xml(xml_bytes)
    except Exception:
        # Fallback: manual parse of key count tags
        import re
        text = xml_bytes.decode("utf-8", "replace")
        def _i(tag):
            m = re.search(rf"<{tag}[^>]*>(\d+)</{tag}>", text)
            return int(m.group(1)) if m else 0
        fetched  = _i("C_INVOICES_FETCHED")
        created  = _i("C_INVOICES_CREATED")
        rejected = _i("C_INVOICES_REJECTED")
        has_rej  = rejected > 0 or (fetched > 0 and created < fetched)
        return {"fetched": fetched, "created": created, "rejected": rejected,
                "has_rejections": has_rej, "rejections": []}


def analyze_log(log_text: str) -> dict:
    """Parse ESS log for GL-style errors (used for the APXIIMPT load step)."""
    try:
        from services.fusion_service import analyze_ess_logs
        return analyze_ess_logs({"all_text": log_text})
    except Exception:
        return {}


# ?? Scenario definitions ??????????????????????????????????????????????????????

class Scenario(NamedTuple):
    num:              int
    key:              str
    description:      str
    expected:         str
    build:            object  # callable() -> (headers, lines)
    exp_created:      int = -1   # -1 = don't check; >=0 = exact match
    exp_rejected:     int = -1   # -1 = don't check; >=0 = exact match


def _inv_id(base_num: int, offset: int) -> str:
    """Unique, deterministic Invoice ID per scenario + run."""
    return str(100000000 + (base_num * 1000) + offset)


def _inv_num(key: str, offset: int) -> str:
    return f"TEST-{RUN_TOKEN}-{key}-{offset:03d}"


# ?? Case 1: Two valid USD invoices ????????????????????????????????????????????

def _build_case1():
    """2 x valid USD invoices -- full amounts, real supplier, real dist account."""
    inv_date = "2026/03/02"
    hdrs, lns = [], []

    # Invoice A: 1000.00 with two 500.00 lines
    iid_a = _inv_id(1, 1)
    hdrs.append(_make_header(iid_a, _inv_num("C1", 1), "1000.00", inv_date,
                              VALID_SUPPLIER_NAME, VALID_SUPPLIER_NUM,
                              VALID_SUPPLIER_SITE,
                              import_set=f"SCEN1_{RUN_TOKEN}",
                              description="Case 1 -- valid USD invoice A"))
    lns.append(_make_line(iid_a, 1, "500.00", dist_set=VALID_DIST_SET, description="Line 1"))
    lns.append(_make_line(iid_a, 2, "500.00", dist_set=VALID_DIST_SET, description="Line 2"))

    # Invoice B: 250.00 single line
    iid_b = _inv_id(1, 2)
    hdrs.append(_make_header(iid_b, _inv_num("C1", 2), "250.00", inv_date,
                              VALID_SUPPLIER_NAME, VALID_SUPPLIER_NUM,
                              VALID_SUPPLIER_SITE,
                              import_set=f"SCEN1_{RUN_TOKEN}",
                              description="Case 1 -- valid USD invoice B"))
    lns.append(_make_line(iid_b, 1, "250.00", dist_set=VALID_DIST_SET, description="Line 1"))

    return hdrs, lns


# ?? Case 2: Duplicate invoice numbers (same as Case 1) ???????????????????????

def _build_case2():
    """Same invoice numbers as Case 1 -- Oracle should reject as duplicates."""
    inv_date = "2026/03/02"
    hdrs, lns = [], []

    iid_a = _inv_id(2, 1)
    hdrs.append(_make_header(iid_a, _inv_num("C1", 1), "1000.00", inv_date,
                              VALID_SUPPLIER_NAME, VALID_SUPPLIER_NUM,
                              VALID_SUPPLIER_SITE,
                              import_set=f"SCEN2_{RUN_TOKEN}",
                              description="Case 2 -- duplicate of Case 1 invoice A"))
    lns.append(_make_line(iid_a, 1, "500.00", dist_set=VALID_DIST_SET))
    lns.append(_make_line(iid_a, 2, "500.00", dist_set=VALID_DIST_SET))

    iid_b = _inv_id(2, 2)
    hdrs.append(_make_header(iid_b, _inv_num("C1", 2), "250.00", inv_date,
                              VALID_SUPPLIER_NAME, VALID_SUPPLIER_NUM,
                              VALID_SUPPLIER_SITE,
                              import_set=f"SCEN2_{RUN_TOKEN}",
                              description="Case 2 -- duplicate of Case 1 invoice B"))
    lns.append(_make_line(iid_b, 1, "250.00", dist_set=VALID_DIST_SET))

    return hdrs, lns


# ?? Case 3: Invalid conversion rate ??????????????????????????????????????????

def _build_case3():
    """EUR invoices with a negative conversion rate -- Oracle must reject."""
    inv_date = "2026/03/02"
    hdrs, lns = [], []

    # Sub-case A: rate = -1 (negative -- clearly invalid)
    iid_a = _inv_id(3, 1)
    hdrs.append(_make_header(iid_a, _inv_num("C3", 1), "800.00", inv_date,
                              VALID_SUPPLIER_NAME, VALID_SUPPLIER_NUM,
                              VALID_SUPPLIER_SITE,
                              currency="EUR",
                              conv_rate_type="Corporate",
                              conv_rate="-1",
                              import_set=f"SCEN3_{RUN_TOKEN}",
                              description="Case 3A -- EUR invoice with rate = -1"))
    lns.append(_make_line(iid_a, 1, "800.00", dist_set=VALID_DIST_SET,
                           description="EUR line -- bad rate"))

    # Sub-case B: rate = 0 (zero -- Oracle cannot divide by zero for conversion)
    iid_b = _inv_id(3, 2)
    hdrs.append(_make_header(iid_b, _inv_num("C3", 2), "500.00", inv_date,
                              VALID_SUPPLIER_NAME, VALID_SUPPLIER_NUM,
                              VALID_SUPPLIER_SITE,
                              currency="EUR",
                              conv_rate_type="Corporate",
                              conv_rate="0",
                              import_set=f"SCEN3_{RUN_TOKEN}",
                              description="Case 3B -- EUR invoice with rate = 0"))
    lns.append(_make_line(iid_b, 1, "500.00", dist_set=VALID_DIST_SET,
                           description="EUR line -- zero rate"))

    return hdrs, lns


# ?? Case 4: Non-existent supplier ?????????????????????????????????????????????

def _build_case4():
    """Supplier name / site completely made up -- Oracle cannot resolve it."""
    inv_date = "2026/03/02"
    hdrs, lns = [], []

    iid_a = _inv_id(4, 1)
    hdrs.append(_make_header(iid_a, _inv_num("C4", 1), "300.00", inv_date,
                              supplier="NONEXISTENT_VENDOR_XYZ_99",
                              sup_num="99999999",
                              site="FAKE_SITE_001",
                              import_set=f"SCEN4_{RUN_TOKEN}",
                              description="Case 4A -- fake supplier name+site"))
    lns.append(_make_line(iid_a, 1, "300.00", dist_set=VALID_DIST_SET,
                           description="Line for fake supplier"))

    # Sub-case B: real supplier, wrong site
    iid_b = _inv_id(4, 2)
    hdrs.append(_make_header(iid_b, _inv_num("C4", 2), "150.00", inv_date,
                              supplier=VALID_SUPPLIER_NAME,
                              sup_num=VALID_SUPPLIER_NUM,
                              site="FAKE_SITE_DOESNT_EXIST",
                              import_set=f"SCEN4_{RUN_TOKEN}",
                              description="Case 4B -- real supplier but invalid site code"))
    lns.append(_make_line(iid_b, 1, "150.00", dist_set=VALID_DIST_SET,
                           description="Line for invalid site"))

    return hdrs, lns


# ?? Case 5: Closed GL accounting period ???????????????????????????????????????????????
# Note: Oracle does NOT reject invoices at FBDI import time for invoice-amount vs
# line-sum mismatches -- that check only fires at posting time.
# To get guaranteed import-level rejections we use accounting dates in periods that
# are either permanently closed (2018) or never existed (2099).

def _build_case5():
    """
    Sub-case A: Accounting date 2018/01/01 -- period is closed, Oracle must reject.
    Sub-case B: Accounting date 2099/12/31 -- period does not exist, Oracle must reject.
    Both use a valid supplier and valid distribution set so those fields are NOT the cause.
    Expected: 0 created, 2 rejected -- "Accounting period is not open" or similar.
    """
    inv_date = "2026/03/02"
    hdrs, lns = [], []

    # Sub-case A: past closed period
    iid_a = _inv_id(5, 1)
    hdrs.append(_make_header(iid_a, _inv_num("C5", 1), "600.00", inv_date,
                              VALID_SUPPLIER_NAME, VALID_SUPPLIER_NUM,
                              VALID_SUPPLIER_SITE,
                              import_set=f"SCEN5_{RUN_TOKEN}",
                              acct_date="2018/01/01",
                              description="Case 5A -- closed GL period (Jan-2018)"))
    lns.append(_make_line(iid_a, 1, "600.00", dist_set=VALID_DIST_SET,
                           acct_date="2018/01/01",
                           description="Line in closed period"))

    # Sub-case B: far-future period (does not exist in Oracle calendar)
    iid_b = _inv_id(5, 2)
    hdrs.append(_make_header(iid_b, _inv_num("C5", 2), "400.00", inv_date,
                              VALID_SUPPLIER_NAME, VALID_SUPPLIER_NUM,
                              VALID_SUPPLIER_SITE,
                              import_set=f"SCEN5_{RUN_TOKEN}",
                              acct_date="2099/12/31",
                              description="Case 5B -- future period that does not exist (Dec-2099)"))
    lns.append(_make_line(iid_b, 1, "400.00", dist_set=VALID_DIST_SET,
                           acct_date="2099/12/31",
                           description="Line in non-existent future period"))

    return hdrs, lns


# ?? Case 6: Invalid distribution combination / distribution set ????????????????
# Note: Oracle allows an invoice with EMPTY distribution through the FBDI importer
# (it is only rejected at posting/validation time).  To get an import-level rejection
# for BOTH sub-cases we must supply an INVALID value, not an empty one.

def _build_case6():
    """
    Sub-case A: dist_combo = "99999-99999-99999-99999-99999" (fake GL segments)
                -> rejected: "Invalid distribution combination"
    Sub-case B: dist_set   = "NONEXISTENT_DIST_SET_9999" (named set that does not exist)
                -> rejected: "Invalid distribution set"
    Both use a valid supplier so supplier errors are not the cause.
    Expected: 0 created, 2 rejected.
    """
    inv_date = "2026/03/02"
    hdrs, lns = [], []

    # Sub-case A: entirely fake GL segment string
    iid_a = _inv_id(6, 1)
    hdrs.append(_make_header(iid_a, _inv_num("C6", 1), "400.00", inv_date,
                              VALID_SUPPLIER_NAME, VALID_SUPPLIER_NUM,
                              VALID_SUPPLIER_SITE,
                              import_set=f"SCEN6_{RUN_TOKEN}",
                              description="Case 6A -- invalid distribution combination (fake segments)"))
    lns.append(_make_line(iid_a, 1, "400.00",
                           dist_combo="99999-99999-99999-99999-99999",
                           description="Line with invalid GL account segments"))

    # Sub-case B: distribution set name that does not exist in this Oracle instance
    iid_b = _inv_id(6, 2)
    hdrs.append(_make_header(iid_b, _inv_num("C6", 2), "175.00", inv_date,
                              VALID_SUPPLIER_NAME, VALID_SUPPLIER_NUM,
                              VALID_SUPPLIER_SITE,
                              import_set=f"SCEN6_{RUN_TOKEN}",
                              description="Case 6B -- non-existent distribution set name"))
    lns.append(_make_line(iid_b, 1, "175.00",
                           dist_set="NONEXISTENT_DIST_SET_9999",
                           description="Line with non-existent distribution set"))

    return hdrs, lns


# ?? Scenario registry ?????????????????????????????????????????????????????????

SCENARIOS: list[Scenario] = [
    Scenario(1, "VALID_USD",
             "2 valid USD invoices (ABC Consulting / Advertising (Full) dist set)",
             "SUCCEEDED -- 2 invoices created, 0 rejected",
             _build_case1,
             exp_created=2, exp_rejected=0),
    Scenario(2, "DUPLICATE",
             "Same invoice numbers as Case 1 re-submitted (duplicate check)",
             "SUCCEEDED -- 0 created, 2 rejected (Duplicate invoice number)",
             _build_case2,
             exp_created=0, exp_rejected=2),
    Scenario(3, "INVALID_RATE",
             "EUR invoices with Conversion Rate = -1 (negative) and 0 (zero)",
             "SUCCEEDED -- 0 created, 2 rejected (Invalid conversion rate)",
             _build_case3,
             exp_created=0, exp_rejected=2),
    Scenario(4, "BAD_SUPPLIER",
             "Completely fake supplier + real supplier with non-existent site",
             "SUCCEEDED -- 0 created, 2 rejected (Invalid supplier / Invalid supplier site)",
             _build_case4,
             exp_created=0, exp_rejected=2),
    Scenario(5, "CLOSED_PERIOD",
             "Accounting date in a closed GL period (2018) and non-existent period (2099)",
             "SUCCEEDED -- 0 created, 2 rejected (Accounting period not open)",
             _build_case5,
             exp_created=0, exp_rejected=2),
    Scenario(6, "BAD_DIST",
             "Fake GL account segments + non-existent distribution set name",
             "SUCCEEDED -- 0 created, 2 rejected (Invalid distribution combination/set)",
             _build_case6,
             exp_created=0, exp_rejected=2),
]


# ?? Result recorder ???????????????????????????????????????????????????????????

class Result(NamedTuple):
    num:           int
    key:           str
    expected:      str
    ess_status:    str       # SUCCEEDED / ERROR / WARNING / TIMEOUT / SKIP
    fetched:       int
    created:       int
    rejected:      int
    rejections:    list      # list of rejection detail dicts
    log_errors:    list      # lines from ESS log analysis
    req_id:        str
    exp_created:   int = -1  # from Scenario -- for pass/fail check
    exp_rejected:  int = -1  # from Scenario -- for pass/fail check


# ?? Main runner ???????????????????????????????????????????????????????????????

def run_scenario(sc: Scenario, dry_run: bool = False) -> Result:
    sep = "=" * 72
    print(f"\n{sep}")
    print(f"  SCENARIO {sc.num}: {sc.key}")
    print(f"  {sc.description}")
    print(f"  Expected: {sc.expected}")
    print(sep)

    # Build FBDI data
    headers, lines = sc.build()
    print(f"  Built: {len(headers)} header(s), {len(lines)} line(s)")

    # Write ZIP
    zip_name = f"SCEN{sc.num}_{sc.key}_{RUN_TOKEN}.zip"
    zip_path = OUT_DIR / zip_name
    _build_zip(headers, lines, zip_path)
    print(f"  ZIP: {zip_path.name}  ({zip_path.stat().st_size / 1024:.1f} KB)")

    if dry_run:
        print("  DRY-RUN: skipping Oracle submission")
        return Result(sc.num, sc.key, sc.expected,
                      "DRY-RUN", 0, 0, 0, [], [], "",
                      sc.exp_created, sc.exp_rejected)

    if not all([FUSION_URL, USERNAME, PASSWORD, AP_BU_ID, AP_LEDGER_ID]):
        print("  SKIP: credentials not configured (set FUSION_URL, FUSION_USER, "
              "FUSION_PASSWORD, AP_BU_ID, AP_LEDGER_ID)")
        return Result(sc.num, sc.key, sc.expected,
                      "SKIP", 0, 0, 0, [], [], "",
                      sc.exp_created, sc.exp_rejected)

    # Submit
    invoice_group = f"SCEN{sc.num}_{RUN_TOKEN}"
    acct_date = _fmt(ACCOUNTING_DATE).replace("/", "-")   # Oracle ParameterList date
    print(f"  Submitting to Oracle -- Invoice Group: {invoice_group}")
    try:
        resp = submit(zip_path, invoice_group, acct_date)
    except Exception as exc:
        print(f"  SUBMIT ERROR: {exc}")
        return Result(sc.num, sc.key, sc.expected,
                      "SUBMIT_ERROR", 0, 0, 0, [], [str(exc)], "",
                      sc.exp_created, sc.exp_rejected)

    req_id = str(resp.get("ReqstId") or "")
    if not req_id or req_id == "-1":
        errmsg = json.dumps({k: v for k, v in resp.items()
                              if k not in ("DocumentContent", "links")}, indent=2)
        print(f"  Oracle rejected submission:\n{errmsg}")
        return Result(sc.num, sc.key, sc.expected,
                      "REJECTED_BY_ORACLE", 0, 0, 0, [], [errmsg], "",
                      sc.exp_created, sc.exp_rejected)

    print(f"  File Loader ESS Request ID: {req_id}")
    print(f"  Polling file loader (up to 4 min)...")
    loader_status = poll_status(req_id, max_polls=16, interval=15)
    print(f"  -> File loader status: {loader_status}")

    # ── Find the actual APXIIMPT job (separate from the file loader) ──────────
    # importBulkData returns the file-loader req ID. The real APXIIMPT job runs
    # as a separate ESS request. We use three strategies in order:
    #   1) get_descendant_requests -- absParentRequestId query (authoritative)
    #   2) scheduler v1 direct name lookup for IDs in [loader+1 .. loader+60]
    #   3) find_ap_import_jobs -- ESSExecutionDetailsRF child job scan (fallback)
    apxiimpt_req_id = ""
    apxiimpt_status = ""
    try:
        from services.fusion_service import find_ap_import_jobs, get_descendant_requests
        class _Cfg:
            def __init__(self):
                self.fusion_url      = FUSION_URL
                self.fusion_username = USERNAME
                self.fusion_password = PASSWORD
        _cfg = _Cfg()

        AP_KEYWORDS = ("Import Payables Invoices", "APXIIMPT", "Payables Invoice Import")

        def _is_apxiimpt(name: str) -> bool:
            return any(kw in (name or "") for kw in AP_KEYWORDS)

        # --- Strategy 1: absParentRequestId query ---
        print(f"  Strategy 1: scheduler absParentRequestId query for {req_id}...")
        descendants = get_descendant_requests(_cfg, req_id)
        if descendants:
            print(f"    Found {len(descendants)} descendant(s):")
            for d in descendants:
                print(f"      req={d['request_id']} name={d['name']!r} status={d['status']}")
                if _is_apxiimpt(d["name"]) and not apxiimpt_req_id:
                    apxiimpt_req_id = d["request_id"]
                    print(f"    -> Identified APXIIMPT: req={apxiimpt_req_id}")
        else:
            print(f"    No descendants found via absParentRequestId query")

        # --- Strategy 2: direct scheduler name lookup for nearby IDs ---
        if not apxiimpt_req_id:
            print(f"  Strategy 2: direct scheduler name lookup for IDs [{req_id}+1 .. +60]...")
            try:
                base_id = int(req_id)
                probe_ids = list(range(base_id + 1, base_id + 61))
                sched_url = f"{FUSION_URL.rstrip('/')}/ess/rest/scheduler/v1/requests"
                found_names = {}
                for pid in probe_ids:
                    try:
                        r = httpx.get(f"{sched_url}/{pid}",
                                      params={"fields": "requestId,name,state,parentRequestId"},
                                      auth=(USERNAME, PASSWORD), timeout=10,
                                      headers={"Accept": "application/json"})
                        if r.status_code == 200:
                            j = r.json()
                            jname  = str(j.get("name") or "").strip()
                            jstate = str(j.get("state") or "").upper()
                            jpid   = str(j.get("parentRequestId") or "")
                            if jname:
                                found_names[str(pid)] = jname
                            if _is_apxiimpt(jname):
                                apxiimpt_req_id = str(pid)
                                print(f"    Found APXIIMPT via scheduler: req={pid} name={jname!r} state={jstate}")
                                break
                    except Exception:
                        pass
                if found_names and not apxiimpt_req_id:
                    print(f"    Scheduler names found (non-APXIIMPT): {found_names}")
            except Exception as exc2:
                print(f"    Strategy 2 error: {exc2}")

        # --- Strategy 3: ESSExecutionDetailsRF child job scan ---
        if not apxiimpt_req_id:
            print(f"  Strategy 3: ESSExecutionDetailsRF forward scan after {req_id}...")
            ap_jobs = find_ap_import_jobs(_cfg, req_id, scan_range=60, invoice_group=invoice_group)
            if ap_jobs:
                for j in ap_jobs:
                    print(f"    Found: req={j['request_id']} name={j['name']!r}")
                    if _is_apxiimpt(j["name"]) and not apxiimpt_req_id:
                        apxiimpt_req_id = str(j["request_id"])
                        break
                if not apxiimpt_req_id:
                    apxiimpt_req_id = str(ap_jobs[0]["request_id"])
                    print(f"    Using first AP job: req={apxiimpt_req_id}")
            else:
                print(f"    No AP jobs found via ESSExecutionDetailsRF scan")

    except Exception as exc:
        print(f"  APXIIMPT discovery failed: {exc}")

    ess_status = loader_status
    if apxiimpt_req_id:
        # Poll APXIIMPT to completion (it may still be running)
        print(f"  Polling APXIIMPT {apxiimpt_req_id} (up to 7.5 min)...")
        ess_status = poll_status(apxiimpt_req_id, max_polls=30, interval=15)
        print(f"  -> APXIIMPT status: {ess_status}")
    else:
        print(f"  WARNING: Could not find APXIIMPT job — results will use file-loader status")

    # Child jobs of APXIIMPT
    effective_req_id = apxiimpt_req_id or req_id
    children = get_child_jobs(effective_req_id)
    if children:
        print(f"  APXIIMPT child jobs ({len(children)}):")
        for c in children:
            print(f"    {c['name']:40s} req={c['request_id']} status={c['status']}")

    # ── Collect all log ZIPs (APXIIMPT + children) ───────────────────────────
    all_log_zips: list[bytes] = []
    try:
        for rid in ([effective_req_id] + [c["request_id"] for c in children]):
            zb = download_log_zip_bytes(rid)
            if zb:
                all_log_zips.append(zb)
    except Exception as exc:
        print(f"  Log download error: {exc}")
    print(f"  Downloaded {len(all_log_zips)} log ZIP(s)")

    # ── Try BIP XML: direct SOAP call using APXIIMPT req ID ──────────────────
    bip_result: dict = {}
    print(f"  Fetching BIP report XML for req {effective_req_id}...")
    xml_bytes = fetch_bip_xml_direct(effective_req_id)
    if xml_bytes and len(xml_bytes) > 100:
        bip_result = parse_bip_xml(xml_bytes)
        if bip_result:
            print(f"    BIP SOAP XML: fetched={bip_result.get('fetched')} "
                  f"created={bip_result.get('created')} rejected={bip_result.get('rejected')}")
        else:
            print(f"    BIP SOAP returned {len(xml_bytes)} bytes but parse failed")
            print(f"    Preview: {xml_bytes[:120]}")

    # ── Fallback: extract BIP XML embedded in ESS log ZIP ────────────────────
    if not bip_result:
        for log_zip_bytes in all_log_zips:
            embedded_xml = extract_xml_from_log_zip(log_zip_bytes)
            if embedded_xml:
                bip_result = parse_bip_xml(embedded_xml)
                if bip_result:
                    print(f"    Log-embedded XML: fetched={bip_result.get('fetched')} "
                          f"created={bip_result.get('created')} "
                          f"rejected={bip_result.get('rejected')}")
                    break

    # ── ESS log text analysis ─────────────────────────────────────────────────
    log_errors: list[str] = []
    try:
        all_log_text = ""
        for log_zip_bytes in all_log_zips:
            try:
                import zipfile as _zf, io as _io
                with _zf.ZipFile(_io.BytesIO(log_zip_bytes)) as z:
                    for name in z.namelist():
                        if any(name.lower().endswith(ext) for ext in (".log", ".out", ".txt")):
                            all_log_text += f"\n--- {name} ---\n"
                            all_log_text += z.read(name).decode("utf-8", "replace")
                        elif name.lower().endswith(".xml") and not bip_result:
                            data = z.read(name)
                            if b"APXIIMPT" in data or b"G_REJECTIONS" in data or b"INVOICES" in data:
                                bip_result = parse_bip_xml(data)
                                if bip_result:
                                    print(f"    Log-XML ({name}): fetched={bip_result.get('fetched')} "
                                          f"created={bip_result.get('created')} "
                                          f"rejected={bip_result.get('rejected')}")
            except Exception:
                pass
        if all_log_text:
            la = analyze_log(all_log_text)
            log_errors = la.get("detail_lines", [])
            if la.get("has_errors") or la.get("has_warnings"):
                print(f"  ESS log errors: {la.get('summary', '')}")
            elif all_log_text:
                # Show first 10 non-blank lines for context
                lines = [l for l in all_log_text.split("\n") if l.strip()][:10]
                print(f"  Log preview ({len(lines)} lines):")
                for l in lines: print(f"    {l}")
    except Exception as exc:
        print(f"  Log analysis error: {exc}")

    fetched    = bip_result.get("fetched",    0)
    created    = bip_result.get("created",    0)
    rejected   = bip_result.get("rejected",   0)
    rejections = bip_result.get("rejections", [])
    bus        = bip_result.get("business_units", [])

    if bip_result:
        bu_str = f"  BU(s): {', '.join(bus)}" if bus else ""
        print(f"  BIP report: fetched={fetched} created={created} rejected={rejected}{bu_str}")

    if rejections:
        print(f"  Rejection details ({len(rejections)}):")
        for idx, r in enumerate(rejections, 1):
            reasons  = r.get("reasons",      [])
            descs    = r.get("descriptions", [])
            lvls     = r.get("line_levels",  [])
            bu       = r.get("business_unit","")
            print(f"    --- Rejection {idx} ---")
            if bu:
                print(f"    Business Unit  : {bu}")
            print(f"    Invoice Number : {r.get('invoice_num','')}")
            print(f"    Invoice ID     : {r.get('invoice_id','')}")
            amt  = r.get('amount','')
            curr = r.get('currency','')
            date = r.get('date','')
            if amt or curr or date:
                print(f"    Amount/Currency: {amt} {curr}  Date: {date}")
            print(f"    Supplier       : {r.get('supplier','')} "
                  f"(Num: {r.get('supplier_num','')}, Site: {r.get('site','')})")
            for i, reason in enumerate(reasons):
                desc  = descs[i]  if i < len(descs) else ""
                level = lvls[i]   if i < len(lvls)  else ""
                level_str = f" [{'Header' if level.strip()=='H' else 'Line' if level.strip()=='L' else 'H/L'}]"
                print(f"    Reason {i+1:2d}       : {reason}{level_str}")
                if desc:
                    print(f"    Description    : {desc}")
    elif created == fetched and fetched > 0:
        print(f"  All {fetched} invoice(s) created successfully.")

    return Result(sc.num, sc.key, sc.expected, ess_status,
                  fetched, created, rejected, rejections, log_errors, req_id,
                  sc.exp_created, sc.exp_rejected)


# ?? Summary table ?????????????????????????????????????????????????????????????

def _pass_flag(r: "Result") -> str:
    """
    PASS  -- ESS ran + BIP counts match expected (where exp_created / exp_rejected >= 0)
    FAIL  -- ESS ran but created/rejected counts differ from expected
    ERROR -- ESS didn't run (submit error, skip, timeout, etc.)
    --    -- no expectation defined (exp = -1)
    """
    if r.ess_status in ("SUBMIT_ERROR", "REJECTED_BY_ORACLE", "SKIP", "DRY-RUN", "TIMEOUT"):
        return "ERROR"
    # Check BIP counts against expected (only when an expectation is set)
    c_ok = (r.exp_created  < 0) or (r.created  == r.exp_created)
    r_ok = (r.exp_rejected < 0) or (r.rejected == r.exp_rejected)
    if r.exp_created < 0 and r.exp_rejected < 0:
        return "--"
    return "PASS" if (c_ok and r_ok) else "FAIL"


def print_summary(results: list[Result]) -> None:
    print("\n" + "=" * 100)
    print("  FINAL SUMMARY")
    print("=" * 100)
    hdr = (f"{'#':>2}  {'KEY':<18}  {'ESS':^10}  "
           f"{'Ftch':>4}  {'Crt':>4} {'ExpCrt':>6}  "
           f"{'Rej':>4} {'ExpRej':>6}  {'PASS?':<6}")
    print(hdr)
    print("-" * 100)
    for r in results:
        exp_crt = f"({r.exp_created})" if r.exp_created >= 0 else "   - "
        exp_rej = f"({r.exp_rejected})" if r.exp_rejected >= 0 else "   - "
        flag    = _pass_flag(r)
        flag_str = f"[{flag}]" if flag in ("FAIL", "ERROR") else flag
        print(f"  {r.num:>2}  {r.key:<18}  {r.ess_status:^10}  "
              f"{r.fetched:>4}  {r.created:>4} {exp_crt:>6}  "
              f"{r.rejected:>4} {exp_rej:>6}  {flag_str}")
    print("-" * 100)

    # Show overall result
    live = [r for r in results if r.ess_status not in ("SKIP", "DRY-RUN")]
    passes = sum(1 for r in live if _pass_flag(r) == "PASS")
    fails  = sum(1 for r in live if _pass_flag(r) == "FAIL")
    errors = sum(1 for r in live if _pass_flag(r) == "ERROR")
    print(f"\n  Live scenarios: {len(live)}  |  PASS: {passes}  |  FAIL: {fails}  |  ERROR: {errors}")

    for r in results:
        if r.rejections:
            print(f"\n  Scenario {r.num} ({r.key}) -- full rejection report:")
            print(f"  {'='*68}")
            for idx, rej in enumerate(r.rejections, 1):
                reasons = rej.get("reasons",      [])
                descs   = rej.get("descriptions", [])
                lvls    = rej.get("line_levels",  [])
                bu      = rej.get("business_unit","")
                amt     = rej.get("amount","")
                curr    = rej.get("currency","")
                date    = rej.get("date","")
                print(f"  [{idx}] Invoice: {rej.get('invoice_num','')}  |  "
                      f"ID: {rej.get('invoice_id','')}  |  "
                      f"Amount: {amt} {curr}  Date: {date}")
                print(f"      Supplier: {rej.get('supplier','')} "
                      f"(#{rej.get('supplier_num','')}, site: {rej.get('site','')})")
                if bu:
                    print(f"      Business Unit: {bu}")
                for i, reason in enumerate(reasons):
                    desc  = descs[i] if i < len(descs) else ""
                    level = lvls[i]  if i < len(lvls)  else ""
                    lvl_s = "Header" if level.strip()=="H" else ("Line" if level.strip()=="L" else "")
                    lvl_tag = f" [{lvl_s}]" if lvl_s else ""
                    print(f"      Rejection {i+1}: {reason}{lvl_tag}")
                    if desc:
                        print(f"               -> {desc}")
        elif r.log_errors and r.ess_status not in ("DRY-RUN", "SKIP"):
            print(f"\n  Scenario {r.num} ({r.key}) -- ESS log errors:")
            for line in r.log_errors[:10]:
                print(f"    {line}")

    # Save JSON result
    out_json = OUT_DIR / f"ap_scenario_results_{RUN_TOKEN}.json"
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump([
            {
                "scenario": r.num, "key": r.key,
                "expected": r.expected,
                "ess_status": r.ess_status,
                "req_id": r.req_id,
                "fetched": r.fetched, "created": r.created, "rejected": r.rejected,
                "rejections": r.rejections,
                "log_errors": r.log_errors,
            }
            for r in results
        ], f, indent=2)
    print(f"\n  Results saved: {out_json.name}")


# ?? Entry point ???????????????????????????????????????????????????????????????

def main():
    parser = argparse.ArgumentParser(description="AP FBDI 6-scenario live test")
    parser.add_argument("--dry-run", action="store_true",
                        help="Build ZIPs but skip Oracle submission")
    parser.add_argument("--cases", default="",
                        help="Comma-separated scenario numbers to run (default: all)")
    args = parser.parse_args()

    run_set: set[int] | None = None
    if args.cases:
        run_set = {int(x.strip()) for x in args.cases.split(",") if x.strip().isdigit()}

    print(f"\n{'='*72}")
    print(f"  AP Invoice FBDI -- 6-Scenario Test Run  [{RUN_TOKEN}]")
    print(f"  Oracle: {FUSION_URL or '(not configured)'}")
    print(f"  {'DRY-RUN mode -- no Oracle calls' if args.dry_run else 'LIVE mode'}")
    print(f"{'='*72}")

    if HDR_DATA_COLS is None:
        print("ERROR: Could not load ap_fbdi_generator.  "
              "Run from the oracle_fbdi_app directory.")
        sys.exit(1)

    results: list[Result] = []
    for sc in SCENARIOS:
        if run_set is not None and sc.num not in run_set:
            continue
        result = run_scenario(sc, dry_run=args.dry_run)
        results.append(result)
        # Brief pause between submissions to avoid ESS queue back-pressure
        if not args.dry_run and sc.num < len(SCENARIOS):
            print("  (waiting 5 s before next scenario...)")
            time.sleep(5)

    print_summary(results)


if __name__ == "__main__":
    main()
