"""
Standalone AP Invoice FBDI submission test.
Uses the user's sample ApInvoicesInterface.csv + ApInvoiceLinesInterface.csv with
fixed Invoice IDs (the originals show Excel scientific notation 2.39239E+11),
packages them into a ZIP, and POSTs to Oracle Fusion importBulkData.

Verifies: Oracle accepts payload (ReqstId != -1), then polls until terminal.

Run:  python ap_submit_test.py
"""
from __future__ import annotations
import base64, csv, io, json, sys, time, uuid, zipfile
from pathlib import Path

import httpx

FUSION_URL  = "https://fa-etao-dev18-saasfademo1.ds-fa.oraclepdemos.com"
USERNAME    = "Kavin.Sasikumar"
PASSWORD    = "12345678"

# Values from the user's APIMPORT.properties — exact replay
AP_BU_ID    = "300000046987012"   # US1 Business Unit ID
AP_LEDGER_ID= "300000046975971"   # US Primary Ledger ID
AP_SOURCE   = "External"
AP_DOC_ACCT = "fin$/payables$/import$"
AP_JOB_NAME = "oracle/apps/ess/financials/payables/invoices/transactions,APXIIMPT"
ERPI        = "/fscmRestApi/resources/11.13.18.05/erpintegrations"

SAMPLE_DIR  = Path(__file__).parent

# ── Fix Excel-mangled Invoice IDs ─────────────────────────────────────────────

def fix_invoice_id(s: str, fallback: str) -> str:
    """Replace scientific notation like '2.39239E+11' with a clean integer."""
    s = (s or "").strip()
    if "e" in s.lower() or "E+" in s:
        try:
            return str(int(float(s)))
        except Exception:
            return fallback
    return s or fallback


def _fix_date(s: str) -> str:
    """Convert DD-MM-YYYY / MM-DD-YYYY / etc → YYYY/MM/DD for Oracle SQL*Loader."""
    s = (s or "").strip()
    if not s: return ""
    from datetime import datetime
    for fmt in ("%Y/%m/%d","%Y-%m-%d","%d-%m-%Y","%m-%d-%Y","%d/%m/%Y","%m/%d/%Y","%d-%b-%Y"):
        try: return datetime.strptime(s, fmt).strftime("%Y/%m/%d")
        except ValueError: pass
    return s


# Indices (0-based) of date columns in ApInvoicesInterface.csv based on header analysis
HDR_DATE_COLS  = [5, 20, 21, 22, 23, 31, 34]   # Invoice Date, Terms Date, Goods Received, Invoice Received, Accounting Date, Prepayment Accounting Date, Conversion Date
LINE_DATE_COLS = [23]                           # Accounting Date


def repack_with_clean_ids(out_zip: Path, batch_token: str) -> tuple[int, int]:
    """
    Rewrite the sample header + line CSVs into a HEADERLESS, positional Oracle FBDI format.
    Returns (n_headers, n_lines).
    """
    base = int(abs(hash(batch_token)) % 900000000) + 100000000   # always 9 digits

    hdr_in  = (SAMPLE_DIR / "ApInvoicesInterface.csv").read_text(encoding="utf-8").splitlines()
    line_in = (SAMPLE_DIR / "ApInvoiceLinesInterface.csv").read_text(encoding="utf-8").splitlines()

    hdr_rows  = list(csv.reader(hdr_in))
    line_rows = list(csv.reader(line_in))
    hdr_data  = [r for r in hdr_rows[1:]  if r and any(r)]   # skip header row
    line_data = [r for r in line_rows[1:] if r and any(r)]

    # Assign each header a fresh Invoice ID (base, base+1, base+2, …)
    for i, row in enumerate(hdr_data):
        row[0] = str(base + i)
        for ci in HDR_DATE_COLS:
            if ci < len(row): row[ci] = _fix_date(row[ci])

    # Re-link lines to headers: walk through line_data, watching for LINE_NUMBER resets
    # to 1 as the boundary between invoices. First non-reset → first invoice.
    cur_idx = 0; last_ln = 0
    for row in line_data:
        try: ln = int(row[1])
        except (ValueError, IndexError): ln = 1
        if ln == 1 and last_ln >= 1:
            cur_idx += 1
        last_ln = ln
        if cur_idx >= len(hdr_data):
            cur_idx = len(hdr_data) - 1
        row[0] = str(base + cur_idx)
        for ci in LINE_DATE_COLS:
            if ci < len(row): row[ci] = _fix_date(row[ci])

    # Write HEADERLESS CSVs into the ZIP
    with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as zf:
        buf = io.StringIO()
        w = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
        for row in hdr_data: w.writerow(row)
        zf.writestr("ApInvoicesInterface.csv", buf.getvalue())

        buf2 = io.StringIO()
        w2 = csv.writer(buf2, quoting=csv.QUOTE_MINIMAL)
        for row in line_data: w2.writerow(row)
        zf.writestr("ApInvoiceLinesInterface.csv", buf2.getvalue())

    return len(hdr_data), len(line_data)


# ── Submit to Oracle Fusion ───────────────────────────────────────────────────

def submit(zip_path: Path, invoice_group: str) -> dict:
    """
    POST GlInterface.zip to importBulkData operation.

    ParameterList for APXIIMPT (matches user's APIMPORT.properties literal values):
      arg1 (empty),                       arg2 = Business Unit ID,
      arg3 = N,                           arg4 = Accounting Date,
      arg5 (empty),                       arg6 (empty),
      arg7 = 1000,                        arg8 = Source,
      arg9 = Invoice Group / Import Set,  arg10 = N,
      arg11 = N,                          arg12 = Ledger ID,
      arg13 (empty),                      arg14 = 1
    """
    b64 = base64.b64encode(zip_path.read_bytes()).decode("utf-8")
    accounting_date = "2026-03-02"
    param_list = (
        f"#NULL,{AP_BU_ID},N,{accounting_date},#NULL,#NULL,1000,"
        f"{AP_SOURCE},{invoice_group},N,N,{AP_LEDGER_ID},#NULL,1"
    )
    payload = {
        "OperationName":   "importBulkData",
        "DocumentContent": b64,
        "ContentType":     "zip",
        "FileName":        zip_path.name,
        "DocumentAccount": AP_DOC_ACCT,
        "JobName":         AP_JOB_NAME,
        "ParameterList":   param_list,
        "CallbackURL":     "#NULL",
        "NotificationCode":"10",
        "JobOptions":      "InterfaceDetails=1,ImportOption=Y,PurgeOption=Y,ExtractFileType=ALL",
    }
    print("ParameterList:", param_list)
    print("Posting to:   ", FUSION_URL + ERPI)
    print(f"Payload size: {len(b64)/1024:.1f} KB base64")

    r = httpx.post(FUSION_URL + ERPI, json=payload,
                   auth=(USERNAME, PASSWORD), timeout=120,
                   headers={"Content-Type":"application/json","Accept":"application/json"})
    print("HTTP", r.status_code)
    data = r.json()
    print(f"  ReqstId        : {data.get('ReqstId')}")
    print(f"  RequestStatus  : {data.get('RequestStatus')}")
    print(f"  StatusCode     : {data.get('StatusCode')}")
    return data


def poll(req_id: str, max_polls: int = 25, interval: int = 15) -> str:
    """Poll ESSJobStatusRF until terminal."""
    url = FUSION_URL + ERPI
    auth = (USERNAME, PASSWORD)
    for i in range(1, max_polls + 1):
        r = httpx.get(url, params={"finder": f"ESSJobStatusRF;requestId={req_id}"},
                      auth=auth, timeout=30, headers={"Accept":"application/json"})
        items = r.json().get("items", [])
        st = str(items[0].get("RequestStatus","")).upper() if items else "?"
        print(f"  Poll {i:2}: {st}")
        if st in ("SUCCEEDED","ERROR","WARNING","CANCELLED","BLOCKED"):
            return st
        time.sleep(interval)
    return "TIMEOUT"


def check_descendants(parent_id: str) -> list[dict]:
    """ESSExecutionDetailsRF returns the child hierarchy."""
    url = FUSION_URL + ERPI
    r = httpx.get(url, params={"finder": f"ESSExecutionDetailsRF;requestId={parent_id}"},
                  auth=(USERNAME, PASSWORD), timeout=30, headers={"Accept":"application/json"})
    items = r.json().get("items", [])
    if not items: return []
    raw = items[0].get("RequestStatus") or ""
    try:
        parsed = json.loads(raw)
    except Exception:
        return []
    flat = []
    def walk(node):
        if isinstance(node, dict):
            if node.get("JOBNAME") or node.get("REQUESTID"):
                flat.append({
                    "name": node.get("JOBNAME") or "",
                    "request_id": node.get("REQUESTID") or "",
                    "status": (node.get("STATUS") or "").upper(),
                })
            for k in ("CHILD","JOBS"):
                if k in node: walk(node[k])
        elif isinstance(node, list):
            for x in node: walk(x)
    walk(parsed)
    return flat


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    batch_token = f"AP-TEST-{uuid.uuid4().hex[:8]}"
    invoice_group = f"AP_TEST_{batch_token[-8:]}"
    zip_path = SAMPLE_DIR / f"{batch_token}.zip"

    print(f"\n=== Building AP FBDI ZIP for batch {batch_token} ===")
    n_h, n_l = repack_with_clean_ids(zip_path, batch_token)
    print(f"  {n_h} headers, {n_l} lines packed into {zip_path.name}")
    print(f"  Invoice group: {invoice_group}")

    print(f"\n=== Submitting to Oracle Fusion ===")
    resp = submit(zip_path, invoice_group)
    eid = str(resp.get("ReqstId") or "")
    if not eid or eid == "-1":
        print("\n*** Oracle rejected the submission ***")
        print(json.dumps({k:v for k,v in resp.items()
                          if k not in ("DocumentContent","links")}, indent=2))
        sys.exit(1)

    print(f"\n=== Polling ESS request {eid} ===")
    final = poll(eid)
    print(f"\nFinal parent status: {final}")

    print("\n=== Child job hierarchy ===")
    for j in check_descendants(eid):
        print(f"  {j['name']:35s} req={j['request_id']} status={j['status']}")
