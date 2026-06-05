"""
Oracle Fusion REST API service.

Confirmed working endpoints (relative paths — host is configured in Settings):
  POST /fscmRestApi/resources/11.13.18.05/erpintegrations  (importBulkData) -> 201
  GET  /fscmRestApi/resources/11.13.18.05/erpintegrations?finder=ESSJobStatusRF;requestId=X -> 200
  GET  /fscmRestApi/resources/11.13.18.05/accountingPeriodStatusLOV -> 200
  GET  /ess/rest/scheduler/v1/requests/{rid}?fields=requestParameters -> 200
"""

from __future__ import annotations

import base64
import logging
from datetime import datetime, timezone
from pathlib import Path

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)

ERPI = "/fscmRestApi/resources/11.13.18.05/erpintegrations"
PERIOD_LOV = "/fscmRestApi/resources/11.13.18.05/accountingPeriodStatusLOV"


def _auth(cfg) -> tuple[str, str]:
    return (cfg.fusion_username, cfg.fusion_password)


def _base(cfg) -> str:
    return cfg.fusion_url.rstrip("/")


# ── Period Status ─────────────────────────────────────────────────────────────

def check_period_status(cfg, ledger_name: str, period_name: str) -> str:
    """
    Query accountingPeriodStatusLOV to find period status.
    ClosingStatus values: O=Open, F=Future Enterable, C=Closed, N=Never Opened, P=Permanently Closed
    Returns: 'Open' | 'Future Enterable' | 'Closed' | 'Not Found' | 'Error'
    """
    # The LOV endpoint returns LedgerId-based records. Since we may not know LedgerId,
    # we search by PeriodName and check if we get any open periods.
    url = f"{_base(cfg)}{PERIOD_LOV}"
    # Convert period name e.g. "May-26" to year=2026, number=5
    try:
        from datetime import datetime
        dt = datetime.strptime(period_name, "%b-%y")
        year, month = dt.year, dt.month
    except ValueError:
        return "Not Found"

    params = {
        "q": f"PeriodYear={year};PeriodNumber={month};AdjustmentPeriodFlag=false",
        "fields": "LedgerId,ClosingStatus,PeriodYear,PeriodNumber",
        "limit": 50,
    }
    try:
        resp = httpx.get(url, params=params, auth=_auth(cfg), timeout=30,
                         headers={"Accept": "application/json"})
        if resp.status_code != 200:
            logger.warning("Period check returned %d", resp.status_code)
            return "Error"
        items = resp.json().get("items", [])
        if not items:
            return "Not Found"
        # Look for any Open or Future Enterable record
        status_map = {"O": "Open", "F": "Future Enterable", "C": "Closed",
                      "N": "Never Opened", "P": "Permanently Closed"}
        statuses = {status_map.get(i.get("ClosingStatus",""), "Unknown") for i in items}
        if "Open" in statuses:
            return "Open"
        if "Future Enterable" in statuses:
            return "Future Enterable"
        if "Closed" in statuses:
            return "Closed"
        return list(statuses)[0] if statuses else "Not Found"
    except Exception as e:
        logger.error("Period check error: %s", e)
        return "Error"


# ── FBDI Submission ───────────────────────────────────────────────────────────

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=4, max=30),
       retry=retry_if_exception_type(httpx.TransportError))
def submit_fbdi(cfg, zip_path: str, group_id: str = "", ledger_name: str = "") -> dict:
    """
    POST GlInterface.zip to Oracle ERP Integration importBulkData.

    GL Journal Import ParameterList (7 args):
      1. LedgerID         — numeric, use #NULL to default to all accessible ledgers
      2. JournalSource    — e.g. Manual
      3. DataAccessSetID  — numeric, use #NULL for default
      4. GroupID          — must match Interface Group Identifier in GlInterface.csv
      5. PostToSuspense   — N
      6. CreateSummary    — N
      7. ImportDFF        — N
    """
    zip_bytes = Path(zip_path).read_bytes()
    b64 = base64.b64encode(zip_bytes).decode("utf-8")

    # ParameterList for GL JournalImportLauncher (7 positional args):
    #   1. Ledger Name  2. Journal Source  3. Data Access Set
    #   4. Group ID (numeric, matches Interface Group Identifier in CSV)
    #   5. Post to Suspense  6. Create Summary  7. Import DFF
    # Ledger name now comes ONLY from the caller (workflow extracts + REST-validates
    # it from the data file). No setting-level default, no hard-coded fallback.
    ledger = (ledger_name or "").strip()
    if not ledger:
        raise ValueError("submit_fbdi: ledger_name is required (resolve from the data file before calling)")
    group  = group_id or "ALL"
    param_list = f"{ledger},Manual,{ledger},{group},N,N,N"

    payload = {
        "OperationName":   "importBulkData",
        "DocumentContent": b64,
        "ContentType":     "zip",
        "FileName":        "GlInterface.zip",
        "DocumentAccount": cfg.fusion_document_account,
        "JobName":         cfg.fusion_job_name,
        "ParameterList":   param_list,
        "CallbackURL":     (cfg.gl_callback_url or "#NULL"),
        "NotificationCode":(cfg.gl_notification_code or "10"),
        "JobOptions":      (cfg.gl_job_options
                            or "EnableEvent=Y,importOption=Y,purgeOption=Y,ExtractFileType!= NONE"),
    }

    url = f"{_base(cfg)}{ERPI}"
    logger.info("Submitting FBDI to Oracle Fusion: %s (%.1f KB)", url, len(zip_bytes)/1024)

    resp = httpx.post(url, json=payload, auth=_auth(cfg), timeout=120,
                      headers={"Content-Type": "application/json", "Accept": "application/json"})
    resp.raise_for_status()
    data = resp.json()
    logger.info("Submission response: ReqstId=%s", data.get("ReqstId"))
    return data


def analyze_ap_bip_xml(xml_bytes: bytes) -> dict:
    """
    Parse Oracle's Import Payables Invoices BIP data XML to detect actual outcome.

    Looks at:
      - <C_INVOICES_FETCHED> / <G_INVOICES_FETCHED>
      - <C_INVOICES_CREATED> / <G_INVOICES_CREATED>
      - <C_INVOICES_REJECTED>
      - <LIST_G_BUSINESS_UNIT_REJECTION>/G_BUSINESS_UNIT_REJECTION/LIST_G_REJECTIONS/G_REJECTIONS

    Returns: {
      'fetched': int, 'created': int, 'rejected': int,
      'rejections': [
        {'invoice_num': str, 'invoice_id': str, 'supplier': str, 'site': str,
         'amount': str, 'reasons': [str, ...], 'descriptions': [str, ...]},
        ...
      ],
      'has_rejections': bool,
      'summary': str,    # one-line for stop_reason
    }
    """
    from xml.etree import ElementTree as ET
    out = {"fetched": 0, "created": 0, "rejected": 0,
           "rejections": [], "has_rejections": False, "summary": ""}
    if not xml_bytes:
        return out
    try:
        root = ET.fromstring(xml_bytes)
    except Exception:
        return out

    def _i(tag, default=0):
        el = root.find(f".//{tag}")
        try: return int((el.text or "").strip()) if el is not None and el.text else default
        except (ValueError, TypeError): return default

    out["fetched"]  = _i("G_INVOICES_FETCHED")  or _i("C_INVOICES_FETCHED")
    out["created"]  = _i("G_INVOICES_CREATED")  or _i("C_INVOICES_CREATED")
    out["rejected"] = _i("C_INVOICES_REJECTED")

    # Walk every G_REJECTIONS block — handles multi-BU and multi-invoice
    for rej in root.findall(".//G_REJECTIONS"):
        invoice = {
            "invoice_num":  (rej.findtext("INVOICE_NUM_R") or "").strip(),
            "invoice_id":   (rej.findtext("INVOICE_ID_R") or "").strip(),
            "supplier":     (rej.findtext("SUPPLIER_NAME_R") or "").strip(),
            "supplier_num": (rej.findtext("SUPPLIER_NUMBER_R") or "").strip(),
            "site":         (rej.findtext("VENDOR_SITE_CODE") or "").strip(),
            "currency":     (rej.findtext("INVOICE_CURRENCY_CODE_R") or "").strip(),
            "date":         (rej.findtext("INVOICE_DATE_R") or "").strip(),
            "amount":       (rej.findtext("INVOICE_AMOUNT_R") or rej.findtext("INVOICE_AMOUNT_REJ") or "").strip(),
            "reasons":      [], "descriptions": [],
        }
        for d in rej.findall("./LIST_G_REJECTIONS_DETAIL/G_REJECTIONS_DETAIL"):
            r = (d.findtext("REJECT_REASON") or "").strip()
            desc = (d.findtext("REJECTION_DESCRIPTION") or "").strip()
            if r:    invoice["reasons"].append(r)
            if desc: invoice["descriptions"].append(desc)
        out["rejections"].append(invoice)

    # Heuristic: also consider "fetched > created" as a problem even if
    # C_INVOICES_REJECTED isn't populated (some Oracle versions omit it)
    out["has_rejections"] = (
        out["rejected"] > 0
        or len(out["rejections"]) > 0
        or (out["fetched"] > 0 and out["created"] == 0)
        or (out["fetched"] > 0 and out["created"] < out["fetched"])
    )

    if out["has_rejections"]:
        # Build a concise summary listing the unique reasons
        all_reasons = []
        for inv in out["rejections"]:
            all_reasons.extend(inv["reasons"])
        unique = list(dict.fromkeys(all_reasons))   # preserve order, de-dup
        reasons_str = "; ".join(unique[:5]) or "see report"
        out["summary"] = (
            f"Import Payables Invoices: {out['rejected'] or len(out['rejections'])} "
            f"of {out['fetched']} invoices rejected ({out['created']} created). "
            f"Reasons: {reasons_str}"
        )
    else:
        out["summary"] = (
            f"Import Payables Invoices: {out['created']}/{out['fetched']} invoices imported successfully."
        )

    return out


# ── BI Publisher: render the real Oracle PDF for an ESS BIP job ───────────────

# Confirmed working live with credentials Kavin.Sasikumar on
# fa-etao-dev18-saasfademo1: report renders to a 10.9 KB PDF.
BIP_REPORT_PATHS = {
    # ESS_JOB_NAME → BIP report absolute path
    "APXIIMPT": "/Financials/Payables/Invoices/ImportPayablesInvoices.xdo",
}


def render_bip_report_pdf(cfg, report_path: str, request_id_param: str,
                            parameter_name: str = "P_REQUEST_ID",
                            output_format: str = "pdf") -> bytes:
    """
    Call BI Publisher's runReport SOAP service to render an Oracle Fusion
    seeded report (e.g. Import Payables Invoices Execution Report).

    Returns the rendered PDF bytes, or b"" on failure.
    """
    import re, base64 as _b64
    envelope = f"""<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
               xmlns:v2="http://xmlns.oracle.com/oxp/service/v2">
  <soap:Body>
    <v2:runReport>
      <v2:reportRequest>
        <v2:attributeFormat>{output_format}</v2:attributeFormat>
        <v2:reportAbsolutePath>{report_path}</v2:reportAbsolutePath>
        <v2:parameterNameValues>
          <v2:listOfParamNameValues>
            <v2:item>
              <v2:name>{parameter_name}</v2:name>
              <v2:values><v2:item>{request_id_param}</v2:item></v2:values>
            </v2:item>
          </v2:listOfParamNameValues>
        </v2:parameterNameValues>
        <v2:sizeOfDataChunkDownload>-1</v2:sizeOfDataChunkDownload>
      </v2:reportRequest>
      <v2:userID>{cfg.fusion_username}</v2:userID>
      <v2:password>{cfg.fusion_password}</v2:password>
    </v2:runReport>
  </soap:Body>
</soap:Envelope>"""
    url = _base(cfg) + "/xmlpserver/services/v2/ReportService"
    try:
        r = httpx.post(url, content=envelope, auth=_auth(cfg), timeout=120,
                       headers={"Content-Type": "text/xml;charset=UTF-8",
                                "SOAPAction": ""})
        if r.status_code != 200:
            logger.warning("BIP runReport HTTP %d for %s: %s",
                           r.status_code, report_path, r.text[:200])
            return b""
        m = re.search(r"<(?:\w+:)?reportBytes>([^<]+)</(?:\w+:)?reportBytes>", r.text)
        if not m:
            logger.warning("BIP runReport: no reportBytes in response (text=%s...)",
                           r.text[:200])
            return b""
        pdf = _b64.b64decode(m.group(1))
        if not pdf.startswith(b"%PDF"):
            logger.warning("BIP runReport returned non-PDF content: %s", pdf[:50])
        logger.info("BIP rendered %d bytes for %s (req=%s)",
                    len(pdf), report_path, request_id_param)
        return pdf
    except Exception as e:
        logger.warning("BIP runReport error: %s", e)
        return b""


# ── AP Master-Data Lookups (Business Unit / Supplier / Site / Terms) ─────────

def lookup_business_unit(cfg, bu_name: str) -> dict:
    """
    Resolve a Business Unit Name to its numeric BU ID, plus the associated
    Primary Ledger ID and Legal Entity ID via finBusinessUnitsLOV REST.

    Returns: {'BusinessUnitId': str, 'PrimaryLedgerId': str, 'LegalEntityId': str,
              'BusinessUnitName': str} or {} if not found.
    """
    if not bu_name:
        return {}
    url = f"{_base(cfg)}/fscmRestApi/resources/11.13.18.05/finBusinessUnitsLOV"
    try:
        r = httpx.get(url, params={"q": f"BusinessUnitName='{bu_name}'", "limit": 5},
                      auth=_auth(cfg), timeout=30, headers={"Accept": "application/json"})
        if r.status_code != 200:
            logger.warning("BU lookup HTTP %d for %s", r.status_code, bu_name)
            return {}
        items = r.json().get("items", [])
        if not items: return {}
        it = items[0]
        return {
            "BusinessUnitId":   str(it.get("BusinessUnitId") or ""),
            "BusinessUnitName": str(it.get("BusinessUnitName") or bu_name),
            "PrimaryLedgerId":  str(it.get("PrimaryLedgerId") or ""),
            "LegalEntityId":    str(it.get("LegalEntityId") or ""),
        }
    except Exception as e:
        logger.warning("BU lookup error: %s", e)
        return {}


def lookup_supplier(cfg, supplier_number: str = "", supplier_name: str = "") -> dict:
    """Look up supplier by number or name. Returns {} if not found."""
    if not supplier_number and not supplier_name: return {}
    url = f"{_base(cfg)}/fscmRestApi/resources/11.13.18.05/suppliers"
    if supplier_number:
        q = f"SupplierNumber='{supplier_number}'"
    else:
        q = f"Supplier='{supplier_name}'"
    try:
        r = httpx.get(url, params={"q": q, "limit": 3},
                      auth=_auth(cfg), timeout=30, headers={"Accept": "application/json"})
        if r.status_code != 200: return {}
        items = r.json().get("items", [])
        if not items: return {}
        it = items[0]
        return {
            "SupplierId":     str(it.get("SupplierId") or ""),
            "SupplierName":   str(it.get("Supplier") or it.get("SupplierName") or ""),
            "SupplierNumber": str(it.get("SupplierNumber") or supplier_number),
        }
    except Exception as e:
        logger.warning("Supplier lookup error: %s", e); return {}


def lookup_supplier_site(cfg, supplier_id: str, site_name: str) -> dict:
    """Verify a supplier site exists for the given supplier. Returns {} if not."""
    if not supplier_id or not site_name: return {}
    url = (f"{_base(cfg)}/fscmRestApi/resources/11.13.18.05/"
           f"suppliers/{supplier_id}/child/sites")
    try:
        r = httpx.get(url, params={"q": f"SupplierSite='{site_name}'", "limit": 3},
                      auth=_auth(cfg), timeout=30, headers={"Accept": "application/json"})
        if r.status_code != 200: return {}
        items = r.json().get("items", [])
        if not items: return {}
        it = items[0]
        return {"SupplierSiteId": str(it.get("SupplierSiteId") or ""),
                "SupplierSite":   str(it.get("SupplierSite") or site_name)}
    except Exception as e:
        logger.warning("Supplier site lookup error: %s", e); return {}


def lookup_payment_term(cfg, name: str) -> dict:
    """Return {'TermsId': '...', 'Name': '...'} or {} if not found."""
    if not name: return {}
    url = f"{_base(cfg)}/fscmRestApi/resources/11.13.18.05/payablesPaymentTerms"
    try:
        r = httpx.get(url, params={"q": f"Name='{name}'", "limit": 3},
                      auth=_auth(cfg), timeout=30, headers={"Accept": "application/json"})
        if r.status_code != 200: return {}
        items = r.json().get("items", [])
        if not items: return {}
        it = items[0]
        return {"TermsId": str(it.get("TermsId") or ""),
                "Name":    str(it.get("Name") or name)}
    except Exception as e:
        logger.warning("Payment term lookup error: %s", e); return {}


# ── AP Invoice FBDI Submission ────────────────────────────────────────────────

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=4, max=30),
       retry=retry_if_exception_type(httpx.TransportError))
def submit_ap_fbdi(
    cfg,
    zip_path: str,
    invoice_group: str = "",
    accounting_date: str = "",
    business_unit_name: str = "",
    business_unit_id: str = "",
    ledger_id: str = "",
    source: str = "External",
    pay_group: str = "1000",
) -> dict:
    """
    POST ApInvoicesImport.zip to Oracle's importBulkData operation, targeting
    the APXIIMPT (Import Payables Invoices) job.

    ParameterList layout — discovered from the user's APIMPORT.properties
    sample and confirmed live (ReqstId 9737430 SUCCEEDED 2026-06-04):

      arg1  empty
      arg2  Business Unit ID (numeric)
      arg3  N
      arg4  Accounting Date (YYYY-MM-DD)
      arg5  empty
      arg6  empty
      arg7  Pay Group (1000 = default)
      arg8  Source (External / INVOICE GATEWAY / etc.)
      arg9  Invoice Group / Import Set token
      arg10 N
      arg11 N
      arg12 Ledger ID (numeric)
      arg13 empty
      arg14 1  (InterfaceDetails)
    """
    # Resolve BU name → IDs via REST (preferred). Falls back to explicit IDs.
    bu_id  = (business_unit_id or "").strip()
    led_id = (ledger_id or "").strip()
    bu_name = (business_unit_name or cfg.ap_business_unit_name or "").strip()
    if (not bu_id or not led_id) and bu_name:
        info = lookup_business_unit(cfg, bu_name)
        if info:
            bu_id  = bu_id  or info.get("BusinessUnitId", "")
            led_id = led_id or info.get("PrimaryLedgerId", "")
            logger.info("Resolved BU '%s' → BU=%s Ledger=%s", bu_name, bu_id, led_id)
    # Last-resort fallback to stored IDs (legacy settings)
    bu_id  = bu_id  or (cfg.ap_business_unit_id or "").strip()
    led_id = led_id or (cfg.ap_ledger_id        or "").strip()
    if not bu_id or not led_id:
        raise ValueError("submit_ap_fbdi: could not resolve Business Unit / Ledger. "
                         "Set the BU name in /settings AP tab (or pass an explicit ID).")

    src       = (source    or cfg.ap_source    or "External").strip()
    pay_grp   = (pay_group or cfg.ap_pay_group or "1000").strip()
    inv_grp   = (invoice_group or cfg.ap_invoice_group or f"AP_BATCH_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}").strip() if False else (invoice_group or "").strip()
    if not inv_grp:
        inv_grp = "AP_BATCH"
    acct_date = (accounting_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")).strip()

    param_list = (
        f"#NULL,{bu_id},N,{acct_date},#NULL,#NULL,{pay_grp},"
        f"{src},{inv_grp},N,N,{led_id},#NULL,1"
    )

    zip_bytes = Path(zip_path).read_bytes()
    b64 = base64.b64encode(zip_bytes).decode("utf-8")

    payload = {
        "OperationName":   "importBulkData",
        "DocumentContent": b64,
        "ContentType":     "zip",
        "FileName":        "ApInvoicesImport.zip",
        "DocumentAccount": cfg.ap_document_account or "fin$/payables$/import$",
        "JobName":         cfg.ap_job_name or "oracle/apps/ess/financials/payables/invoices/transactions,APXIIMPT",
        "ParameterList":   param_list,
        "CallbackURL":     (cfg.ap_callback_url or "#NULL"),
        "NotificationCode":(cfg.ap_notification_code or "10"),
        "JobOptions":      (cfg.ap_job_options
                            or "InterfaceDetails=1,ImportOption=Y,PurgeOption=Y,ExtractFileType=ALL"),
    }

    url = f"{_base(cfg)}{ERPI}"
    logger.info("Submitting AP FBDI to Oracle Fusion: %s (%.1f KB)  group=%s",
                url, len(zip_bytes)/1024, inv_grp)
    logger.info("AP ParameterList: %s", param_list)

    resp = httpx.post(url, json=payload, auth=_auth(cfg), timeout=120,
                      headers={"Content-Type": "application/json", "Accept": "application/json"})
    resp.raise_for_status()
    data = resp.json()
    logger.info("AP submission response: ReqstId=%s", data.get("ReqstId"))
    return data


# ── AP-specific JI lookup (Import Payables Invoices = the equivalent of GL's
#    Import Journals: Child for AP). Same forward-scan pattern. ───────────────

def find_ap_import_jobs(cfg, after_request_id: str, scan_range: int = 30,
                        invoice_group: str = "", max_workers: int = 10) -> list[dict]:
    """
    importBulkData returns the file-loader request ID; the actual
    "Import Payables Invoices" and "Import Payables Invoices Report" run as
    separate ESS requests with higher IDs. This function scans the next
    `scan_range` IDs and returns any that are part of the AP import chain.

    Detection matches job names:
      - "Import Payables Invoices"
      - "Import Payables Invoices Report"
      - "APXIIMPT"

    Returns list of {request_id, name, status, scanned_from}.
    """
    try:
        start = int(after_request_id)
    except (ValueError, TypeError):
        return []
    found: list[dict] = []
    for rid in range(start + 1, start + scan_range + 1):
        try:
            det = get_execution_details(cfg, str(rid))
            for j in det.get("child_jobs", []):
                name = (j.get("name") or "").strip()
                if ("Import Payables Invoices" in name
                        or "APXIIMPT" in name
                        or "Payables Invoices Report" in name):
                    found.append({
                        "request_id": j.get("request_id") or str(rid),
                        "name":       name,
                        "status":     j.get("status") or "",
                        "scanned_from": str(rid),
                    })
        except Exception:
            continue
    # Dedupe
    seen = set(); out = []
    for j in found:
        if j["request_id"] not in seen:
            seen.add(j["request_id"]); out.append(j)
    return out


# ── ESS Status ────────────────────────────────────────────────────────────────

def get_ess_status(cfg, request_id: str) -> str:
    """
    Poll ESS job status.
    Returns: WAIT | RUNNING | SUCCEEDED | ERROR | WARNING | UNKNOWN
    """
    url = f"{_base(cfg)}{ERPI}"
    params = {"finder": f"ESSJobStatusRF;requestId={request_id}"}
    try:
        resp = httpx.get(url, params=params, auth=_auth(cfg), timeout=30,
                         headers={"Accept": "application/json"})
        if resp.status_code != 200:
            return "UNKNOWN"
        items = resp.json().get("items", [])
        if not items:
            return "UNKNOWN"
        status = str(items[0].get("RequestStatus", "")).upper()
        # Normalize Oracle status names
        mapping = {
            "SUCCEEDED": "SUCCEEDED", "SUCCESS": "SUCCEEDED",
            "RUNNING": "RUNNING", "RUN": "RUNNING",
            "WAIT": "WAIT", "WAITING": "WAIT", "PENDING": "WAIT",
            "ERROR": "ERROR", "FAILED": "ERROR", "FAILURE": "ERROR",
            "WARNING": "WARNING", "WARN": "WARNING",
            "CANCELLED": "ERROR", "BLOCKED": "ERROR",
        }
        return mapping.get(status, status or "WAIT")
    except Exception as e:
        logger.warning("ESS status check error: %s", e)
        return "UNKNOWN"


def get_ess_log(cfg, request_id: str) -> str:
    """Legacy: returns a short status summary. Use download_ess_logs for full logs."""
    url = f"{_base(cfg)}{ERPI}"
    params = {"finder": f"ESSJobStatusRF;requestId={request_id}",
              "fields": "RequestStatus,StatusCode,ESSParameters"}
    try:
        resp = httpx.get(url, params=params, auth=_auth(cfg), timeout=30,
                         headers={"Accept": "application/json"})
        if resp.status_code == 200:
            items = resp.json().get("items", [])
            if items:
                return str(items[0])
    except Exception:
        pass
    return ""


def get_child_requests(cfg, parent_request_id: str) -> list[dict]:
    """
    Find child ESS requests of a parent (e.g. JournalImportLauncher → ImportJournals → child).
    Tries several finders Oracle exposes; returns [] if none accessible.
    """
    url = f"{_base(cfg)}{ERPI}"
    finders = [
        f"ESSJobStatusRF;parentRequestId={parent_request_id}",
        f"ESSJobChildren;parentId={parent_request_id}",
        f"ESSJobStatusRF;requestId={parent_request_id}",
    ]
    for finder in finders:
        try:
            r = httpx.get(url, params={"finder": finder}, auth=_auth(cfg),
                          timeout=30, headers={"Accept":"application/json"})
            if r.status_code == 200:
                items = r.json().get("items", [])
                if items:
                    return items
        except Exception:
            continue
    return []


# ── Ledger resolution via Oracle REST ─────────────────────────────────────────
# In-memory cache so we don't hit Oracle for every submission.
_LEDGER_CACHE: dict[str, dict] = {}


def lookup_ledger(cfg, name: str = "", ledger_id: str = "") -> dict | None:
    """
    Resolve a ledger against Oracle's REST API.

    Returns {'ledger_id': '300000046975971', 'name': 'US Primary Ledger'} on success,
    or None when Oracle can't find a matching record.

    - name='US Primary Ledger'    → look up the numeric ID
    - ledger_id='300000046975971' → validate that this ID exists; returns the name

    Results are cached in-memory by both name and ID.
    """
    name      = (name or "").strip()
    ledger_id = (ledger_id or "").strip()
    if not name and not ledger_id:
        return None

    cache_key = f"name:{name}" if name else f"id:{ledger_id}"
    if cache_key in _LEDGER_CACHE:
        return _LEDGER_CACHE[cache_key]

    base = _base(cfg)
    auth = _auth(cfg)
    headers = {"Accept": "application/json"}

    # Try several known LOV / resource endpoints; first hit wins.
    if name:
        q = f"Name='{name}'"
    else:
        q = f"LedgerId={ledger_id}"
    endpoints = [
        f"{base}/fscmRestApi/resources/11.13.18.05/ledgersLOV",
        f"{base}/fscmRestApi/resources/11.13.18.05/primaryLedgersLOV",
        f"{base}/fscmRestApi/resources/11.13.18.05/journalsLedgersLOV",
    ]
    for url in endpoints:
        try:
            r = httpx.get(url, params={"q": q, "fields": "LedgerId,Name"},
                          auth=auth, timeout=15, headers=headers)
            if r.status_code != 200:
                continue
            items = r.json().get("items", [])
            if not items:
                continue
            item = items[0]
            result = {
                "ledger_id": str(item.get("LedgerId", "")),
                "name":      str(item.get("Name", "")),
                "source":    url.rsplit("/", 1)[-1],
            }
            # cache under both keys so subsequent lookups by either hit
            _LEDGER_CACHE[f"name:{result['name']}"] = result
            if result["ledger_id"]:
                _LEDGER_CACHE[f"id:{result['ledger_id']}"] = result
            logger.info("Resolved ledger '%s' -> id=%s via %s",
                        result["name"], result["ledger_id"], result["source"])
            return result
        except Exception as e:
            logger.debug("lookup_ledger via %s failed: %s", url, e)
    logger.warning("lookup_ledger found no ledger for name=%r id=%r", name, ledger_id)
    return None


def get_ji_group_id(cfg, ji_request_id: str) -> str | None:
    """
    Read the Group ID (submit.argument4) from an Import Journals ESS request via
    the Scheduler REST API. This is what links an Import Journals job back to
    the original submission's GlInterface.csv Interface Group Identifier.

    Reference: Oracle's JournalImportLauncher ParameterList layout —
       arg1=Ledger, arg2=Source, arg3=DataAccessSet, arg4=GroupID,
       arg5=PostSuspense, arg6=CreateSummary, arg7=ImportDFF.

    Returns the group_id string, or None when not exposed / lookup fails.
    """
    base = cfg.fusion_url.rstrip("/")
    url  = f"{base}/ess/rest/scheduler/v1/requests/{ji_request_id}"
    try:
        r = httpx.get(url, params={"fields": "requestParameters"},
                      auth=_auth(cfg), timeout=20,
                      headers={"Accept": "application/json"})
        if r.status_code != 200:
            return None
        params = r.json().get("requestParameters") or []
        for p in params:
            if p.get("name") == "submit.argument4":
                return str(p.get("value", "")).strip()
    except Exception as e:
        logger.debug("get_ji_group_id failed for %s: %s", ji_request_id, e)
    return None


def get_descendant_requests(cfg, parent_request_id: str) -> list[dict]:
    """
    Use Oracle's Scheduler REST API to find every descendant ESS request
    spawned by our original submission. The `absParentRequestId` field is set
    on every child Oracle creates on our behalf — including the separately
    spawned 'Import Journals' and 'Import Journals: Child' jobs.

    This is the AUTHORITATIVE way to correlate Oracle-spawned jobs back to
    our submission. Replaces the unreliable forward-scan + group_id approach.

    Endpoint:  GET /ess/rest/scheduler/v1/requests?q=absParentRequestId eq <id>
    Reference: Oracle Fusion Cloud Apps "Get job request information" REST API.

    Returns: list of {request_id, name, status, parent_request_id} dicts.
            Empty list on error or when endpoint isn't accessible.
    """
    base = cfg.fusion_url.rstrip("/")
    url  = f"{base}/ess/rest/scheduler/v1/requests"
    q    = f"absParentRequestId eq {parent_request_id}"
    params = {
        "q":      q,
        "limit":  100,
        "fields": "requestId,parentRequestId,absParentRequestId,name,state,executionState",
    }
    try:
        r = httpx.get(url, params=params, auth=_auth(cfg), timeout=30,
                      headers={"Accept": "application/json"})
        if r.status_code != 200:
            logger.warning("Scheduler REST returned %s for absParentRequestId=%s — "
                           "will fall back to forward scan",
                           r.status_code, parent_request_id)
            return []
        items = r.json().get("items", [])
        out = []
        for it in items:
            out.append({
                "request_id":         str(it.get("requestId") or ""),
                "parent_request_id":  str(it.get("parentRequestId") or ""),
                "name":               str(it.get("name") or ""),
                "status":             str(it.get("state") or it.get("executionState") or "").upper(),
            })
        logger.info("Scheduler REST found %d descendant(s) of %s",
                    len(out), parent_request_id)
        return out
    except Exception as e:
        logger.warning("get_descendant_requests failed for %s: %s",
                       parent_request_id, e)
        return []


def get_request_parameters(cfg, request_id: str) -> str:
    """
    Fetch the parameter/arg list for an ESS request. Used to correlate
    Import Journals jobs back to the submission that triggered them (by group_id).

    Oracle exposes parameter info under several possible field names depending on
    job type and instance config. We probe a few and return the first non-empty
    one as a concatenated string. Returns "" when nothing is retrievable.
    """
    url = f"{_base(cfg)}{ERPI}"
    # Request all known param-bearing fields at once
    field_str = "ESSParameters,ParameterList,RequestParameters,Parameters,RequestStatus,StatusCode"
    params = {"finder": f"ESSJobStatusRF;requestId={request_id}", "fields": field_str}
    try:
        r = httpx.get(url, params=params, auth=_auth(cfg), timeout=20,
                      headers={"Accept": "application/json"})
        if r.status_code == 200:
            items = r.json().get("items", [])
            if items:
                item = items[0]
                # Collect every plausible field; concatenate so a single `in` check works
                parts = []
                for k in ("ESSParameters", "ParameterList",
                          "RequestParameters", "Parameters"):
                    v = item.get(k)
                    if v:
                        parts.append(str(v))
                if not parts:
                    # Last-ditch: include the whole item dict as text so a numeric
                    # group_id buried in a nested structure still matches
                    parts.append(str(item))
                return " | ".join(parts)
    except Exception as e:
        logger.debug("get_request_parameters failed for %s: %s", request_id, e)
    return ""


def find_journal_import_jobs(cfg, after_request_id: str, scan_range: int = 20,
                             group_id: str = "", max_workers: int = 10) -> list[dict]:
    """
    Oracle's importBulkData returns the request ID of the file-loader job.
    The actual "Import Journals" / "Import Journals: Child" run as separate ESS requests
    with higher IDs. This function scans the next `scan_range` IDs in PARALLEL and
    returns any that are part of the Journal Import chain.

    When multiple submissions run concurrently, several users' Import Journals jobs land
    in the same ID range. Pass `group_id` (the numeric Interface Group Identifier used
    at submission time) to filter — only jobs whose ESSParameters contain that group_id
    will be returned, so each submission gets ITS OWN Import Journals jobs.

    Scans parallel HTTP requests with `max_workers` threads, and exits early as soon as
    we have BOTH a parent "Import Journals" and its "Import Journals: Child" matched.

    Returns: list of {request_id, name, status} dicts.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    try:
        start = int(after_request_id)
    except (ValueError, TypeError):
        return []

    group_id = (group_id or "").strip()
    rids = list(range(start + 1, start + scan_range + 1))

    def _probe(rid: int) -> list[dict]:
        """Fetch execution details for one rid; return matching JI rows."""
        out = []
        try:
            det = get_execution_details(cfg, str(rid))
            for j in det.get("child_jobs", []):
                name = (j.get("name") or "").strip()
                if "Import Journals" not in name and "JournalImport" not in name:
                    continue
                ji_rid = j.get("request_id") or str(rid)
                # AUTHORITATIVE correlation: each spawned Import Journals job
                # carries our submission's group_id as submit.argument4 in its
                # requestParameters (Scheduler REST API). Skip jobs whose
                # argument4 doesn't match our group_id.
                if group_id:
                    ji_group = get_ji_group_id(cfg, ji_rid)
                    if ji_group is None:
                        # Couldn't read params (newly spawned, transient 4xx).
                        # Skip and let the caller retry on the next scan iteration —
                        # safer than including a possibly-foreign job.
                        logger.debug("JI rid=%s — params not yet available, skipping",
                                     ji_rid)
                        continue
                    if ji_group != group_id:
                        logger.debug("JI rid=%s rejected: group=%s (ours=%s)",
                                     ji_rid, ji_group, group_id)
                        continue
                    logger.info("JI rid=%s matched group_id=%s", ji_rid, group_id)
                out.append({
                    "request_id":   ji_rid,
                    "name":         name,
                    "status":       j.get("status") or "",
                    "scanned_from": str(rid),
                })
        except Exception:
            pass
        return out

    found: list[dict] = []
    seen_rids: set[str] = set()
    have_parent = have_child = False

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_probe, rid): rid for rid in rids}
        for fut in as_completed(futures):
            for j in fut.result():
                if j["request_id"] in seen_rids:
                    continue
                seen_rids.add(j["request_id"])
                found.append(j)
                low = j["name"].lower()
                if "child" in low: have_child = True
                else:              have_parent = True
            # Early exit once we have OUR parent + child
            if group_id and have_parent and have_child:
                for f in futures:
                    f.cancel()
                break
    return found


def get_execution_details(cfg, request_id: str) -> dict:
    """
    Return the full job hierarchy + statuses for an ESS request via ESSExecutionDetailsRF.
    This includes the 'Import Journals: Child' job which reflects whether the GL Journal
    Import actually accepted rows. STATUS values: SUCCEEDED | WARNING | ERROR | RUNNING.

    A WARNING status from 'Import Journals: Child' means SOME or ALL rows were rejected
    even though the parent ESS request reported SUCCEEDED.

    Returns: {
        'parent_id': str, 'child_jobs': [{name, path, request_id, status}],
        'worst_child_status': 'SUCCEEDED' | 'WARNING' | 'ERROR' | '',
        'has_failures': bool, 'raw': dict
    }
    """
    import json as _json
    url = f"{_base(cfg)}{ERPI}"
    try:
        r = httpx.get(url, params={"finder": f"ESSExecutionDetailsRF;requestId={request_id}"},
                      auth=_auth(cfg), timeout=30, headers={"Accept": "application/json"})
        if r.status_code != 200:
            return {"parent_id": request_id, "child_jobs": [], "worst_child_status": "",
                    "has_failures": False, "raw": {}}
        items = r.json().get("items", [])
        if not items:
            return {"parent_id": request_id, "child_jobs": [], "worst_child_status": "",
                    "has_failures": False, "raw": {}}

        raw_status = items[0].get("RequestStatus", "")
        try:
            parsed = _json.loads(raw_status)
        except Exception:
            parsed = {}

        # JOBS can be a single object or a list — normalize to list
        jobs_field = parsed.get("JOBS", []) if isinstance(parsed, dict) else []
        if isinstance(jobs_field, dict):
            jobs_list = [jobs_field]
        elif isinstance(jobs_field, list):
            jobs_list = jobs_field
        else:
            jobs_list = []

        # Flatten any nested CHILD entries (some Oracle responses nest deeper)
        flat = []
        def _walk(node):
            if isinstance(node, dict):
                if node.get("JOBNAME") or node.get("REQUESTID"):
                    flat.append({
                        "name":       node.get("JOBNAME") or "",
                        "path":       node.get("JOBPATH") or "",
                        "request_id": node.get("REQUESTID") or "",
                        "status":     (node.get("STATUS") or "").upper(),
                    })
                if "CHILD" in node:  _walk(node["CHILD"])
                if "JOBS" in node:   _walk(node["JOBS"])
            elif isinstance(node, list):
                for x in node: _walk(x)
        for j in jobs_list:
            _walk(j)

        # Severity ranking
        order = {"SUCCEEDED": 0, "RUNNING": 1, "WARNING": 2, "ERROR": 3, "FAILED": 3, "CANCELLED": 3}
        worst = ""
        for j in flat:
            if order.get(j["status"], -1) > order.get(worst, -1):
                worst = j["status"]
        has_failures = worst in ("WARNING", "ERROR", "FAILED", "CANCELLED")

        return {
            "parent_id": request_id,
            "child_jobs": flat,
            "worst_child_status": worst,
            "has_failures": has_failures,
            "raw": parsed,
        }
    except Exception as e:
        logger.warning("ESSExecutionDetailsRF error: %s", e)
        return {"parent_id": request_id, "child_jobs": [], "worst_child_status": "",
                "has_failures": False, "raw": {}}


def scheduled_processes_url(cfg, request_id: str) -> str:
    """Build a direct URL to the Scheduled Processes page filtered to this request."""
    base = cfg.fusion_url.rstrip("/")
    return f"{base}/fscmUI/faces/FuseWelcome?fndGlobalItemNodeId=itemNode_tools_scheduled_processes&" \
           f"fndProcessId={request_id}"


def _try_download(cfg, request_id: str) -> bytes | None:
    """
    Try multiple Oracle endpoints / formats to retrieve a ZIP of log+output for one ESS request.
    Returns raw zip bytes or None. Valid FileType values: LOG | OUT | ALL.
    """
    import base64 as _b64
    url = f"{_base(cfg)}{ERPI}"
    auth = _auth(cfg)
    hdrs_json = {"Content-Type": "application/json", "Accept": "application/json"}
    hdrs_get  = {"Accept": "application/json"}

    # Variant A: GET finder ESSJobExecutionDetailsRF (canonical Oracle example)
    for ft in ("ALL", "LOG", "OUT"):
        for sep in (";", ","):
            finder = f"ESSJobExecutionDetailsRF;requestId={request_id}{sep}fileType={ft}"
            try:
                r = httpx.get(url, params={"finder": finder}, auth=auth, timeout=20, headers=hdrs_get)
                if r.status_code == 200:
                    items = r.json().get("items", [])
                    if items and items[0].get("DocumentContent"):
                        logger.info("Log download via GET finder fileType=%s succeeded", ft)
                        return _b64.b64decode(items[0]["DocumentContent"])
            except Exception:
                pass

    # Variant B: POST downloadESSJobExecutionDetails with proper ReqstId + FileType
    for ft in ("ALL", "LOG", "OUT"):
        payload = {
            "OperationName": "downloadESSJobExecutionDetails",
            "ReqstId":       str(request_id),
            "FileType":      ft,
            "DocumentContent": None,
            "DocumentId":    None,
            "FileName":      None,
            "ContentType":   None,
            "ParameterList": str(request_id),
        }
        try:
            r = httpx.post(url, json=payload, auth=auth, timeout=25, headers=hdrs_json)
            if r.status_code in (200, 201):
                j = r.json()
                if j.get("DocumentContent"):
                    logger.info("Log download via POST fileType=%s succeeded", ft)
                    return _b64.b64decode(j["DocumentContent"])
                # If we got a DocumentId, try getDocumentForDocumentId
                doc_id = j.get("DocumentId")
                if doc_id and doc_id != str(request_id):
                    payload2 = {"OperationName": "getDocumentForDocumentId", "DocumentId": doc_id}
                    r2 = httpx.post(url, json=payload2, auth=auth, timeout=120, headers=hdrs_json)
                    if r2.status_code in (200, 201):
                        b64 = r2.json().get("DocumentContent")
                        if b64:
                            logger.info("Log download via getDocumentForDocumentId(%s) succeeded", doc_id)
                            return _b64.b64decode(b64)
        except Exception:
            pass

    return None


def download_ess_logs(cfg, parent_request_id: str, group_id: str = "",
                      ji_jobs: list[dict] | None = None) -> dict:
    """
    Download log+output files for an ESS request hierarchy.
    Tries the parent first, then every direct child request ID.

    `ji_jobs` (optional): pre-claimed Import Journals jobs to also download.
    When provided, we DO NOT re-scan for JI jobs — we trust the caller's claim.
    This is how the workflow guarantees per-submission isolation under concurrency.
    """
    import io, zipfile as _zip
    combined: dict[str, bytes] = {}
    rid_to_name: dict[str, str] = {str(parent_request_id): "Load_Interface_File_for_Import"}

    # Build the list of request IDs to try — only our own jobs
    ids_to_try = [str(parent_request_id)]
    try:
        det = get_execution_details(cfg, parent_request_id)
        for j in det.get("child_jobs", []):
            cid = str(j.get("request_id") or "")
            if cid and cid not in ids_to_try:
                ids_to_try.append(cid)
                rid_to_name[cid] = (j.get("name") or "child").replace(" ", "_").replace(":", "")
    except Exception:
        pass

    # IMPORTANT: distinguish between
    #   ji_jobs=None  → caller didn't pre-claim, fall back to internal scan (legacy)
    #   ji_jobs=[]    → caller TRIED to claim but lost the race → no JI jobs belong
    #                   to this submission, DO NOT scan again
    #   ji_jobs=[...] → caller's claimed list, use it verbatim
    if ji_jobs is not None:
        for j in ji_jobs:
            cid = str(j.get("request_id", ""))
            if cid and cid not in ids_to_try:
                ids_to_try.append(cid)
                rid_to_name[cid] = (j.get("name") or "import_journals").replace(" ", "_").replace(":", "")
    elif group_id:
        # Legacy path — only when caller didn't pass ji_jobs at all
        try:
            for j in find_journal_import_jobs(cfg, parent_request_id, scan_range=20, group_id=group_id):
                cid = str(j["request_id"])
                if cid and cid not in ids_to_try:
                    ids_to_try.append(cid)
                    rid_to_name[cid] = (j.get("name") or "import_journals").replace(" ", "_").replace(":", "")
        except Exception:
            pass
    logger.info("Fetching logs for request IDs (group_id=%s, claimed_ji=%d): %s",
                group_id, len(ji_jobs or []), ids_to_try)

    for rid in ids_to_try:
        zb = _try_download(cfg, rid)
        if not zb:
            continue
        try:
            with _zip.ZipFile(io.BytesIO(zb)) as zf:
                for name in zf.namelist():
                    key = f"{rid}/{name}"  # prefix with request id to avoid collisions
                    if key not in combined:
                        combined[key] = zf.read(name)
        except Exception:
            # Not a zip — treat as a single file
            combined[f"{rid}/output.txt"] = zb

    if not combined:
        return {"zip_bytes": None, "files": {}, "all_text": "", "rid_to_name": rid_to_name}

    # Re-pack into a single ZIP for email attachment
    out_buf = io.BytesIO()
    with _zip.ZipFile(out_buf, "w", _zip.ZIP_DEFLATED) as zf:
        for name, data in combined.items():
            zf.writestr(name, data)
    zip_bytes = out_buf.getvalue()

    files = {}
    for name, data in combined.items():
        try:
            files[name] = data.decode("utf-8", errors="replace")
        except Exception:
            pass
    all_text = "\n\n".join(f"--- {k} ---\n{v}" for k, v in files.items())
    return {"zip_bytes": zip_bytes, "files": files, "all_text": all_text,
            "rid_to_name": rid_to_name}


def analyze_ess_logs(logs: dict) -> dict:
    """
    Inspect downloaded logs for Journal Import errors.

    Skips the static "Error Key" legend section (which lists ALL possible error
    codes in every report) and only counts error codes that actually appear
    next to data lines in the "Error Lines" table.
    """
    import re
    text = logs.get("all_text", "")
    if not text:
        return {"has_errors": False, "has_warnings": False, "error_count": 0,
                "summary": "No log content available.", "detail_lines": []}

    # Drop the static "Error Key" legend so we don't false-positive on its codes
    legend_match = re.search(r"={5,}\s*Error Key\s*={5,}", text)
    body = text[:legend_match.start()] if legend_match else text

    detail: list[str] = []

    # 1. SQL*Loader errors (always real)
    sql_errors = re.findall(r"Record \d+: Rejected.*?\n[^\n]*ORA-\d+:[^\n]*", body)
    detail.extend(sql_errors)

    # 2. JI "Error Lines" section: error code followed by Source name and amounts/accounts
    #    e.g. "EF04                           Manual                         2026-05-16  USD ..."
    # Capture the FULL line so the UI can show the offending row, not just the code.
    err_line_matches = list(re.finditer(
        r"^(E[A-Z]{1,3}\d{1,3})\s+(Manual|Spreadsheet|Payables|Receivables|\w+)\s+\d{4}[^\n]*",
        body, flags=re.MULTILINE))
    real_errors = [(m.group(1), m.group(2)) for m in err_line_matches]
    err_codes = sorted(set(c for c, _src in real_errors))
    # Append each unique error line (cap to keep stop_reason readable)
    seen_lines: set[str] = set()
    for m in err_line_matches:
        line = m.group(0).strip()
        # Collapse runs of spaces so the formatted log isn't jagged
        compact = re.sub(r"\s{2,}", "  ", line)
        if compact not in seen_lines:
            seen_lines.add(compact)
            detail.append(compact)
        if len(seen_lines) >= 20:
            break

    # 2b. Error Key legend descriptions for ONLY the codes that actually
    #     appeared above. Oracle's JI logs document each code in a section
    #     that looks like:
    #         ====== Error Key ======
    #         EF04  Account combination flagged 'detail posting not allowed'.
    #         EF05  ...
    legend_text = text[legend_match.start():] if legend_match else ""
    if legend_text and err_codes:
        for code in err_codes:
            # Each legend entry: code at line start, then description until blank line / next code
            m = re.search(rf"^{re.escape(code)}\s+(.+?)(?=\n\s*\n|\n\s*E[A-Z]{{1,3}}\d{{1,3}}\s|\Z)",
                          legend_text, flags=re.MULTILINE | re.DOTALL)
            if m:
                desc = re.sub(r"\s+", " ", m.group(1)).strip()
                detail.append(f"{code}: {desc[:240]}")

    # 3. Invalid account problem descriptions
    invalid_acct = re.findall(
        r"(?:FLEX-DATA NOT ENTERED|FLEX-VALUE DOES NOT EXIST|"
        r"This new code combination includes summary segment|"
        r"Detail posting isn[\'’]t allowed)[^\n]*", body)
    detail.extend(invalid_acct[:20])

    # 4. SQL*Loader rejection count
    rejected = re.search(r"Total logical records rejected:\s+(\d+)", body)
    n_rejected = int(rejected.group(1)) if rejected else 0

    # 5. JI Totals row showing Error/Warning status
    #    "Manual    746648255 Error    4 ..."  vs  "Manual    563764730 Success  4 ..."
    totals_row = re.search(
        r"(Manual|Spreadsheet|Payables|Receivables|\w+)\s+\d{6,}\s+(Error|Warning|Success)\s+\d+",
        body)
    inner_status = totals_row.group(2).upper() if totals_row else ""

    # 6. "Silent" Journal Import failure: launch_journal_import scans
    #    GL_INTERFACE.group_id and finds nothing. Oracle reports SUCCEEDED at
    #    the ESS level even though no rows were posted. Catch this explicitly.
    zero_groups = re.search(r"Total:\s*0\s+group\s+id\(s\)", body, re.IGNORECASE)
    if zero_groups:
        detail.append("launch_journal_import: Total: 0 group id(s). "
                      "Rows loaded into GL_INTERFACE did not match the JI "
                      "group_id parameter — nothing was posted.")

    # 7. SQL*Loader loaded 0 rows (file format or all-rows-rejected)
    zero_loaded = re.search(
        r"Table\s+\w+\s*:\s*\n\s*0\s+Rows successfully loaded", body)
    if zero_loaded:
        detail.append("SQL*Loader: 0 Rows successfully loaded.")

    has_errors  = bool(sql_errors or invalid_acct or n_rejected > 0
                       or inner_status == "ERROR" or err_codes
                       or zero_groups or zero_loaded)
    has_warnings = inner_status == "WARNING" and not has_errors

    parts = []
    if err_codes:    parts.append(f"JI error codes: {', '.join(err_codes)}")
    if n_rejected:   parts.append(f"SQL*Loader rejected {n_rejected} rows")
    if invalid_acct: parts.append(f"{len(invalid_acct)} invalid account problems")
    if zero_groups:  parts.append("Journal Import found 0 matching group_id rows")
    if zero_loaded:  parts.append("SQL*Loader loaded 0 rows")
    if inner_status: parts.append(f"JI status: {inner_status}")
    summary = "; ".join(parts) or ("Errors found in log." if has_errors else "No errors detected.")

    return {
        "has_errors": has_errors,
        "has_warnings": has_warnings,
        "error_count": len(detail),
        "summary": summary,
        "detail_lines": detail[:30],
    }


# ── Silent Interface Purge ────────────────────────────────────────────────────

def purge_interface_rows(cfg, group_id: str, ledger_id: str = "",
                        je_source: str = "Manual") -> bool:
    """Submit Oracle's Purge Journal Import Interface ESS job for our group_id.
    Best-effort: returns True if accepted, False on any error.
    Silent — logs but never raises."""
    if not group_id:
        return False
    url = f"{_base(cfg)}/ess/rest/scheduler/v1/requests"
    job_attempts = [
        {"name": "Purge Journal Import Interface",
         "jobDefinitionName": "JournalImportPurgeJob"},
        {"name": "JournalImportPurgeJob",
         "jobDefinitionName": "JournalImportPurgeJob"},
    ]
    for attempt in job_attempts:
        payload = {
            "operation": "submitRequest",
            "name": attempt["name"],
            "jobDefinitionName": attempt["jobDefinitionName"],
            "applicationName": "FscmEss",
            "parameterList": [str(group_id), str(ledger_id or ""), je_source or "Manual"],
        }
        try:
            r = httpx.post(url, json=payload, auth=_auth(cfg), timeout=20,
                           headers={"Content-Type": "application/json",
                                    "Accept": "application/json"})
            if r.status_code in (200, 201, 202):
                logger.info("Submitted GL_INTERFACE purge: group_id=%s ledger_id=%s status=%d",
                            group_id, ledger_id, r.status_code)
                return True
        except Exception as e:
            logger.debug("purge_interface_rows attempt failed (%s): %s",
                         attempt["name"], e)
    logger.warning("purge_interface_rows: could not submit purge for "
                   "group_id=%s ledger_id=%s (all job-name variants failed)",
                   group_id, ledger_id)
    return False


# ── Currency Conversion Rate ──────────────────────────────────────────────────

_RATE_CACHE: dict[tuple, float] = {}
# Hardcoded fallback for common currency pairs (mid-2024 mid-market rates).
_FX_FALLBACK = {
    ("USD","USD"): 1.00, ("USD","INR"): 83.0, ("USD","EUR"): 0.92,
    ("USD","GBP"): 0.79, ("USD","JPY"): 149.0, ("USD","AUD"): 1.52,
    ("USD","CAD"): 1.36, ("USD","CNY"): 7.24, ("USD","SGD"): 1.34,
    ("INR","USD"): 0.012, ("EUR","USD"): 1.09, ("GBP","USD"): 1.27,
    ("EUR","INR"): 90.0, ("GBP","INR"): 105.0,
}


def get_conversion_rate(cfg, from_curr: str, to_curr: str, rate_date: str,
                       rate_type: str = "Corporate", req_id: str = "") -> float:
    """Return FX rate from_curr → to_curr on rate_date. Tries Oracle's Daily
    Rates REST API first; falls back to a hardcoded table on miss/error.
    rate_date format: 'YYYY-MM-DD'.

    When `req_id` is provided, every step (REST hit, REST miss + fallback,
    inverse fallback, final default) is appended to that request's process
    logs so users can see the path that was taken on the request detail page.
    """
    def _proc_log(level: str, msg: str):
        if not req_id:
            return
        try:
            from database import append_log
            append_log(req_id, level, msg)
        except Exception:
            pass

    fc = (from_curr or "").strip().upper()
    tc = (to_curr or "").strip().upper()
    if not fc or not tc:
        return 1.0
    if fc == tc:
        _proc_log("INFO", f"FX rate {fc}→{tc}: 1.0 (same currency, no REST call)")
        return 1.0
    key = (fc, tc, rate_date, rate_type)
    if key in _RATE_CACHE:
        cached = _RATE_CACHE[key]
        _proc_log("INFO", f"FX rate {fc}→{tc} on {rate_date}: {cached} (cache hit)")
        return cached

    # 1. Try Oracle's currencyRates REST endpoint via CurrencyRatesFinder.
    #    Endpoint shape was verified against a live Fusion tenant:
    #    GET /fscmRestApi/resources/11.13.18.05/currencyRates
    #      ?finder=CurrencyRatesFinder;fromCurrency=USD,toCurrency=INR,
    #              startDate=2025-12-16,endDate=2025-12-16,
    #              currencyConversionType=Corporate
    #    Returns items: [{ConversionRate: 83.767, ...}].
    #    Field names are CASE-SENSITIVE and require comma separators inside the
    #    finder. The (legacy) /dailyRates path returns 404 in modern tenants.
    rest_endpoint = f"{_base(cfg)}/fscmRestApi/resources/11.13.18.05/currencyRates"
    def _try_direction(src: str, dst: str) -> "float | None":
        finder = (f"CurrencyRatesFinder;fromCurrency={src},toCurrency={dst},"
                  f"startDate={rate_date},endDate={rate_date},"
                  f"currencyConversionType={rate_type}")
        _proc_log("INFO",
                  f"FX rate REST call: GET {rest_endpoint}?finder={finder}")
        try:
            resp = httpx.get(rest_endpoint,
                             params={"finder": finder, "limit": 5,
                                     "fields": "FromCurrency,ToCurrency,ConversionDate,ConversionRate"},
                             auth=_auth(cfg), timeout=15,
                             headers={"Accept": "application/json"})
            if resp.status_code != 200:
                _proc_log("WARNING",
                          f"FX rate REST returned status {resp.status_code} for "
                          f"{src}→{dst} on {rate_date}")
                return None
            items = resp.json().get("items", [])
            if not items:
                _proc_log("WARNING",
                          f"FX rate REST returned no items for {src}→{dst} on {rate_date}")
                return None
            rv = items[0].get("ConversionRate")
            if rv is None:
                return None
            try:
                rate_f = float(rv)
                if rate_f > 0:
                    return rate_f
            except (TypeError, ValueError):
                pass
            return None
        except Exception as e:
            logger.debug("Oracle currencyRates lookup failed for %s→%s: %s", src, dst, e)
            _proc_log("WARNING",
                      f"FX rate REST call raised ({e}) for {src}→{dst} — falling back")
            return None

    # Direct: from→to
    rate = _try_direction(fc, tc)
    if rate is not None:
        _RATE_CACHE[key] = rate
        logger.info("FX rate via Oracle REST: %s→%s on %s = %s", fc, tc, rate_date, rate)
        _proc_log("INFO",
                  f"FX rate {fc}→{tc} on {rate_date}: {rate} "
                  f"(Oracle currencyRates REST — direct)")
        return rate
    # Inverse: try to→from, then 1/rate
    inv = _try_direction(tc, fc)
    if inv is not None and inv > 0:
        rate = 1.0 / inv
        _RATE_CACHE[key] = rate
        logger.info("FX rate via Oracle REST (inverse): %s→%s = %.6f", fc, tc, rate)
        _proc_log("INFO",
                  f"FX rate {fc}→{tc} on {rate_date}: {rate:.6f} "
                  f"(Oracle currencyRates REST — inverse of {tc}→{fc}={inv})")
        return rate

    # 2. Fallback table — direct
    if (fc, tc) in _FX_FALLBACK:
        rate = _FX_FALLBACK[(fc, tc)]
        _RATE_CACHE[key] = rate
        logger.warning("FX rate via fallback table: %s→%s = %s (REST unavailable)",
                       fc, tc, rate)
        _proc_log("INFO",
                  f"FX rate {fc}→{tc}: {rate} (hardcoded fallback table)")
        return rate
    # 3. Inverse
    if (tc, fc) in _FX_FALLBACK and _FX_FALLBACK[(tc, fc)]:
        rate = 1.0 / _FX_FALLBACK[(tc, fc)]
        _RATE_CACHE[key] = rate
        logger.warning("FX rate via inverse fallback: %s→%s = %.6f", fc, tc, rate)
        _proc_log("INFO",
                  f"FX rate {fc}→{tc}: {rate:.6f} (inverse of {tc}→{fc} fallback)")
        return rate

    # 4. Final fallback
    logger.warning("FX rate unknown for %s→%s — defaulting to 1.0", fc, tc)
    _proc_log("WARNING",
              f"FX rate unknown for {fc}→{tc} — defaulting to 1.0 "
              f"(neither REST nor fallback table has this pair)")
    _RATE_CACHE[key] = 1.0
    return 1.0


# ── Connection Test ───────────────────────────────────────────────────────────

def test_connection(cfg) -> dict:
    """Quick test of Oracle Fusion connectivity. Returns {ok, message}."""
    try:
        resp = httpx.get(
            f"{_base(cfg)}{ERPI}",
            auth=_auth(cfg), timeout=15,
            headers={"Accept": "application/json"},
        )
        if resp.status_code in (200, 201):
            return {"ok": True, "message": f"Connected ✅  (HTTP {resp.status_code})"}
        return {"ok": False, "message": f"HTTP {resp.status_code} — check credentials"}
    except Exception as e:
        return {"ok": False, "message": str(e)}
