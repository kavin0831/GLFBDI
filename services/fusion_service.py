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
def submit_fbdi(cfg, zip_path: str, group_id: str = "") -> dict:
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
    ledger = cfg.fusion_ledger_name or "US Primary Ledger"
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
        "CallbackURL":     "#NULL",
        "NotificationCode":"10",
        "JobOptions":      "EnableEvent=Y,importOption=Y,purgeOption=Y,ExtractFileType!= NONE",
    }

    url = f"{_base(cfg)}{ERPI}"
    logger.info("Submitting FBDI to Oracle Fusion: %s (%.1f KB)", url, len(zip_bytes)/1024)

    resp = httpx.post(url, json=payload, auth=_auth(cfg), timeout=120,
                      headers={"Content-Type": "application/json", "Accept": "application/json"})
    resp.raise_for_status()
    data = resp.json()
    logger.info("Submission response: ReqstId=%s", data.get("ReqstId"))
    return data


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
    real_errors = re.findall(
        r"^(E[A-Z]{1,3}\d{1,3})\s+(Manual|Spreadsheet|Payables|Receivables|\w+)\s+\d{4}",
        body, flags=re.MULTILINE)
    err_codes = sorted(set(c for c, _src in real_errors))

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

    has_errors  = bool(sql_errors or invalid_acct or n_rejected > 0
                       or inner_status == "ERROR" or err_codes)
    has_warnings = inner_status == "WARNING" and not has_errors

    parts = []
    if err_codes:    parts.append(f"JI error codes: {', '.join(err_codes)}")
    if n_rejected:   parts.append(f"SQL*Loader rejected {n_rejected} rows")
    if invalid_acct: parts.append(f"{len(invalid_acct)} invalid account problems")
    if inner_status: parts.append(f"JI status: {inner_status}")
    summary = "; ".join(parts) or ("Errors found in log." if has_errors else "No errors detected.")

    return {
        "has_errors": has_errors,
        "has_warnings": has_warnings,
        "error_count": len(detail),
        "summary": summary,
        "detail_lines": detail[:30],
    }


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
