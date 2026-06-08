"""
Oracle Fusion GL FBDI Automation — Main Application
Run: python app.py  OR  uvicorn app:app --reload
No login required. Settings managed via /settings page.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

# Load .env BEFORE importing anything that reads os.environ (database.py does)
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI, Form, Request, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, FileResponse
from typing import List
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from database import (
    AppSettings, JournalRequest, SessionLocal, get_settings, init_db,
    store_log_file, store_secure_file, get_process_logs, get_log_files_meta,
    store_uploaded_file, get_uploaded_file, store_generated_file,
    get_generated_file, has_generated_file, append_log, _mdb,
)
from services.fusion_service import test_connection
from services.gmail_service import gmail_available
from workflow import process_request
from workflow_ap import process_ap_request


def _dispatch_processing(req_id: str, txn_type: str = "GL") -> None:
    """Pick the right pipeline based on transaction_type ('GL' or 'AP')."""
    if (txn_type or "").upper() == "AP":
        threading.Thread(target=process_ap_request, args=(req_id,), daemon=True).start()
    else:
        threading.Thread(target=process_request, args=(req_id,), daemon=True).start()

# ── Storage dirs (must exist before logging) ──────────────────────────────────
STORAGE = Path(__file__).parent / "storage"
STORAGE.mkdir(exist_ok=True)
(STORAGE / "logs").mkdir(exist_ok=True)
(STORAGE / "uploads").mkdir(exist_ok=True)
(STORAGE / "fbdi").mkdir(exist_ok=True)
(Path(__file__).parent / "config").mkdir(exist_ok=True)

# ── Logging ───────────────────────────────────────────────────────────────────
# Force UTF-8 on the console so emoji / arrows in log messages don't crash
# the StreamHandler under Windows cp1252.
import sys as _sys
try:
    _sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    _sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

_stream_handler = logging.StreamHandler(_sys.stdout)
_file_handler   = logging.FileHandler(str(STORAGE / "logs" / "app.log"),
                                       mode="a", encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[_stream_handler, _file_handler],
)
logger = logging.getLogger(__name__)

# ── Lifespan ──────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    logger.info("Database ready: MongoDB")
    threading.Thread(target=_preload_ml, daemon=True).start()
    threading.Thread(target=_gmail_poll_loop, daemon=True).start()
    logger.info("Oracle Fusion GL FBDI Automation started — http://localhost:8000")
    yield

# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(title="Oracle Fusion GL FBDI Automation", docs_url="/api/docs", lifespan=lifespan)
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")


def _preload_ml():
    try:
        from services.ml_mapper import _get_model
        _get_model()
        logger.info("ML mapper model loaded and ready")
    except Exception as e:
        logger.warning("ML model pre-load failed (will load on first use): %s", e)


def _gmail_poll_loop():
    """Background thread: poll Gmail inbox every N seconds."""
    time.sleep(5)  # brief startup delay
    while True:
        try:
            _poll_gmail_once()
        except Exception as e:
            logger.warning("Gmail poll error: %s", e)
        cfg = get_settings()
        time.sleep(cfg.gmail_poll_seconds)


def _poll_gmail_once():
    if not gmail_available():
        return
    cfg = get_settings()

    # Prefer App Password (IMAP) when configured — works on any host
    try:
        from services.gmail_imap import (app_password_available,
                                          fetch_unprocessed_messages as _imap_fetch,
                                          mark_processed as _imap_mark)
        if app_password_available():
            messages = _imap_fetch()
            if messages:
                logger.info("IMAP poll: %d unprocessed journal email(s)", len(messages))
            for msg in messages:
                try:
                    for att in msg["attachments"]:
                        r_id = str(uuid.uuid4())
                        if not _store_and_verify_upload(r_id, att["file_name"],
                                                       att["file_bytes"], "IMAP"):
                            continue  # already logged inside helper
                        _create_and_process(r_id, msg["message_id"], msg["sender"],
                                            msg["subject"], att["file_name"],
                                            att["file_type"], att["file_size_bytes"], cfg)
                    _imap_mark(msg["imap_uid"])
                except Exception as e:
                    logger.error("IMAP processing failed for %s: %s",
                                 msg.get("message_id"), e)
            return
    except Exception as e:
        logger.warning("IMAP poll path failed, falling back to OAuth: %s", e)

    # Fallback: OAuth Gmail API path (only used when App Password isn't set)
    from services.gmail_service import (
        download_attachments, fetch_unprocessed_emails,
        get_gmail_service, mark_failed, mark_processed,
    )
    service = get_gmail_service()
    messages = fetch_unprocessed_emails(service)
    if not messages:
        return
    logger.info("Gmail OAuth: %d unprocessed journal email(s)", len(messages))
    for msg in messages:
        try:
            files = download_attachments(service, msg["id"])
            if not files:
                logger.info("No .csv/.txt/.zip attachments in %s — marking failed", msg["id"])
                mark_failed(service, msg["id"])
                continue
            for att in files:
                r_id = str(uuid.uuid4())
                if not _store_and_verify_upload(r_id, att["file_name"],
                                               att["file_bytes"], "Gmail-OAuth"):
                    continue
                _create_and_process(r_id, msg["id"], att["sender"], att["subject"],
                                    att["file_name"], att["file_type"],
                                    att["file_size_bytes"], cfg)
            mark_processed(service, msg["id"])
        except Exception as e:
            logger.error("Gmail processing failed for %s: %s", msg["id"], e)
            try: mark_failed(service, msg["id"])
            except Exception: pass


def _store_and_verify_upload(req_id: str, filename: str, content,
                             source_label: str) -> bool:
    """Store an emailed attachment in MongoDB and verify it's actually
    retrievable before the workflow kicks off. Returns False (and skips
    the request) when storage silently fails — previously this could
    happen with Fernet encryption errors and leave the Downloads sidebar
    empty for Gmail-polled requests."""
    try:
        if isinstance(content, str):
            content = content.encode("utf-8", errors="replace")
        if not isinstance(content, (bytes, bytearray)):
            logger.error("%s upload skipped — attachment %s is not bytes (%s)",
                         source_label, filename, type(content).__name__)
            return False
        if not content:
            logger.error("%s upload skipped — attachment %s is empty",
                         source_label, filename)
            return False
        store_uploaded_file(req_id, filename, content)
        # Verify the round trip — if encryption or upsert failed silently,
        # this returns None and we abandon the request rather than create
        # an orphaned MongoDB row with no source file.
        check = get_uploaded_file(req_id)
        if check is None or len(check) != len(content):
            logger.error("%s upload verification failed for %s (req=%s) — "
                         "storage call did not persist the bytes",
                         source_label, filename, req_id)
            return False
        logger.info("%s upload stored & verified: %s -> req=%s (%d bytes)",
                    source_label, filename, req_id, len(content))
        return True
    except Exception as e:
        logger.error("%s upload error for %s (req=%s): %s",
                     source_label, filename, req_id, e)
        return False


def _create_and_process(req_id, email_id, sender, subject, file_name,
                        file_type, file_size, cfg):
    # Route by subject keyword — AP if AP filter matches, else GL.
    txn = "GL"
    ap_filter = (getattr(cfg, "gmail_subject_filter_ap", "") or "").strip().lower()
    if ap_filter and ap_filter in (subject or "").lower():
        txn = "AP"
    extra = {}
    if txn == "AP":
        extra.update(
            ap_business_unit_id   = cfg.ap_business_unit_id,
            ap_business_unit_name = cfg.ap_business_unit_name,
            ap_ledger_id          = cfg.ap_ledger_id,
            ap_source             = cfg.ap_source,
            ap_pay_group          = cfg.ap_pay_group,
            ap_invoice_group      = cfg.ap_invoice_group,
        )
    with SessionLocal() as db:
        req = JournalRequest(
            id=req_id, email_id=email_id, sender_email=sender,
            email_subject=subject, file_name=file_name,
            file_path=file_name,  # virtual: file lives in MongoDB
            file_type=file_type, file_size_bytes=file_size,
            status="RECEIVED", current_stage="QUEUED",
            transaction_type=txn,
            **extra,
        )
        db.add(req); db.commit()
    _dispatch_processing(req_id, txn)
    logger.info("Queued Gmail %s attachment for %s: %s (%d bytes, MongoDB)",
                txn, req_id, file_name, file_size)


# ── Dashboard ─────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, txn: str = "gl"):
    txn_u = (txn or "gl").upper()
    if txn_u not in ("GL", "AP"): txn_u = "GL"
    with SessionLocal() as db:
        reqs = (db.query(JournalRequest)
                  .filter({"transaction_type": txn_u})
                  .order_by(("created_at", -1))
                  .limit(50).all())
        total    = db.query(JournalRequest).filter_by(transaction_type=txn_u).count()
        success  = db.query(JournalRequest).filter(
            {"transaction_type": txn_u, "status": "SUCCEEDED"}).count()
        warnings = db.query(JournalRequest).filter(
            {"transaction_type": txn_u, "status": "SUCCEEDED_WITH_WARNINGS"}).count()
        failed   = db.query(JournalRequest).filter(
            {"transaction_type": txn_u, "status": "FAILED"}).count()
        running  = db.query(JournalRequest).filter(
            {"transaction_type": txn_u,
             "status": {"$in": ["RECEIVED", "PROCESSING"]}}).count()
    return templates.TemplateResponse("dashboard.html", {
        "request": request, "requests": reqs, "txn": txn_u,
        "total": total, "success": success, "warnings": warnings,
        "failed": failed, "running": running,
        "gmail_ok": gmail_available(),
    })


# ── All Requests page (no row limit, with filter + search) ────────────────────
@app.get("/requests", response_class=HTMLResponse)
async def all_requests(request: Request, q: str = "", status: str = "", txn: str = ""):
    """Full listing of every request, with optional search, status and txn filter."""
    q = (q or "").strip()
    status = (status or "").strip()
    txn_f  = (txn or "").strip().upper()
    if txn_f not in ("GL", "AP", ""):
        txn_f = ""
    with SessionLocal() as db:
        base: dict = {}
        if txn_f:
            base["transaction_type"] = txn_f
        total    = db.query(JournalRequest).filter(base).count()
        success  = db.query(JournalRequest).filter({**base, "status": "SUCCEEDED"}).count()
        warnings = db.query(JournalRequest).filter({**base, "status": "SUCCEEDED_WITH_WARNINGS"}).count()
        failed   = db.query(JournalRequest).filter({**base, "status": "FAILED"}).count()
        running  = db.query(JournalRequest).filter(
            {**base, "status": {"$in": ["RECEIVED", "PROCESSING"]}}).count()

        # Build the filter for the result list
        mongo_filter: dict = dict(base)
        if status:
            mongo_filter["status"] = status
        if q:
            # Case-insensitive substring across file_name / sender_email / email_subject
            import re as _re
            rx = {"$regex": _re.escape(q), "$options": "i"}
            mongo_filter["$or"] = [
                {"file_name":     rx},
                {"sender_email":  rx},
                {"email_subject": rx},
                {"journal_name":  rx},
            ]
        reqs = (db.query(JournalRequest)
                  .filter(mongo_filter)
                  .order_by(("created_at", -1))
                  .all())
    return templates.TemplateResponse("requests_all.html", {
        "request": request, "requests": reqs,
        "total": total, "success": success, "warnings": warnings,
        "failed": failed, "running": running,
        "q": q, "status_filter": status, "txn_filter": txn_f,
    })


# ── Request detail ────────────────────────────────────────────────────────────
@app.get("/request/{req_id}", response_class=HTMLResponse)
async def request_detail(request: Request, req_id: str):
    with SessionLocal() as db:
        req = db.get(JournalRequest, req_id)
        if not req:
            return HTMLResponse("Not found", status_code=404)
    logs = get_process_logs(req_id)
    log_files = get_log_files_meta(req_id)
    all_files = _all_files_for_request(req_id)
    return templates.TemplateResponse("request_detail.html", {
        "request": request, "req": req,
        "process_logs": logs, "log_files": log_files,
        "all_files": all_files,
    })


@app.get("/api/request/{req_id}/status")
async def api_status(req_id: str):
    with SessionLocal() as db:
        req = db.get(JournalRequest, req_id)
        if not req: return JSONResponse({"error":"not found"}, status_code=404)
    return {"status": req.status, "stage": req.current_stage,
            "ess_status": req.ess_final_status, "fusion_request_id": req.fusion_request_id}


@app.get("/api/request/{req_id}")
async def api_request_detail(req_id: str):
    """Full request data as JSON — for testing and integrations."""
    with SessionLocal() as db:
        req = db.get(JournalRequest, req_id)
        if not req: return JSONResponse({"error":"not found"}, status_code=404)
    from database import get_process_logs, get_log_files_meta
    logs = get_process_logs(req_id)
    log_files = get_log_files_meta(req_id)
    raw = req._raw()
    # Serialise datetime fields
    for k, v in raw.items():
        if hasattr(v, "isoformat"):
            raw[k] = v.isoformat()
    return JSONResponse({
        **raw,
        "process_logs": logs,
        "log_files": {k: {kk: str(vv) for kk, vv in m.items()} for k, m in log_files.items()},
    })


# ── Manual upload ─────────────────────────────────────────────────────────────
@app.get("/upload", response_class=HTMLResponse)
async def upload_page(request: Request):
    return templates.TemplateResponse("upload.html",
                                       {"request": request, "cfg": get_settings()})


_ALLOWED_UPLOAD_EXT_GL = {".zip", ".csv", ".txt"}
_ALLOWED_UPLOAD_EXT_AP = {".zip", ".csv"}     # AP: only zip + csv (and zip must contain CSVs)


def _check_ap_zip_has_csv(content: bytes) -> tuple[bool, str]:
    """For AP uploads: a ZIP must contain at least one .csv file inside."""
    import zipfile as _zf, io as _io
    try:
        with _zf.ZipFile(_io.BytesIO(content)) as zf:
            for n in zf.namelist():
                if n.lower().endswith(".csv"):
                    return True, ""
            return False, "AP ZIP must contain at least one .csv file inside"
    except Exception as e:
        return False, f"could not read ZIP: {e}"


def _classify_ap_csv(filename: str, content: bytes) -> str:
    """
    Return "header" | "lines" | "flat" | "unknown" for an AP CSV based on
    filename hint first, then content header sniffing.
    """
    n = (filename or "").lower()
    if "apinvoiceslineinterface" in n or "apinvoicelinesinterface" in n or "_lines" in n:
        return "lines"
    if "apinvoicesinterface" in n or "_header" in n or "_hdr" in n:
        return "header"
    # Content sniff: peek at first 600 bytes
    try:
        head = content[:800].decode("utf-8", errors="replace").lower()
    except Exception:
        head = ""
    # Header file has "*business unit", "*invoice number", supplier columns
    if "business unit" in head and ("invoice number" in head or "*invoice number" in head) \
       and "supplier" in head and "line type" not in head:
        return "header"
    # Line file has "*line type", "distribution combination/set"
    if "*line type" in head or "line type" in head and "amount" in head and "distribution" in head:
        return "lines"
    # Otherwise it's a flat journal (single CSV with everything)
    return "flat"


def _pack_ap_pair_zip(header_bytes: bytes, lines_bytes: bytes) -> bytes:
    """Pack a pre-built header + lines pair into a single FBDI ZIP."""
    import zipfile as _zf, io as _io
    buf = _io.BytesIO()
    with _zf.ZipFile(buf, "w", _zf.ZIP_DEFLATED) as zf:
        zf.writestr("ApInvoicesInterface.csv",     header_bytes)
        zf.writestr("ApInvoiceLinesInterface.csv", lines_bytes)
    return buf.getvalue()


def _pair_ap_csv_uploads(
    file_blobs: list[tuple[str, bytes]],
) -> tuple[list[tuple[str, bytes, str]], list[tuple[str, bytes]], list[str]]:
    """
    Look at all the uploaded files. If there are matching ApInvoicesInterface
    + ApInvoiceLinesInterface pairs, pack each pair into one ZIP.

    Returns:
      paired_zips  : [(virtual_zip_name, zip_bytes, "paired"), ...]
      passthrough  : [(name, bytes), ...]  — flat CSVs or singleton files
      warnings     : list of human-readable messages
    """
    headers: list[tuple[str, bytes]] = []
    lines:   list[tuple[str, bytes]] = []
    flats:   list[tuple[str, bytes]] = []
    warns:   list[str] = []

    for name, content in file_blobs:
        kind = _classify_ap_csv(name, content)
        if kind == "header":  headers.append((name, content))
        elif kind == "lines": lines.append((name, content))
        else:                 flats.append((name, content))

    paired: list[tuple[str, bytes, str]] = []
    # If we have header + lines candidates, pair them by order
    if headers and lines:
        # Each pair → one FBDI zip submission. If counts mismatch, pair as many
        # as possible and warn about the orphans.
        n = min(len(headers), len(lines))
        for i in range(n):
            h_name, h_bytes = headers[i]
            l_name, l_bytes = lines[i]
            zip_bytes = _pack_ap_pair_zip(h_bytes, l_bytes)
            paired.append((f"AP_Batch_{i+1}_{Path(h_name).stem}.zip",
                            zip_bytes, "paired"))
        if len(headers) != len(lines):
            extra_h = headers[n:]; extra_l = lines[n:]
            for n2, _ in extra_h:
                warns.append(f"{n2}: unpaired AP header CSV (no matching lines file uploaded)")
            for n2, _ in extra_l:
                warns.append(f"{n2}: unpaired AP lines CSV (no matching header file uploaded)")
        return paired, flats, warns

    # Header without lines or lines without header — error rather than silently
    # creating broken submissions
    for n2, _ in headers:
        warns.append(f"{n2}: AP header CSV uploaded without a matching ApInvoiceLinesInterface.csv")
    for n2, _ in lines:
        warns.append(f"{n2}: AP lines CSV uploaded without a matching ApInvoicesInterface.csv")
    return paired, flats, warns


@app.post("/upload")
async def upload_file(
    request: Request,
    files: List[UploadFile] = File(...),
    transaction_type: str = Form("GL"),
    ledger_name: str = Form(""),
    accounting_date: str = Form(""),
    journal_name: str = Form(""),
    currency: str = Form("USD"),
    period_name: str = Form(""),
    # AP-specific overrides (optional — fall back to /settings AP defaults)
    ap_business_unit_id: str = Form(""),
    ap_business_unit_name: str = Form(""),
    ap_ledger_id: str = Form(""),
    ap_source: str = Form(""),
    ap_pay_group: str = Form(""),
    ap_invoice_group: str = Form(""),
    legal_entity: str = Form(""),
):
    cfg = get_settings()
    txn = (transaction_type or "GL").upper()
    allowed_ext = _ALLOWED_UPLOAD_EXT_AP if txn == "AP" else _ALLOWED_UPLOAD_EXT_GL
    req_ids = []
    errors = []

    # ── AP multi-CSV pairing ──────────────────────────────────────────────
    # When the user uploads ApInvoicesInterface.csv + ApInvoiceLinesInterface.csv
    # as separate files, we pair them into one ZIP submission per pair instead
    # of creating two orphan jobs.
    raw_blobs: list[tuple[str, bytes]] = []
    for file in files:
        ext = Path(file.filename).suffix.lower()
        if ext not in allowed_ext:
            errors.append(f"{file.filename}: for {txn}, only {', '.join(sorted(allowed_ext))} allowed")
            continue
        raw_blobs.append((file.filename, await file.read()))

    if txn == "AP" and len(raw_blobs) >= 2 and all(n.lower().endswith(".csv") for n, _ in raw_blobs):
        paired, passthrough, pair_warns = _pair_ap_csv_uploads(raw_blobs)
        if pair_warns: errors.extend(pair_warns)
        # Replace the raw_blobs list with paired ZIPs + any leftover flats
        new_blobs: list[tuple[str, bytes]] = []
        for name, zb, _ in paired:
            new_blobs.append((name, zb))
        new_blobs.extend(passthrough)
        if paired:
            logger.info("AP multi-CSV paired into %d ZIP(s) (passthrough=%d)",
                        len(paired), len(passthrough))
        raw_blobs = new_blobs

    for filename, content in raw_blobs:
        ext = Path(filename).suffix.lower() or ".zip"
        req_id  = str(uuid.uuid4())

        # AP-specific: a ZIP must contain at least one .csv inside
        if txn == "AP" and ext == ".zip":
            ok, msg = _check_ap_zip_has_csv(content)
            if not ok:
                errors.append(f"{filename}: {msg}")
                continue

        # All data lives in MongoDB — nothing written to local disk.
        if not _store_and_verify_upload(req_id, filename, content, "manual_upload"):
            errors.append(f"{filename}: storage failed — see server log")
            continue

        with SessionLocal() as db:
            req_kwargs = dict(
                id=req_id, file_name=filename,
                file_path=filename,
                file_type=ext.lstrip("."),
                file_size_bytes=len(content), status="RECEIVED",
                current_stage="QUEUED",
                transaction_type=txn,
                accounting_date=accounting_date,
                sender_email="manual_upload",
            )
            if txn == "AP":
                req_kwargs.update(
                    ap_business_unit_id   = ap_business_unit_id   or cfg.ap_business_unit_id,
                    ap_business_unit_name = ap_business_unit_name or cfg.ap_business_unit_name,
                    ap_ledger_id          = ap_ledger_id          or cfg.ap_ledger_id,
                    ap_source             = ap_source             or cfg.ap_source,
                    ap_pay_group          = ap_pay_group          or cfg.ap_pay_group,
                    ap_invoice_group      = ap_invoice_group      or cfg.ap_invoice_group,
                    legal_entity          = legal_entity          or "",
                    journal_name          = journal_name or Path(filename).stem,
                )
            else:
                req_kwargs.update(
                    ledger_name  = ledger_name or "",
                    journal_name = journal_name or Path(filename).stem,
                    currency_code= currency,
                    period_name  = period_name,
                )
            req = JournalRequest(**req_kwargs)
            db.add(req); db.commit()

        _dispatch_processing(req_id, txn)
        req_ids.append(req_id)
        logger.info("Queued upload: %s → %s (%d bytes, MongoDB-only, %s)",
                    filename, req_id, len(content), txn)

    if errors:
        err_msg = "; ".join(errors)
        if not req_ids:
            return RedirectResponse(f"/upload?err={err_msg}", status_code=303)

    if len(req_ids) == 1:
        return RedirectResponse(f"/request/{req_ids[0]}", status_code=303)
    return RedirectResponse(f"/?msg=Queued+{len(req_ids)}+file(s)+for+processing", status_code=303)


# ── Approval endpoint ─────────────────────────────────────────────────────────
_APPROVED_PAGE = """<html><body style="font-family:'Segoe UI',sans-serif;display:flex;
align-items:center;justify-content:center;height:100vh;background:{bg};">
<div style="text-align:center;color:white;padding:40px">
<div style="font-size:72px">{icon}</div>
<h1 style="font-size:32px;margin:16px 0 8px">{title}</h1>
<p style="opacity:.85;font-size:16px">{msg}</p>
<p style="opacity:.6;font-size:13px;margin-top:24px">You may close this window.</p>
</div></body></html>"""


@app.get("/approve/{token}", response_class=HTMLResponse)
async def handle_approval(token: str, action: str = "continue"):
    with SessionLocal() as db:
        req = db.query(JournalRequest).filter_by(approval_token=token).first()
        if not req:
            return HTMLResponse(_APPROVED_PAGE.format(bg="#555",icon="⏰",title="Link Expired",msg="This link has expired or is invalid."), status_code=410)
        if req.approval_status in ("APPROVED","REJECTED"):
            return HTMLResponse(_APPROVED_PAGE.format(bg="#555",icon="ℹ️",title="Already Responded",msg=f"Import already {req.approval_status.lower()}."))
        if action.lower() in ("continue","approve","yes"):
            req.approval_status = "APPROVED"
            db.commit()
            return HTMLResponse(_APPROVED_PAGE.format(bg="#1e8e3e",icon="✅",title="Import Approved",msg=f"The GL import will now proceed. You will receive a completion email."))
        else:
            req.approval_status = "REJECTED"
            db.commit()
            return HTMLResponse(_APPROVED_PAGE.format(bg="#d93025",icon="❌",title="Import Rejected",msg="The GL import has been cancelled."))


# ── Settings ──────────────────────────────────────────────────────────────────
@app.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request):
    cfg = get_settings()
    msg = request.query_params.get("msg","")
    test_result = request.query_params.get("test","")
    return templates.TemplateResponse("settings.html", {
        "request": request, "cfg": cfg, "msg": msg, "test_result": test_result,
    })


@app.post("/settings/save")
async def save_settings(
    fusion_url:            str = Form(...),
    fusion_username:       str = Form(...),
    fusion_password:       str = Form(...),
    fusion_document_account: str = Form(...),
    fusion_job_name:       str = Form(...),
    gmail_subject_filter:  str = Form(...),
    notification_email:    str = Form(...),
    gmail_poll_seconds:    int = Form(60),
    mapping_threshold:     float = Form(0.70),
    ess_max_minutes:       int = Form(30),
    app_base_url:          str = Form(""),
    gmail_user:            str = Form(""),
    gmail_app_password:    str = Form(""),
    # ── AP-specific (optional) ───────────────────────────────────────────────
    ap_document_account:   str = Form(""),
    ap_job_name:           str = Form(""),
    ap_business_unit_id:   str = Form(""),
    ap_business_unit_name: str = Form(""),
    ap_ledger_id:          str = Form(""),
    ap_source:             str = Form(""),
    ap_pay_group:          str = Form(""),
    ap_invoice_group:      str = Form(""),
    gmail_subject_filter_ap: str = Form(""),
    # importBulkData fixed params + BIP report
    gl_callback_url:       str = Form(""),
    gl_notification_code:  str = Form(""),
    gl_job_options:        str = Form(""),
    ap_callback_url:       str = Form(""),
    ap_notification_code:  str = Form(""),
    ap_job_options:        str = Form(""),
    ap_bip_report_path:    str = Form(""),
    ap_bip_report_param:   str = Form(""),
):
    with SessionLocal() as db:
        cfg = db.get(AppSettings, 1)
        cfg.fusion_url             = fusion_url
        cfg.fusion_username        = fusion_username
        cfg.fusion_password        = fusion_password
        cfg.fusion_document_account= fusion_document_account
        cfg.fusion_job_name        = fusion_job_name
        cfg.gmail_subject_filter   = gmail_subject_filter
        cfg.notification_email     = notification_email
        cfg.gmail_poll_seconds     = gmail_poll_seconds
        cfg.mapping_threshold      = mapping_threshold
        cfg.ess_max_minutes        = ess_max_minutes
        cfg.app_base_url           = app_base_url
        cfg.gmail_user             = gmail_user
        if gmail_app_password:
            cfg.gmail_app_password = gmail_app_password
        # AP — only overwrite if a value was actually sent (keeps previous defaults)
        if ap_document_account:   cfg.ap_document_account   = ap_document_account
        if ap_job_name:           cfg.ap_job_name           = ap_job_name
        if ap_business_unit_id:   cfg.ap_business_unit_id   = ap_business_unit_id
        if ap_business_unit_name: cfg.ap_business_unit_name = ap_business_unit_name
        if ap_ledger_id:          cfg.ap_ledger_id          = ap_ledger_id
        if ap_source:             cfg.ap_source             = ap_source
        if ap_pay_group:          cfg.ap_pay_group          = ap_pay_group
        if ap_invoice_group:      cfg.ap_invoice_group      = ap_invoice_group
        if gmail_subject_filter_ap: cfg.gmail_subject_filter_ap = gmail_subject_filter_ap
        # importBulkData fixed params — overwrite even if blank so user can clear them
        if gl_callback_url        is not None: cfg.gl_callback_url       = gl_callback_url       or "#NULL"
        if gl_notification_code   is not None: cfg.gl_notification_code  = gl_notification_code  or "10"
        if gl_job_options         is not None and gl_job_options.strip(): cfg.gl_job_options    = gl_job_options
        if ap_callback_url        is not None: cfg.ap_callback_url       = ap_callback_url       or "#NULL"
        if ap_notification_code   is not None: cfg.ap_notification_code  = ap_notification_code  or "10"
        if ap_job_options         is not None and ap_job_options.strip(): cfg.ap_job_options    = ap_job_options
        if ap_bip_report_path     is not None and ap_bip_report_path.strip():  cfg.ap_bip_report_path = ap_bip_report_path
        if ap_bip_report_param    is not None and ap_bip_report_param.strip(): cfg.ap_bip_report_param = ap_bip_report_param
        cfg.updated_at             = datetime.now(timezone.utc)
        db.commit()
    return RedirectResponse("/settings?msg=Settings+saved+successfully", status_code=303)


@app.post("/settings/test-fusion")
async def test_fusion():
    cfg = get_settings()
    result = test_connection(cfg)
    return result


@app.post("/settings/test-gmail")
async def test_gmail_app_password():
    """Try logging in to Gmail IMAP using the configured App Password."""
    from services.gmail_imap import test_app_password
    ok, msg = test_app_password()
    return {"ok": ok, "message": msg}


@app.post("/settings/network-diagnose")
async def network_diagnose():
    """
    Step-by-step check of outbound connectivity needed for Gmail App Password.
    Helps tell whether HF Spaces is blocking port 993/587 vs other failures.
    """
    import socket, ssl as _ssl, time
    results = []

    def step(label, fn):
        t0 = time.monotonic()
        try:
            fn()
            results.append({"step": label, "ok": True,
                            "ms": int((time.monotonic() - t0) * 1000)})
            return True
        except Exception as e:
            results.append({"step": label, "ok": False,
                            "ms": int((time.monotonic() - t0) * 1000),
                            "error": f"{type(e).__name__}: {str(e)[:160]}"})
            return False

    # 1. DNS lookup
    addr = {"imap": None, "smtp": None}
    def _dns_imap(): addr["imap"] = socket.gethostbyname("imap.gmail.com")
    def _dns_smtp(): addr["smtp"] = socket.gethostbyname("smtp.gmail.com")
    step("DNS resolve imap.gmail.com", _dns_imap)
    step("DNS resolve smtp.gmail.com", _dns_smtp)

    # 2. Plain TCP connect (short timeout — if blocked, fail fast)
    def _tcp(host, port):
        s = socket.socket(); s.settimeout(8)
        try:
            s.connect((host, port))
        finally:
            s.close()
    step("TCP connect imap.gmail.com:993", lambda: _tcp("imap.gmail.com", 993))
    step("TCP connect smtp.gmail.com:587", lambda: _tcp("smtp.gmail.com", 587))

    # 3. TLS handshake (catches firewalls that allow TCP but block SSL)
    def _tls(host, port):
        ctx = _ssl.create_default_context()
        s = socket.socket(); s.settimeout(8)
        try:
            s.connect((host, port))
            with ctx.wrap_socket(s, server_hostname=host) as ss:
                ss.recv(1)   # IMAP/SMTP both send a banner immediately
        finally:
            try: s.close()
            except: pass
    step("TLS handshake imap.gmail.com:993", lambda: _tls("imap.gmail.com", 993))

    # 4. Outbound HTTPS sanity check (proves egress works at all)
    def _https():
        import httpx
        r = httpx.get("https://www.google.com/generate_204", timeout=8)
        if r.status_code not in (204, 200): raise RuntimeError(f"got {r.status_code}")
    step("HTTPS google.com:443", _https)

    overall_ok = all(r["ok"] for r in results[:5])
    return {"ok": overall_ok, "results": results, "resolved_ips": addr}


@app.post("/settings/save-gmail-password")
async def save_gmail_password(
    gmail_user:         str = Form(...),
    gmail_app_password: str = Form(...),
):
    """Save only the App Password fields, leaving other settings untouched."""
    with SessionLocal() as db:
        cfg = db.get(AppSettings, 1)
        cfg.gmail_user = gmail_user.strip()
        # Strip spaces — Google copies the App Password with spaces in it
        if gmail_app_password.strip():
            cfg.gmail_app_password = gmail_app_password.replace(" ", "").strip()
        db.commit()
    return RedirectResponse("/gmail-setup?msg=Gmail+App+Password+saved+%E2%80%94+click+Test+Connection+to+verify",
                             status_code=303)


# ── Gmail setup ───────────────────────────────────────────────────────────────
def _is_headless() -> bool:
    """True when running in a container with no display (HF Spaces, Docker, etc.)."""
    import os
    # No DISPLAY env var on Linux, or explicit HF_SPACE_ID set by Hugging Face
    return (os.environ.get("SPACE_ID") is not None
            or os.environ.get("HF_SPACE") is not None
            or (os.name == "posix" and not os.environ.get("DISPLAY")))


@app.get("/gmail-setup", response_class=HTMLResponse)
async def gmail_setup_page(request: Request):
    cfg = get_settings()
    creds_p = _safe_creds_path(cfg)
    token_p = _safe_token_path(cfg)
    # Also consider MongoDB-only storage as "exists" — files get restored from
    # MongoDB on demand, so the badge should turn green if EITHER is present.
    from database import get_secure_file as _gsf
    creds_exists = creds_p.is_file() or _gsf("gmail_credentials") is not None
    token_exists = token_p.is_file() or _gsf("gmail_token") is not None
    return templates.TemplateResponse("gmail_setup.html", {
        "request": request, "creds_exists": creds_exists, "token_exists": token_exists,
        "creds_path": str(creds_p),
        "headless": _is_headless(),
        "oauth_redirect_uri": _gmail_oauth_redirect_uri(),
    })


@app.post("/gmail-setup/upload-credentials")
async def upload_gmail_creds(file: UploadFile = File(...)):
    cfg = get_settings()
    dest = _safe_creds_path(cfg)
    dest.parent.mkdir(parents=True, exist_ok=True)
    content = await file.read()
    dest.write_bytes(content)
    store_secure_file("gmail_credentials", content)
    return RedirectResponse("/gmail-setup?msg=Credentials+uploaded", status_code=303)


@app.post("/gmail-setup/upload-token")
async def upload_gmail_token(file: UploadFile = File(...)):
    """
    Upload a pre-generated OAuth token (gmail_token.json) — the path used when
    running on a headless server (Hugging Face Space, Docker container) that
    can't open a browser for the local-server OAuth flow.

    To produce the token: run the app locally, complete the Authorize Gmail
    flow once (browser opens, you grant access), copy `config/gmail_token.json`,
    upload it here.
    """
    cfg = get_settings()
    dest = _safe_token_path(cfg)
    dest.parent.mkdir(parents=True, exist_ok=True)
    content = await file.read()
    # Validate it parses as JSON before storing — bad tokens silently break polling
    try:
        import json as _json
        _json.loads(content.decode("utf-8"))
    except Exception as e:
        return RedirectResponse(f"/gmail-setup?err=Invalid+token+JSON%3A+{str(e)[:60]}",
                                 status_code=303)
    dest.write_bytes(content)
    store_secure_file("gmail_token", content)
    return RedirectResponse("/gmail-setup?msg=OAuth+token+uploaded+%E2%80%94+Gmail+polling+active",
                             status_code=303)


def _gmail_oauth_redirect_uri() -> str:
    """The Google-side redirect URL — must be registered in your OAuth client."""
    from workflow import _public_base_url
    return f"{_public_base_url()}/gmail-setup/oauth-callback"


def _safe_creds_path(cfg) -> Path:
    """Resolve gmail_credentials_file safely; fall back to default if blank."""
    raw = (cfg.gmail_credentials_file or "config/gmail_credentials.json").strip()
    if not raw or Path(raw).is_dir():
        raw = "config/gmail_credentials.json"
    return Path(raw)


def _safe_token_path(cfg) -> Path:
    """Resolve gmail_token_file safely; fall back to default if blank."""
    raw = (cfg.gmail_token_file or "config/gmail_token.json").strip()
    if not raw or Path(raw).is_dir():
        raw = "config/gmail_token.json"
    return Path(raw)


def _ensure_credentials_on_disk(creds_path: Path) -> bool:
    """If credentials.json was wiped by an HF rebuild, restore it from MongoDB."""
    if creds_path.is_file():
        return True
    from database import get_secure_file as _gsf
    stored = _gsf("gmail_credentials")
    if not stored:
        return False
    creds_path.parent.mkdir(parents=True, exist_ok=True)
    creds_path.write_bytes(stored)
    logger.info("Restored gmail_credentials.json from MongoDB for OAuth flow")
    return True


@app.get("/gmail-setup/oauth-start")
async def gmail_oauth_start():
    """
    Start a server-side OAuth flow. The user's browser is redirected to Google's
    consent page; Google then redirects back to /gmail-setup/oauth-callback with
    a `code` query param. Works on any headless host (HF Spaces, Docker) because
    the consent UI runs in the USER's browser, not on the server.
    """
    from google_auth_oauthlib.flow import Flow
    cfg = get_settings()
    creds_path = _safe_creds_path(cfg)
    if not _ensure_credentials_on_disk(creds_path):
        return RedirectResponse("/gmail-setup?err=Upload+credentials.json+first",
                                 status_code=303)
    try:
        flow = Flow.from_client_secrets_file(
            str(creds_path),
            scopes=["https://www.googleapis.com/auth/gmail.modify"],
            redirect_uri=_gmail_oauth_redirect_uri(),
        )
        auth_url, _state = flow.authorization_url(
            access_type="offline",
            prompt="consent",
            include_granted_scopes="true",
        )
    except Exception as e:
        return RedirectResponse(f"/gmail-setup?err=Could+not+build+auth+URL%3A+{str(e)[:80]}",
                                 status_code=303)
    return RedirectResponse(auth_url, status_code=303)


@app.get("/gmail-setup/oauth-callback")
async def gmail_oauth_callback(code: str = "", error: str = ""):
    """Google sends the user's browser back here with ?code=... — we exchange it."""
    if error:
        return RedirectResponse(f"/gmail-setup?err=Google+returned+{error[:80]}",
                                 status_code=303)
    if not code:
        return RedirectResponse("/gmail-setup?err=No+code+returned+from+Google",
                                 status_code=303)
    from google_auth_oauthlib.flow import Flow
    cfg = get_settings()
    creds_path = _safe_creds_path(cfg)
    token_path = _safe_token_path(cfg)
    if not _ensure_credentials_on_disk(creds_path):
        return RedirectResponse("/gmail-setup?err=Upload+credentials.json+first",
                                 status_code=303)
    try:
        flow = Flow.from_client_secrets_file(
            str(creds_path),
            scopes=["https://www.googleapis.com/auth/gmail.modify"],
            redirect_uri=_gmail_oauth_redirect_uri(),
        )
        flow.fetch_token(code=code)
        token_json = flow.credentials.to_json()
        # Write to disk + MongoDB
        token_path.parent.mkdir(parents=True, exist_ok=True)
        token_path.write_text(token_json)
        store_secure_file("gmail_token", token_json.encode())
        logger.info("Gmail OAuth callback completed — token stored at %s", token_path)
    except Exception as e:
        return RedirectResponse(f"/gmail-setup?err=Token+exchange+failed%3A+{str(e)[:120]}",
                                 status_code=303)
    return RedirectResponse(
        "/gmail-setup?msg=Gmail+authorized+successfully+%E2%80%94+polling+active",
        status_code=303)


@app.post("/gmail-setup/authorize")
async def authorize_gmail():
    """Trigger OAuth2 flow — opens browser on server machine."""
    try:
        from services.gmail_service import get_gmail_service
        get_gmail_service()  # triggers browser flow and saves token
        return RedirectResponse("/gmail-setup?msg=Gmail+authorized+successfully", status_code=303)
    except Exception as e:
        return RedirectResponse(f"/gmail-setup?err={str(e)[:100]}", status_code=303)


# ── File download ─────────────────────────────────────────────────────────────
_FTYPE_TO_KIND = {
    "zip":           "fbdi_zip",
    "csv":           "fbdi_csv",
    "bad":           "bad_csv",
    "log":           "ess_log",
    # AP-specific
    "ap_hdr_csv":    "ap_hdr_csv",
    "ap_line_csv":   "ap_line_csv",
    "ap_report_pdf": "ap_report_pdf",
    "ap_report_xml": "ap_report_xml",
    "ap_report_html":"ap_report_html",
    "ap_report_txt": "ap_report_txt",
}
_FTYPE_MIME = {
    "zip":           "application/zip",
    "csv":           "text/csv",
    "bad":           "text/csv",
    "log":           "application/zip",
    "ap_hdr_csv":    "text/csv",
    "ap_line_csv":   "text/csv",
    "ap_report_pdf": "application/pdf",
    "ap_report_xml": "application/xml",
    "ap_report_html":"text/html",
    "ap_report_txt": "text/plain",
}


def _resolve_versioned_kind(req_id: str, kind: str):
    """Return get_generated_file() result, falling back to the highest-versioned
    {kind}_v* if the exact kind isn't present. Reprocess runs store artifacts
    under e.g. ess_log_v1 / fbdi_zip_v2, so the legacy 'ess_log' key would
    otherwise 404 even when the user clearly has logs for v1."""
    result = get_generated_file(req_id, kind)
    if result:
        return result
    try:
        doc = _mdb()["generated_files"].find_one({"_id": req_id})
        if doc:
            candidates: list[tuple[int, str]] = []
            for k in (doc.get("files", {}) or {}).keys():
                if k.startswith(kind + "_v"):
                    try:
                        n = int(k.rsplit("_v", 1)[1])
                        candidates.append((n, k))
                    except ValueError:
                        pass
            if candidates:
                candidates.sort(reverse=True)
                return get_generated_file(req_id, candidates[0][1])
    except Exception:
        pass
    return None


@app.get("/download/{req_id}/{ftype}")
async def download_file(req_id: str, ftype: str, inline: int = 0):
    from fastapi.responses import Response
    kind = _FTYPE_TO_KIND.get(ftype)
    if not kind:
        return JSONResponse({"error": f"unknown ftype: {ftype}"}, 400)
    result = _resolve_versioned_kind(req_id, kind)
    if not result:
        return JSONResponse({"error": "file not found in DB"}, 404)
    content, filename = result
    disp = "inline" if inline else "attachment"
    return Response(
        content=content,
        media_type=_FTYPE_MIME.get(ftype, "application/octet-stream"),
        headers={"Content-Disposition": f'{disp}; filename="{filename}"'},
    )


# ── AP Invoice Import Report — view PDF inline in browser ────────────────────

@app.get("/request/{req_id}/ap-report")
async def view_ap_report(req_id: str, fmt: str = ""):
    """
    View the AP Import Report inline. Prefers our rendered HTML (always present
    after a successful run because we build it from the BIP XML data), with PDF
    and XML available as fall-backs and as separate downloads.
    """
    from fastapi.responses import Response
    # Priority: HTML (most readable) → PDF → XML → friendly 404
    order = ["ap_report_html", "ap_report_pdf", "ap_report_xml", "ap_report_txt"]
    if fmt == "pdf":  order = ["ap_report_pdf"] + order
    if fmt == "xml":  order = ["ap_report_xml"] + order
    if fmt == "html": order = ["ap_report_html"] + order

    media_map = {
        "ap_report_html": "text/html",
        "ap_report_pdf":  "application/pdf",
        "ap_report_xml":  "application/xml",
        "ap_report_txt":  "text/plain",
    }
    for kind in order:
        result = _resolve_versioned_kind(req_id, kind)
        if result:
            content, filename = result
            return Response(
                content=content,
                media_type=media_map.get(kind, "application/octet-stream"),
                headers={"Content-Disposition": f'inline; filename="{filename}"'},
            )
    return HTMLResponse(
        "<html><body style='font-family:sans-serif;padding:40px'>"
        "<h2>AP Import Report not available</h2>"
        "<p>Possible reasons:</p>"
        "<ul><li>The import job hasn't finished yet — refresh in a moment</li>"
        "<li>Oracle's Report sub-job ran but produced no data (e.g. PurgeOption removed the records)</li>"
        "<li>The request was a GL journal, not AP</li></ul>"
        f"<p><a href='/request/{req_id}'>← Back to request</a></p>"
        "</body></html>", status_code=404)


# ── Files API + per-file download ─────────────────────────────────────────────

def _all_files_for_request(req_id: str) -> list[dict]:
    """List every file (original upload + all generated kinds) for a request."""
    out: list[dict] = []
    have_original = False
    try:
        up_doc = _mdb()["uploaded_files"].find_one({"_id": req_id})
        if up_doc:
            out.append({
                "kind":        "original",
                "filename":    up_doc.get("filename", "uploaded"),
                "size_bytes":  up_doc.get("size_bytes", 0),
                "uploaded_at": str(up_doc.get("stored_at", "")),
            })
            have_original = True
    except Exception:
        pass

    # Fallback: when uploaded_files doc is missing (e.g. silent encrypt
    # failure during Gmail polling) but the JournalRequest still records
    # a file_name, surface it so the Downloads card and download endpoint
    # can serve it. This keeps Gmail-sourced failed requests recoverable.
    if not have_original:
        try:
            with SessionLocal() as db:
                req = db.get(JournalRequest, req_id)
                if req and (req.file_name or req.file_path):
                    out.append({
                        "kind":        "original",
                        "filename":    req.file_name or req.file_path or "uploaded",
                        "size_bytes":  int(getattr(req, "file_size_bytes", 0) or 0),
                        "uploaded_at": str(getattr(req, "created_at", "") or ""),
                    })
        except Exception:
            pass

    try:
        gen_doc = _mdb()["generated_files"].find_one({"_id": req_id})
        if gen_doc:
            for kind, meta in (gen_doc.get("files", {}) or {}).items():
                out.append({
                    "kind":        kind,
                    "filename":    meta.get("filename", kind),
                    "size_bytes":  meta.get("size_bytes", 0),
                    "uploaded_at": str(meta.get("stored_at", "")),
                })
    except Exception:
        pass
    return out


@app.get("/request/{req_id}/files")
async def list_request_files(req_id: str):
    return JSONResponse(_all_files_for_request(req_id))


@app.get("/request/{req_id}/download/{filename}")
async def download_request_file(req_id: str, filename: str):
    """Download any stored file for this request by its stored filename."""
    from fastapi.responses import Response
    # 1. Try original upload
    try:
        up_doc = _mdb()["uploaded_files"].find_one({"_id": req_id})
        if up_doc and up_doc.get("filename") == filename:
            content = get_uploaded_file(req_id)
            if content is not None:
                return Response(
                    content=content,
                    media_type="application/octet-stream",
                    headers={"Content-Disposition": f'attachment; filename="{filename}"'},
                )
    except Exception:
        pass
    # 2. Search through generated files by filename
    try:
        gen_doc = _mdb()["generated_files"].find_one({"_id": req_id})
        if gen_doc:
            for kind, meta in (gen_doc.get("files", {}) or {}).items():
                if meta.get("filename") == filename:
                    result = get_generated_file(req_id, kind)
                    if result:
                        content, fname = result
                        # MIME guessing
                        if fname.lower().endswith(".zip"):
                            mt = "application/zip"
                        elif fname.lower().endswith(".csv"):
                            mt = "text/csv"
                        else:
                            mt = "application/octet-stream"
                        return Response(
                            content=content,
                            media_type=mt,
                            headers={"Content-Disposition": f'attachment; filename="{fname}"'},
                        )
    except Exception:
        pass
    return JSONResponse({"error": "file not found"}, 404)


@app.get("/request/{req_id}/dl/kind/{kind:path}")
async def download_by_kind(req_id: str, kind: str):
    """Download a stored artifact by its exact MongoDB kind key (e.g. ap_hdr_csv_v0)."""
    from fastapi.responses import Response
    result = get_generated_file(req_id, kind)
    if not result:
        return JSONResponse({"error": f"kind '{kind}' not found"}, 404)
    content, filename = result
    if filename.lower().endswith(".zip"):
        mt = "application/zip"
    elif filename.lower().endswith(".csv"):
        mt = "text/csv"
    elif filename.lower().endswith(".pdf"):
        mt = "application/pdf"
    elif filename.lower().endswith(".xml"):
        mt = "application/xml"
    else:
        mt = "application/octet-stream"
    return Response(
        content=content,
        media_type=mt,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/request/{req_id}/dl/log/{key:path}")
async def download_log_file(req_id: str, key: str):
    """Download a specific stored ESS log file by its MongoDB key."""
    from fastapi.responses import Response
    from database import get_log_file
    result = get_log_file(req_id, key)
    if not result:
        return JSONResponse({"error": f"log key '{key}' not found"}, 404)
    content, filename = result
    if filename.lower().endswith(".zip"):
        mt = "application/zip"
    elif filename.lower().endswith(".xml"):
        mt = "application/xml"
    elif filename.lower().endswith(".pdf"):
        mt = "application/pdf"
    else:
        mt = "text/plain"
    return Response(
        content=content,
        media_type=mt,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ── Edit & Reprocess ──────────────────────────────────────────────────────────

def _parse_request_source_to_table(req_id: str):
    """Load current source bytes for a request and parse into (headers, rows[][]).

    Uses latest_edit_filename if set, otherwise the original upload. ZIPs are
    extracted to find the CSV inside.
    """
    import tempfile as _tf, os as _os, zipfile as _zip, io as _io
    from utils.fbdi_generator import DATA_COLS
    from utils.file_parser import parse_to_records

    with SessionLocal() as db:
        req = db.get(JournalRequest, req_id)
        if not req:
            return None, None, None
        version = int(getattr(req, "version", 0) or 0)

    # Always prefer the HIGHEST-versioned edit if one exists. This is more
    # robust than trusting `latest_edit_filename` alone — a missed commit on
    # that field used to send users back to the original upload. Falls back
    # to original only if no edit version is present.
    source_bytes: bytes | None = None
    source_name = "uploaded.csv"
    try:
        gen_doc = _mdb()["generated_files"].find_one({"_id": req_id})
        edits: list[tuple[int, str]] = []
        if gen_doc:
            for kind in (gen_doc.get("files", {}) or {}).keys():
                if kind.startswith("edited_csv_v"):
                    try:
                        n = int(kind.rsplit("_v", 1)[1])
                        edits.append((n, kind))
                    except ValueError:
                        pass
        if edits:
            edits.sort(reverse=True)
            highest_n, highest_kind = edits[0]
            result = get_generated_file(req_id, highest_kind)
            if result is not None:
                source_bytes, source_name = result
                version = highest_n
    except Exception:
        pass

    if source_bytes is None:
        source_bytes = get_uploaded_file(req_id)
        with SessionLocal() as db:
            r = db.get(JournalRequest, req_id)
            source_name = (r.file_name if r else "uploaded.csv") or "uploaded.csv"

    if source_bytes is None:
        return None, None, version

    # If ZIP, extract the first CSV inside
    if source_name.lower().endswith(".zip"):
        try:
            with _zip.ZipFile(_io.BytesIO(source_bytes)) as zf:
                csv_name = next((n for n in zf.namelist()
                                 if n.lower().endswith(".csv") and not n.startswith("__")), None)
                if csv_name:
                    source_bytes = zf.read(csv_name)
                    source_name = csv_name
        except Exception:
            pass

    # Write to temp + parse
    suffix = Path(source_name).suffix or ".csv"
    fd, tmppath = _tf.mkstemp(suffix=suffix)
    _os.close(fd)
    try:
        Path(tmppath).write_bytes(source_bytes)
        records, cols = parse_to_records(tmppath)
    finally:
        try: _os.remove(tmppath)
        except Exception: pass

    headers = list(DATA_COLS)

    # Map source columns → FBDI canonical names. Without this, business CSVs
    # (with headers like "date" / "company" / "cost_center") show empty cells
    # because rec.get("*Effective Date of Transaction") never matches.
    src_to_fbdi: dict[str, str] = {}
    try:
        # If source already uses FBDI canonical headers, identity-map them.
        for c in cols:
            if c in headers:
                src_to_fbdi[c] = c
        # ML-map any remaining columns through the same mapper the workflow uses.
        unmapped = [c for c in cols if c not in src_to_fbdi]
        if unmapped:
            from services.ml_mapper import map_all_columns, load_history_boost
            ml_results = map_all_columns(unmapped, history_boost=load_history_boost())
            CONF = 0.45  # match workflow's threshold; lower than this = no mapping
            for r in ml_results:
                if r.get("target_field") and float(r.get("confidence", 0)) >= CONF:
                    src_to_fbdi[r["source_field"]] = r["target_field"]
    except Exception as _e:
        logger.warning("Edit: ML mapping failed for %s, showing raw cells: %s",
                       req_id, _e)

    # Invert: for each FBDI canonical header, which source column feeds it?
    fbdi_to_src = {fbdi: src for src, fbdi in src_to_fbdi.items()}

    rows: list[list[str]] = []
    for rec in records:
        row = []
        for h in headers:
            # 1) Source column already named canonically (e.g. FBDI re-edits)
            val = rec.get(h, "")
            # 2) Otherwise, look up the source column the ML mapper assigned
            if val in (None, "") and h in fbdi_to_src:
                val = rec.get(fbdi_to_src[h], "")
            row.append("" if val is None else str(val))
        rows.append(row)
    return headers, rows, version


@app.get("/request/{req_id}/edit", response_class=HTMLResponse)
async def edit_request(request: Request, req_id: str):
    with SessionLocal() as db:
        req = db.get(JournalRequest, req_id)
        if not req:
            return HTMLResponse("Not found", status_code=404)
    # AP requests get the two-tab wizard (headers + lines)
    if getattr(req, "transaction_type", "GL") == "AP":
        return RedirectResponse(f"/request/{req_id}/edit-ap", status_code=303)

    headers, rows, version = _parse_request_source_to_table(req_id)
    if headers is None:
        return HTMLResponse("Source file not found", status_code=404)
    return templates.TemplateResponse("edit_csv.html", {
        "request": request, "req": req,
        "headers": headers, "rows": rows, "version": version,
    })


def _csv_to_table(content: bytes) -> tuple[list[str], list[list[str]]]:
    """Parse a CSV byte string into (headers, rows). Returns empty if blank."""
    import csv as _csv, io as _io
    if not content: return [], []
    text = content.decode("utf-8", errors="replace")
    reader = _csv.reader(_io.StringIO(text))
    rows = [r for r in reader if any((c or "").strip() for c in r)]
    if not rows: return [], []
    return rows[0], rows[1:]


@app.get("/request/{req_id}/edit-ap", response_class=HTMLResponse)
async def edit_ap_request(request: Request, req_id: str):
    """
    Two-tab wizard for AP requests:
      Tab 1 — ApInvoicesInterface.csv (invoice headers)
      Tab 2 — ApInvoiceLinesInterface.csv (line items)
    Loads the generated positional FBDI columns from the AP generator so
    the user edits in Oracle's exact column layout.
    """
    from utils.ap_fbdi_generator import AP_HEADER_COLUMNS, AP_LINE_COLUMNS
    with SessionLocal() as db:
        req = db.get(JournalRequest, req_id)
        if not req:
            return HTMLResponse("Not found", status_code=404)

    hdr_kind  = _resolve_versioned_kind(req_id, "ap_hdr_csv")
    line_kind = _resolve_versioned_kind(req_id, "ap_line_csv")
    hdr_content  = hdr_kind[0]  if hdr_kind  else b""
    line_content = line_kind[0] if line_kind else b""

    hdr_cols_in_file,  hdr_rows  = _csv_to_table(hdr_content)
    line_cols_in_file, line_rows = _csv_to_table(line_content)

    # Oracle FBDI CSVs are headerless positional — use the generator's column
    # names as labels (the FBDI file itself has no header row)
    hdr_columns  = [c for c in AP_HEADER_COLUMNS if c != "END"]
    line_columns = [c for c in AP_LINE_COLUMNS  if c != "END"]

    # If our stored file already has data rows (no header), every row should
    # be aligned with hdr_columns positionally.
    # The first stored "row" might be the data; CSV reader treated row 0 as
    # the column-name row but our FBDI is headerless, so we put it back.
    if hdr_cols_in_file: hdr_rows  = [hdr_cols_in_file]  + hdr_rows
    if line_cols_in_file: line_rows = [line_cols_in_file] + line_rows

    # ── Fallback: if no FBDI CSVs exist (all-failed validation before fix),
    # load the original uploaded file so the user has data to edit. ──────────
    raw_fallback = False
    raw_upload_columns: list[str] = []
    raw_upload_rows: list[list[str]] = []
    if not hdr_rows:
        from database import get_uploaded_file as _get_orig
        orig_bytes = _get_orig(req_id)
        if orig_bytes:
            import csv as _csv_mod, io as _io_mod
            text = orig_bytes.decode("utf-8", errors="replace")
            reader = _csv_mod.reader(_io_mod.StringIO(text))
            all_rows = [r for r in reader if any((c or "").strip() for c in r)]
            if all_rows:
                raw_fallback = True
                raw_upload_columns = all_rows[0]
                raw_upload_rows    = all_rows[1:]

    version = int(getattr(req, "version", 0) or 0)
    return templates.TemplateResponse("edit_ap.html", {
        "request": request, "req": req, "version": version,
        "hdr_columns":  hdr_columns,  "hdr_rows":  hdr_rows,
        "line_columns": line_columns, "line_rows": line_rows,
        "raw_fallback": raw_fallback,
        "raw_upload_columns": raw_upload_columns,
        "raw_upload_rows":    raw_upload_rows,
    })


@app.post("/request/{req_id}/save_edit_ap")
async def save_edit_ap(req_id: str, request: Request):
    """Save both header + line CSVs from the AP wizard and reprocess."""
    import csv as _csv, io as _io, traceback as _tb
    try:
        body = await request.json()
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"invalid JSON body: {e}"}, 400)
    hdr_rows  = body.get("hdr_rows")  or []
    line_rows = body.get("line_rows") or []
    if not isinstance(hdr_rows, list) or not isinstance(line_rows, list):
        return JSONResponse({"ok": False, "error": "hdr_rows / line_rows must be arrays"}, 400)

    try:
      with SessionLocal() as db:
        req = db.get(JournalRequest, req_id)
        if not req:
            return JSONResponse({"ok": False, "error": "not found"}, 404)
        current_version = int(getattr(req, "version", 0) or 0)
        new_version = current_version + 1

        # Write both CSVs (headerless, END sentinel per row)
        def _write(rows):
            buf = _io.StringIO(newline="")
            w = _csv.writer(buf, lineterminator="\n", quoting=_csv.QUOTE_MINIMAL)
            for r in rows:
                if r and r[-1] == "END":
                    w.writerow(r)
                else:
                    w.writerow(list(r) + ["END"])
            return buf.getvalue().encode("utf-8")

        hdr_bytes  = _write(hdr_rows)
        line_bytes = _write(line_rows)

        # Pack into ZIP for resubmission
        import zipfile as _zf
        zip_buf = _io.BytesIO()
        with _zf.ZipFile(zip_buf, "w", _zf.ZIP_DEFLATED) as zf:
            zf.writestr("ApInvoicesInterface.csv",     hdr_bytes)
            zf.writestr("ApInvoiceLinesInterface.csv", line_bytes)
        zip_bytes = zip_buf.getvalue()

        # Persist versioned copies + canonical v0 keys
        store_generated_file(req_id, f"ap_hdr_csv_v{new_version}",
                              f"ApInvoicesInterface_v{new_version}.csv", hdr_bytes)
        store_generated_file(req_id, f"ap_line_csv_v{new_version}",
                              f"ApInvoiceLinesInterface_v{new_version}.csv", line_bytes)
        store_generated_file(req_id, f"fbdi_zip_v{new_version}",
                              f"ApInvoicesImport_v{new_version}.zip", zip_bytes)
        # Also overwrite canonical so downloads pick up latest
        store_generated_file(req_id, "ap_hdr_csv_v0",
                              "ApInvoicesInterface.csv", hdr_bytes)
        store_generated_file(req_id, "ap_line_csv_v0",
                              "ApInvoiceLinesInterface.csv", line_bytes)
        store_generated_file(req_id, "fbdi_zip_v0",
                              "ApInvoicesImport.zip", zip_bytes)

        # Also store the new ZIP as the source upload so the next reprocess
        # picks it up — workflow_ap's _process_prebuilt_ap_zip path will fire
        try:
            store_uploaded_file(req_id, f"AP_Edited_v{new_version}.zip", zip_bytes)
            req.file_name = f"AP_Edited_v{new_version}.zip"
            req.file_type = "zip"
            req.file_size_bytes = len(zip_bytes)
        except Exception as _e:
            logger.warning("AP edit: store_uploaded_file failed: %s", _e)

        # Bump version + queue reprocess as AP
        req.version = new_version
        req.status = "RECEIVED"
        req.current_stage = "QUEUED"
        req.error_message = None
        req.stop_reason = None
        req.ap_invoices_rejected = 0
        req.ap_rejections_json = []
        db.commit()

      _dispatch_processing(req_id, "AP")
      return JSONResponse({"ok": True, "version": new_version})
    except Exception as e:
        logger.error("save_edit_ap failed for %s: %s\n%s",
                     req_id, e, _tb.format_exc())
        return JSONResponse({"ok": False,
                              "error": f"{type(e).__name__}: {e}"}, 500)


@app.post("/request/{req_id}/save_edit")
async def save_edit(req_id: str, request: Request):
    """Accept JSON {headers:[...], rows:[[...]]}, write CSV, store, bump version."""
    import csv as _csv, io as _io
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid JSON"}, 400)
    headers = body.get("headers") or []
    rows    = body.get("rows") or []
    if not isinstance(headers, list) or not isinstance(rows, list):
        return JSONResponse({"ok": False, "error": "headers/rows missing"}, 400)

    with SessionLocal() as db:
        req = db.get(JournalRequest, req_id)
        if not req:
            return JSONResponse({"ok": False, "error": "request not found"}, 404)
        current_version = int(getattr(req, "version", 0) or 0)
        new_version = current_version + 1
        filename = f"edited_v{new_version}.csv"

        # Write CSV with UTF-8 (no BOM), LF endings
        buf = _io.StringIO(newline="")
        w = _csv.writer(buf, lineterminator="\n", quoting=_csv.QUOTE_MINIMAL)
        w.writerow(headers)
        for r in rows:
            # Normalize each row to len(headers)
            padded = list(r) + [""] * (len(headers) - len(r))
            w.writerow([str(c) if c is not None else "" for c in padded[:len(headers)]])
        csv_bytes = buf.getvalue().encode("utf-8")

        # Store under a versioned kind AND under an alias for the workflow loader
        store_generated_file(req_id, f"edited_csv_v{new_version}", filename, csv_bytes)
        store_generated_file(req_id, f"edit_source_{filename}", filename, csv_bytes)
        req.version = new_version
        req.latest_edit_filename = filename
        db.commit()
    append_log(req_id, "INFO",
               f"User saved edit version {new_version} ({len(rows)} rows)")
    return JSONResponse({"ok": True, "version": new_version, "filename": filename})


@app.post("/request/{req_id}/reprocess")
async def reprocess_request(req_id: str):
    with SessionLocal() as db:
        req = db.get(JournalRequest, req_id)
        if not req:
            return JSONResponse({"error": "not found"}, 404)
        parent = getattr(req, "parent_request_id", "") or req_id
        version = int(getattr(req, "version", 0) or 0)
        txn_type = (getattr(req, "transaction_type", "GL") or "GL").upper()
        req.parent_request_id = parent
        req.status = "RECEIVED"
        req.current_stage = "QUEUED"
        req.error_message = None
        req.stop_reason = None
        db.commit()
    append_log(req_id, "INFO",
               f"=== REPROCESS triggered (edit v{version}, txn={txn_type}) ===")
    _dispatch_processing(req_id, txn_type)
    return RedirectResponse(f"/request/{req_id}", status_code=303)


# ── API: manual trigger ───────────────────────────────────────────────────────
@app.post("/api/poll-now")
async def poll_now():
    threading.Thread(target=_poll_gmail_once, daemon=True).start()
    return {"message": "Gmail check triggered"}


@app.post("/api/retry/{req_id}")
async def retry_request(req_id: str):
    with SessionLocal() as db:
        req = db.get(JournalRequest, req_id)
        if not req: return JSONResponse({"error":"not found"}, 404)
        if req.status != "FAILED":
            return JSONResponse({"error":f"Cannot retry status={req.status}"}, 400)
        txn_type = (getattr(req, "transaction_type", "GL") or "GL").upper()
        req.status = "RECEIVED"
        req.error_message = None
        req.stop_reason = None
        req.current_stage = "QUEUED"
        db.commit()
    _dispatch_processing(req_id, txn_type)
    return {"message": "Retry queued", "request_id": req_id}


# ── Health ────────────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "ok", "db": "mongodb", "ml": "sentence-transformers"}


if __name__ == "__main__":
    import os as _os
    _port = int(_os.environ.get("PORT", "8000"))
    uvicorn.run("app:app", host="0.0.0.0", port=_port, reload=False, log_level="info")
