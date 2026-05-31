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
    store_uploaded_file, get_generated_file, has_generated_file,
)
from services.fusion_service import test_connection
from services.gmail_service import gmail_available
from workflow import process_request

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
    from services.gmail_service import (
        download_attachments, fetch_unprocessed_emails,
        get_gmail_service, mark_failed, mark_processed,
    )
    service = get_gmail_service()
    messages = fetch_unprocessed_emails(service)
    if not messages:
        return
    logger.info("Gmail: found %d unprocessed journal email(s)", len(messages))
    cfg = get_settings()
    for msg in messages:
        try:
            files = download_attachments(service, msg["id"])
            if not files:
                logger.info("No .csv/.txt/.zip attachments in %s — marking failed", msg["id"])
                mark_failed(service, msg["id"])
                continue
            for att in files:
                r_id = str(uuid.uuid4())
                # Store the attachment bytes in MongoDB — no disk write
                store_uploaded_file(r_id, att["file_name"], att["file_bytes"])
                _create_and_process(r_id, msg["id"], att["sender"], att["subject"],
                                    att["file_name"], att["file_type"],
                                    att["file_size_bytes"], cfg)
            mark_processed(service, msg["id"])
        except Exception as e:
            logger.error("Gmail processing failed for %s: %s", msg["id"], e)
            try: mark_failed(service, msg["id"])
            except Exception: pass


def _create_and_process(req_id, email_id, sender, subject, file_name,
                        file_type, file_size, cfg):
    with SessionLocal() as db:
        req = JournalRequest(
            id=req_id, email_id=email_id, sender_email=sender,
            email_subject=subject, file_name=file_name,
            file_path=file_name,  # virtual: file lives in MongoDB
            file_type=file_type, file_size_bytes=file_size,
            status="RECEIVED", current_stage="QUEUED",
            ledger_name=cfg.fusion_ledger_name,
        )
        db.add(req)
        db.commit()
    threading.Thread(target=process_request, args=(req_id,), daemon=True).start()
    logger.info("Queued Gmail attachment for %s: %s (%d bytes, MongoDB)",
                req_id, file_name, file_size)


# ── Dashboard ─────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    with SessionLocal() as db:
        reqs = db.query(JournalRequest).order_by(("created_at", -1)).limit(50).all()
        total    = db.query(JournalRequest).count()
        success  = db.query(JournalRequest).filter_by(status="SUCCEEDED").count()
        failed   = db.query(JournalRequest).filter_by(status="FAILED").count()
        running  = db.query(JournalRequest).filter(
            {"status": {"$in": ["RECEIVED", "PROCESSING"]}}).count()
    return templates.TemplateResponse("dashboard.html", {
        "request": request, "requests": reqs,
        "total": total, "success": success, "failed": failed, "running": running,
        "gmail_ok": gmail_available(),
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
    return templates.TemplateResponse("request_detail.html", {
        "request": request, "req": req,
        "process_logs": logs, "log_files": log_files,
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
    return templates.TemplateResponse("upload.html", {"request": request})


_ALLOWED_UPLOAD_EXT = {".zip", ".csv", ".txt"}


@app.post("/upload")
async def upload_file(
    request: Request,
    files: List[UploadFile] = File(...),
    ledger_name: str = Form(""),
    accounting_date: str = Form(""),
    journal_name: str = Form(""),
    currency: str = Form("USD"),
    period_name: str = Form(""),
):
    cfg = get_settings()
    req_ids = []
    errors = []

    for file in files:
        ext = Path(file.filename).suffix.lower()
        if ext not in _ALLOWED_UPLOAD_EXT:
            errors.append(f"{file.filename}: only .zip, .csv, .txt allowed")
            continue

        req_id  = str(uuid.uuid4())
        content = await file.read()

        # All data lives in MongoDB — nothing written to local disk.
        store_uploaded_file(req_id, file.filename, content)

        with SessionLocal() as db:
            req = JournalRequest(
                id=req_id, file_name=file.filename,
                file_path=file.filename,  # virtual name only — file lives in MongoDB
                file_type=ext.lstrip("."),
                file_size_bytes=len(content), status="RECEIVED",
                current_stage="QUEUED",
                ledger_name=ledger_name or cfg.fusion_ledger_name,
                accounting_date=accounting_date,
                journal_name=journal_name or Path(file.filename).stem,
                currency_code=currency,
                period_name=period_name,
                sender_email="manual_upload",
            )
            db.add(req)
            db.commit()

        threading.Thread(target=process_request, args=(req_id,), daemon=True).start()
        req_ids.append(req_id)
        logger.info("Queued upload: %s → %s (%d bytes, MongoDB-only)",
                    file.filename, req_id, len(content))

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
    fusion_ledger_name:    str = Form(...),
    fusion_document_account: str = Form(...),
    fusion_job_name:       str = Form(...),
    gmail_subject_filter:  str = Form(...),
    notification_email:    str = Form(...),
    gmail_poll_seconds:    int = Form(60),
    mapping_threshold:     float = Form(0.70),
    ess_max_minutes:       int = Form(30),
):
    with SessionLocal() as db:
        cfg = db.get(AppSettings, 1)
        cfg.fusion_url             = fusion_url
        cfg.fusion_username        = fusion_username
        cfg.fusion_password        = fusion_password
        cfg.fusion_ledger_name     = fusion_ledger_name
        cfg.fusion_document_account= fusion_document_account
        cfg.fusion_job_name        = fusion_job_name
        cfg.gmail_subject_filter   = gmail_subject_filter
        cfg.notification_email     = notification_email
        cfg.gmail_poll_seconds     = gmail_poll_seconds
        cfg.mapping_threshold      = mapping_threshold
        cfg.ess_max_minutes        = ess_max_minutes
        cfg.updated_at             = datetime.now(timezone.utc)
        db.commit()
    return RedirectResponse("/settings?msg=Settings+saved+successfully", status_code=303)


@app.post("/settings/test-fusion")
async def test_fusion():
    cfg = get_settings()
    result = test_connection(cfg)
    return result


# ── Gmail setup ───────────────────────────────────────────────────────────────
@app.get("/gmail-setup", response_class=HTMLResponse)
async def gmail_setup_page(request: Request):
    cfg = get_settings()
    creds_exists = Path(cfg.gmail_credentials_file).exists()
    token_exists = Path(cfg.gmail_token_file).exists()
    return templates.TemplateResponse("gmail_setup.html", {
        "request": request, "creds_exists": creds_exists, "token_exists": token_exists,
        "creds_path": cfg.gmail_credentials_file,
    })


@app.post("/gmail-setup/upload-credentials")
async def upload_gmail_creds(file: UploadFile = File(...)):
    cfg = get_settings()
    dest = Path(cfg.gmail_credentials_file)
    dest.parent.mkdir(parents=True, exist_ok=True)
    content = await file.read()
    dest.write_bytes(content)
    store_secure_file("gmail_credentials", content)
    return RedirectResponse("/gmail-setup?msg=Credentials+uploaded", status_code=303)


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
    "zip": "fbdi_zip",
    "csv": "fbdi_csv",
    "bad": "bad_csv",
    "log": "ess_log",
}
_FTYPE_MIME = {
    "zip": "application/zip",
    "csv": "text/csv",
    "bad": "text/csv",
    "log": "application/zip",
}


@app.get("/download/{req_id}/{ftype}")
async def download_file(req_id: str, ftype: str):
    from fastapi.responses import Response
    kind = _FTYPE_TO_KIND.get(ftype)
    if not kind:
        return JSONResponse({"error": f"unknown ftype: {ftype}"}, 400)
    result = get_generated_file(req_id, kind)
    if not result:
        return JSONResponse({"error": "file not found in DB"}, 404)
    content, filename = result
    return Response(
        content=content,
        media_type=_FTYPE_MIME.get(ftype, "application/octet-stream"),
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


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
        req.status = "RECEIVED"
        req.error_message = None
        req.current_stage = "QUEUED"
        db.commit()
    threading.Thread(target=process_request, args=(req_id,), daemon=True).start()
    return {"message": "Retry queued", "request_id": req_id}


# ── Health ────────────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "ok", "db": "mongodb", "ml": "sentence-transformers"}


if __name__ == "__main__":
    import os as _os
    _port = int(_os.environ.get("PORT", "8000"))
    uvicorn.run("app:app", host="0.0.0.0", port=_port, reload=False, log_level="info")
