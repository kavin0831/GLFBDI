"""
Core processing workflow — runs in a background thread per journal request.
No LangGraph dependency; just plain Python with clear stage transitions.
All state is persisted to SQLite after every stage.
"""

from __future__ import annotations

import logging
import time
import uuid
import zipfile as _zipfile
from datetime import datetime, timezone
from pathlib import Path

import shutil
import tempfile

from database import (JournalRequest, MappingHistory, SessionLocal, get_settings,
                      store_log_file, append_log, hash_file, get_uploaded_file,
                      store_generated_file, get_generated_file,
                      claim_ji_request, get_ji_claim_owner)


def _attachment(req_id: str, kind: str, virtual_path: str | None) -> tuple[str, bytes] | None:
    """
    Resolve an attachment to (filename, bytes), trying disk first then MongoDB.
    `kind` matches what was passed to store_generated_file (fbdi_csv, fbdi_zip,
    bad_csv, ess_log).  Returns None if neither location has the file.
    """
    if virtual_path and Path(virtual_path).is_file():
        p = Path(virtual_path)
        try:
            return p.name, p.read_bytes()
        except Exception:
            pass
    result = get_generated_file(req_id, kind)
    if result:
        bytes_data, filename = result
        return filename, bytes_data
    return None
from services.fusion_service import (
    analyze_ess_logs, check_period_status, download_ess_logs,
    find_journal_import_jobs, get_child_requests, get_descendant_requests,
    get_ess_log, get_ess_status, get_execution_details, lookup_ledger,
    scheduled_processes_url, submit_fbdi,
)
from services.ml_mapper import load_history_boost, map_all_columns, save_mapping_to_history
from utils.fbdi_generator import build_rows, package_zip, write_bad_csv, write_csv, verify_csv
from utils.file_parser import parse_to_records, detect_file_format

logger = logging.getLogger(__name__)

STORAGE = Path(__file__).parent / "storage"
# Temp working dir (auto-cleaned per request)
_TEMP_ROOT = Path(tempfile.gettempdir()) / "oracle_fbdi_work"


def _temp_dir(req_id: str) -> Path:
    d = _TEMP_ROOT / req_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def _cleanup_temp(req_id: str):
    d = _TEMP_ROOT / req_id
    if d.exists():
        shutil.rmtree(d, ignore_errors=True)


def _db_update(request_id: str, **kwargs):
    with SessionLocal() as db:
        req = db.get(JournalRequest, request_id)
        if req:
            for k, v in kwargs.items():
                setattr(req, k, v)
            req.updated_at = datetime.now(timezone.utc)
            db.commit()


# ── Stage helpers ──────────────────────────────────────────────────────────────

def _stage_parse(req_id: str, file_path: str) -> tuple[list[dict], list[str], str] | None:
    """Returns (records, cols, file_format) or None on failure."""
    _db_update(req_id, current_stage="PARSING", status="PROCESSING")
    try:
        fhash = hash_file(file_path)
        if fhash:
            _db_update(req_id, file_hash=fhash)
        file_fmt = detect_file_format(file_path)
        append_log(req_id, "INFO", f"Detected file format: {file_fmt}")
        records, cols = parse_to_records(file_path)
        logger.info("Parsed %d rows, %d columns from %s [format=%s]",
                    len(records), len(cols), file_path, file_fmt)
        return records, cols, file_fmt
    except Exception as e:
        _db_update(req_id, status="FAILED", error_message=str(e), current_stage="PARSE_ERROR")
        _send_failure(req_id, f"File could not be parsed: {e}")
        return None


def _stage_discover(req_id: str, records: list[dict], cols: list[str]) -> dict:
    """Simple rule-based discovery of journal metadata from the data."""
    _db_update(req_id, current_stage="DISCOVERING")
    meta: dict = {}

    # Try to detect accounting date from column names
    date_cols = [c for c in cols if any(k in c.lower() for k in ("date","dt","acctg","effective","gl date","posting"))]
    if date_cols and records:
        meta["date_col"] = date_cols[0]
        meta["accounting_date"] = str(records[0].get(date_cols[0], ""))

    # Currency
    ccy_cols = [c for c in cols if any(k in c.lower() for k in ("currency","ccy","curr"))]
    if ccy_cols and records:
        val = str(records[0].get(ccy_cols[0], ""))
        if len(val) == 3 and val.isalpha():
            meta["currency_code"] = val.upper()
    meta.setdefault("currency_code", "USD")

    # Ledger name
    ledger_cols = [c for c in cols if any(k in c.lower() for k in ("ledger","book","sob"))]
    if ledger_cols and records:
        meta["ledger_from_data"] = str(records[0].get(ledger_cols[0], ""))

    cfg = get_settings()
    meta.setdefault("ledger_name", cfg.fusion_ledger_name)
    meta.setdefault("journal_category", "Manual")
    meta.setdefault("journal_source", "Manual")

    logger.info("Discovery result: %s", meta)
    return meta


def _stage_validate(req_id: str, records: list[dict], cols: list[str],
                    mappings: list[dict]) -> tuple[bool, list[int]]:
    """Basic validation: check balance, null amounts. Returns (ok, bad_row_indices)."""
    _db_update(req_id, current_stage="VALIDATING")
    src_to_tgt = {m["source_field"]: m["target_field"] for m in mappings if m.get("target_field")}
    dr_col = next((s for s,t in src_to_tgt.items() if t == "Entered Debit Amount"), None)
    cr_col = next((s for s,t in src_to_tgt.items() if t == "Entered Credit Amount"), None)

    total_dr = total_cr = 0.0
    bad_indices: list[int] = []
    errors: list[str] = []

    for i, row in enumerate(records):
        dr = _to_float(row.get(dr_col)) if dr_col else None
        cr = _to_float(row.get(cr_col)) if cr_col else None
        if dr is None and cr is None:
            bad_indices.append(i)
        else:
            if dr and dr < 0:
                bad_indices.append(i)
                errors.append(f"Row {i+1}: negative debit {dr}")
            if cr and cr < 0:
                bad_indices.append(i)
                errors.append(f"Row {i+1}: negative credit {cr}")
            total_dr += dr or 0.0
            total_cr += cr or 0.0

    balance_ok = abs(total_dr - total_cr) < 0.01
    if not balance_ok:
        errors.append(f"Journal does not balance: Dr={total_dr:.2f} Cr={total_cr:.2f}")

    good_count = len(records) - len(bad_indices)
    _db_update(req_id,
               total_rows=len(records), good_rows=good_count,
               bad_rows=len(bad_indices),
               total_debit=total_dr, total_credit=total_cr,
               validation_json={"errors": errors, "balance_ok": balance_ok,
                                 "bad_row_indices": bad_indices})

    logger.info("Validation: %d good, %d bad, balance=%s", good_count, len(bad_indices), balance_ok)
    return balance_ok, bad_indices


def _to_float(v) -> float | None:
    if v is None: return None
    try:
        s = str(v).replace(",","").strip()
        return float(s) if s else None
    except (ValueError, TypeError):
        return None


def _stage_map(req_id: str, cols: list[str], meta: dict,
               file_fmt: str = "raw_data") -> list[dict]:
    _db_update(req_id, current_stage="MAPPING")
    # For files already carrying Oracle column headers, exact-match is enough (no heavy ML)
    history_boost = load_history_boost()
    mappings = map_all_columns(cols, history_boost=history_boost)
    _db_update(req_id, mapping_json=mappings)
    mapped = sum(1 for m in mappings if m.get("target_field"))
    method_counts: dict[str, int] = {}
    for m in mappings:
        method_counts[m.get("method","?")] = method_counts.get(m.get("method","?"), 0) + 1
    append_log(req_id, "INFO",
               f"Column mapping: {mapped}/{len(cols)} mapped | methods={method_counts} | format={file_fmt}")
    logger.info("Mapped %d/%d columns (format=%s)", mapped, len(cols), file_fmt)
    return mappings


def _stage_generate(req_id: str, records: list[dict], mappings: list[dict],
                    meta: dict, bad_indices: list[int]) -> tuple[Path, Path, Path | None]:
    """Build GlInterface.csv + GlInterface.zip in a temp dir, persist all bytes to MongoDB."""
    _db_update(req_id, current_stage="GENERATING")
    out_dir = _temp_dir(req_id)

    cfg = get_settings()
    full_meta = {
        **meta,
        "request_id":    req_id,
        "bad_row_indices": bad_indices,
        "ledger_name":   meta.get("ledger_name") or cfg.fusion_ledger_name,
        # If the workflow resolved a Ledger ID via Oracle REST, pass it through.
        # build_rows already honours meta["ledger_id"] when present.
        "ledger_id":     (meta.get("ledger_id") or "").strip(),
    }
    good_rows, bad_rows = build_rows(records, mappings, full_meta)

    csv_path = out_dir / "GlInterface.csv"
    write_csv(good_rows, csv_path)

    bad_csv_path = None
    if bad_rows:
        bad_csv_path = out_dir / "bad_data.csv"
        write_bad_csv(bad_rows, bad_csv_path)

    cfg = get_settings()
    zip_path = package_zip(csv_path, full_meta, cfg.fusion_document_account)

    csv_ok, csv_errs = verify_csv(csv_path)
    if not csv_ok:
        append_log(req_id, "WARNING",
                   f"GlInterface.csv format issues ({len(csv_errs)}): {'; '.join(csv_errs[:3])}")
    else:
        append_log(req_id, "INFO",
                   f"GlInterface.csv verified OK — {len(good_rows)} rows, each ends with END")

    # Persist all generated files to MongoDB — local disk is just a working area
    store_generated_file(req_id, "fbdi_csv", csv_path.name, csv_path.read_bytes())
    store_generated_file(req_id, "fbdi_zip", zip_path.name, zip_path.read_bytes())
    if bad_csv_path:
        store_generated_file(req_id, "bad_csv", bad_csv_path.name, bad_csv_path.read_bytes())

    _db_update(req_id,
               fbdi_csv_path=csv_path.name,   # virtual: name only
               fbdi_zip_path=zip_path.name,
               bad_data_csv_path=bad_csv_path.name if bad_csv_path else None,
               good_rows=len(good_rows), bad_rows=len(bad_rows),
               fbdi_csv_hash=hash_file(csv_path),
               fbdi_zip_hash=hash_file(zip_path),
               bad_csv_hash=hash_file(bad_csv_path) if bad_csv_path else None)
    return csv_path, zip_path, bad_csv_path


def _stage_submit(req_id: str, zip_path: Path, group_id: str = "") -> str | None:
    """Submit GlInterface.zip. Returns fusion_request_id or None on failure."""
    _db_update(req_id, current_stage="SUBMITTING")
    cfg = get_settings()
    try:
        resp = submit_fbdi(cfg, str(zip_path), group_id=group_id)
        eid = str(resp.get("ReqstId",""))
        _db_update(req_id, fusion_request_id=eid or "UNKNOWN")
        if eid and eid not in ("-1", ""):
            logger.info("Submitted to Oracle Fusion: ReqstId=%s", eid)
            return eid
        # -1 means Oracle rejected the payload (wrong format, closed period, etc.)
        logger.error("Oracle returned ReqstId=%s — submission rejected", eid)
        _db_update(req_id, status="FAILED", current_stage="SUBMIT_REJECTED",
                   stop_reason=f"Oracle rejected the import (ReqstId={eid}). "
                                "Check: ledger name, period is Open, document account, and CSV format.")
        _send_failure(req_id, f"Oracle rejected the import (ReqstId={eid}). "
                              "Verify the ledger name, period status, and document account in Settings.")
        return None
    except Exception as e:
        logger.error("Fusion submission failed: %s", e)
        _db_update(req_id, status="FAILED", error_message=str(e), current_stage="SUBMIT_ERROR")
        _send_failure(req_id, f"Oracle Fusion submission failed: {e}")
        return None


def _stage_monitor(req_id: str, eid: str) -> str:
    """Poll ESS status. Returns final status string."""
    if not eid or eid in ("-1","QUEUED"):
        return "QUEUED"
    _db_update(req_id, current_stage="MONITORING")
    cfg = get_settings()
    # Adaptive backoff: short jobs finish in ~10s, so poll fast at the start
    # then settle to the configured interval for the long tail.
    deadline   = time.monotonic() + cfg.ess_max_minutes * 60
    fast_polls = 6     # ~12s of 2-second polling
    poll_n     = 0
    while time.monotonic() < deadline:
        poll_n += 1
        status = get_ess_status(cfg, eid)
        logger.info("ESS poll %d: %s", poll_n, status)
        _db_update(req_id, ess_final_status=status)
        if status in ("SUCCEEDED","ERROR","WARNING"):
            return status
        interval = 2 if poll_n <= fast_polls else cfg.ess_poll_seconds
        time.sleep(interval)
    return "TIMEOUT"


# ── Notification helpers ───────────────────────────────────────────────────────

def _send_started(req_id: str, good_rows: int, period: str):
    with SessionLocal() as db:
        req = db.get(JournalRequest, req_id)
        if not req: return
    cfg = get_settings()
    html = f"""
<html><body style="font-family:'Segoe UI',sans-serif;max-width:640px;margin:auto">
<div style="background:linear-gradient(135deg,#1a73e8,#0d47a1);color:white;padding:22px 28px;border-radius:8px 8px 0 0">
<h2 style="margin:0">🚀 GL Import Started</h2><p style="opacity:.85;margin:6px 0 0;font-size:14px">
GlInterface.zip submitted to Oracle Fusion automatically — all {good_rows} rows are valid.</p></div>
<div style="border:1px solid #ddd;padding:22px;border-radius:0 0 8px 8px">
<table style="width:100%;border-collapse:collapse">
<tr><td style="padding:8px;background:#f8f9fa;font-weight:bold;border:1px solid #e0e0e0">Request ID</td><td style="padding:8px;border:1px solid #e0e0e0">{req.id[:8]}...</td></tr>
<tr><td style="padding:8px;font-weight:bold;border:1px solid #e0e0e0">Journal</td><td style="padding:8px;border:1px solid #e0e0e0">{req.journal_name or "—"}</td></tr>
<tr><td style="padding:8px;background:#f8f9fa;font-weight:bold;border:1px solid #e0e0e0">Ledger</td><td style="padding:8px;border:1px solid #e0e0e0">{req.ledger_name}</td></tr>
<tr><td style="padding:8px;font-weight:bold;border:1px solid #e0e0e0">Period</td><td style="padding:8px;border:1px solid #e0e0e0">{period}</td></tr>
<tr><td style="padding:8px;background:#f8f9fa;font-weight:bold;border:1px solid #e0e0e0">ESS Request ID</td><td style="padding:8px;border:1px solid #e0e0e0">{req.fusion_request_id}</td></tr>
<tr><td style="padding:8px;font-weight:bold;border:1px solid #e0e0e0">Rows Imported</td><td style="padding:8px;border:1px solid #e0e0e0">{good_rows}</td></tr>
</table>
<p style="color:#666;font-size:12px;margin-top:12px">You will receive another email when the Oracle Fusion ESS job completes.</p>
</div></body></html>"""
    from services.gmail_service import send_email
    send_email(cfg.notification_email, f"🚀 FBDI Started — {req.journal_name or req.file_name} | {period}", html)


def _public_base_url() -> str:
    """
    Public-facing base URL used in approval emails so the Continue/Reject buttons
    are reachable by the recipient (not their localhost).
    Priority: APP_BASE_URL env var > SPACE_HOST (HF auto-set) > settings.app_base_url > localhost fallback.
    """
    import os as _os
    base = _os.environ.get("APP_BASE_URL", "").strip()
    if base:
        return base.rstrip("/")
    # Hugging Face Spaces sets SPACE_HOST to e.g. "kavin08028292002-glgbdi.hf.space"
    space_host = _os.environ.get("SPACE_HOST", "").strip()
    if space_host:
        return f"https://{space_host}"
    try:
        cfg_url = (get_settings().app_base_url or "").strip()
        if cfg_url:
            return cfg_url.rstrip("/")
    except Exception:
        pass
    return "http://localhost:8000"


def _send_approval_email(req_id: str, token: str, good_rows: int, bad_rows: int):
    with SessionLocal() as db:
        req = db.get(JournalRequest, req_id)
        if not req: return
    cfg = get_settings()
    base = _public_base_url()
    cont_url = f"{base}/approve/{token}?action=continue"
    rej_url  = f"{base}/approve/{token}?action=reject"
    html = f"""
<html><body style="font-family:'Segoe UI',sans-serif;max-width:640px;margin:auto">
<div style="background:linear-gradient(135deg,#f9ab00,#e37400);color:white;padding:22px 28px;border-radius:8px 8px 0 0">
<h2 style="margin:0">⚠️ GL Import — Approval Required</h2></div>
<div style="border:1px solid #ddd;padding:22px;border-radius:0 0 8px 8px">
<table style="width:100%;border-collapse:collapse;margin-bottom:18px">
<tr><td style="padding:8px;background:#fef7e0;font-weight:bold;border:1px solid #e0e0e0">File</td><td style="padding:8px;border:1px solid #e0e0e0">{req.file_name}</td></tr>
<tr><td style="padding:8px;font-weight:bold;border:1px solid #e0e0e0">Ledger</td><td style="padding:8px;border:1px solid #e0e0e0">{req.ledger_name}</td></tr>
<tr><td style="padding:8px;background:#fef7e0;font-weight:bold;border:1px solid #e0e0e0;color:#1e8e3e">✅ Good Rows</td><td style="padding:8px;border:1px solid #e0e0e0;color:#1e8e3e;font-weight:bold">{good_rows}</td></tr>
<tr><td style="padding:8px;font-weight:bold;border:1px solid #e0e0e0;color:#d93025">❌ Bad Rows</td><td style="padding:8px;border:1px solid #e0e0e0;color:#d93025;font-weight:bold">{bad_rows}</td></tr>
</table>
<p>bad_data.csv is attached. Review it and choose:</p>
<div style="text-align:center;margin:24px 0">
<a href="{cont_url}" style="background:#1e8e3e;color:white;padding:12px 28px;border-radius:6px;text-decoration:none;font-weight:bold;margin-right:12px">✅ Continue Import ({good_rows} rows)</a>
<a href="{rej_url}"  style="background:#d93025;color:white;padding:12px 28px;border-radius:6px;text-decoration:none;font-weight:bold">❌ Reject</a>
</div>
<p style="color:#999;font-size:12px">Link expires in 24 hours.</p>
</div></body></html>"""
    from services.gmail_service import send_email
    atts = []
    bad_att = _attachment(req_id, "bad_csv", req.bad_data_csv_path)
    if bad_att: atts.append(bad_att)
    send_email(cfg.notification_email,
               f"⚠️ Approval Required — {bad_rows} bad rows | {req.file_name}",
               html, atts)


def _send_success(req_id: str):
    with SessionLocal() as db:
        req = db.get(JournalRequest, req_id)
        if not req: return
    cfg = get_settings()
    oracle_link = ""
    if req.fusion_request_id and req.fusion_request_id not in ("-1", "QUEUED", ""):
        sched_url = scheduled_processes_url(cfg, req.fusion_request_id)
        oracle_link = f"""
<div style="background:#e8f0fe;border:1px solid #b8d4f8;border-radius:4px;padding:14px;margin-top:12px">
<strong>🔗 View in Oracle Fusion:</strong><br/>
<a href="{sched_url}" style="color:#1a73e8;word-break:break-all">Open Scheduled Processes → Process {req.fusion_request_id}</a>
</div>"""
    html = f"""
<html><body style="font-family:'Segoe UI',sans-serif;max-width:640px;margin:auto">
<div style="background:#1e8e3e;color:white;padding:22px 28px;border-radius:8px 8px 0 0">
<h2 style="margin:0">✅ GL Import Succeeded</h2></div>
<div style="border:1px solid #ddd;padding:22px;border-radius:0 0 8px 8px">
<table style="width:100%;border-collapse:collapse">
<tr><td style="padding:8px;background:#e6f4ea;font-weight:bold;border:1px solid #e0e0e0">ESS Request ID</td><td style="padding:8px;border:1px solid #e0e0e0">{req.fusion_request_id}</td></tr>
<tr><td style="padding:8px;font-weight:bold;border:1px solid #e0e0e0">Status</td><td style="padding:8px;border:1px solid #e0e0e0;color:#1e8e3e;font-weight:bold">{req.ess_final_status}</td></tr>
<tr><td style="padding:8px;background:#e6f4ea;font-weight:bold;border:1px solid #e0e0e0">Ledger</td><td style="padding:8px;border:1px solid #e0e0e0">{req.ledger_name}</td></tr>
<tr><td style="padding:8px;font-weight:bold;border:1px solid #e0e0e0">Period</td><td style="padding:8px;border:1px solid #e0e0e0">{req.period_name}</td></tr>
<tr><td style="padding:8px;background:#e6f4ea;font-weight:bold;border:1px solid #e0e0e0">Rows Imported</td><td style="padding:8px;border:1px solid #e0e0e0">{req.good_rows}</td></tr>
</table>{oracle_link}</div></body></html>"""
    from services.gmail_service import send_email
    send_email(cfg.notification_email, f"✅ GL Import Succeeded — {req.fusion_request_id}", html)


def _send_failure(req_id: str, reason: str = ""):
    with SessionLocal() as db:
        req = db.get(JournalRequest, req_id)
        if not req: return
    cfg = get_settings()
    reason = reason or req.stop_reason or req.error_message or "Unknown error"
    # Build Oracle UI link + JI Child status
    oracle_link = ""
    child_status_html = ""
    if req.fusion_request_id and req.fusion_request_id not in ("-1", "QUEUED", ""):
        sched_url = scheduled_processes_url(cfg, req.fusion_request_id)
        # Pull child job statuses (Import Journals: Child)
        try:
            det = get_execution_details(cfg, req.fusion_request_id)
            jobs = list(det.get("child_jobs", []))
            # Also include the separately-spawned Import Journals requests,
            # filtered to THIS submission's group_id so concurrent jobs don't bleed in.
            jobs.extend(find_journal_import_jobs(
                cfg, req.fusion_request_id, scan_range=30,
                group_id=getattr(req, "fusion_group_id", "") or ""))
            if jobs:
                rows = "".join(
                    f'<tr><td style="padding:6px;border:1px solid #ddd">{j["name"]}</td>'
                    f'<td style="padding:6px;border:1px solid #ddd"><code>{j["request_id"]}</code></td>'
                    f'<td style="padding:6px;border:1px solid #ddd;color:{"#1e8e3e" if j["status"]=="SUCCEEDED" else "#d93025" if j["status"] in ("ERROR","FAILED") else "#f9ab00"};font-weight:bold">{j["status"]}</td></tr>'
                    for j in jobs[:15]
                )
                child_status_html = f"""
<div style="margin-bottom:14px">
<strong>Child Job Statuses (Oracle ESS):</strong>
<table style="width:100%;border-collapse:collapse;margin-top:6px;font-size:13px">
<tr style="background:#f5f5f5"><th style="padding:6px;border:1px solid #ddd;text-align:left">Job</th>
<th style="padding:6px;border:1px solid #ddd;text-align:left">Request ID</th>
<th style="padding:6px;border:1px solid #ddd;text-align:left">Status</th></tr>
{rows}</table></div>"""
        except Exception:
            pass

        oracle_link = f"""
<div style="background:#e8f0fe;border:1px solid #b8d4f8;border-radius:6px;padding:16px;margin-bottom:14px">
<strong style="font-size:14px">📋 View full Journal Import Execution Report in Oracle Fusion:</strong><br/>
<a href="{sched_url}" style="color:#1a73e8;word-break:break-all;font-weight:600">Open Scheduled Processes → Process {req.fusion_request_id}</a><br/>
<small style="color:#666">Navigate: Tools → Scheduled Processes → Search Process ID {req.fusion_request_id} → View Output → Republish JI Execution Report</small><br/>
<small style="color:#999">To enable automatic log download, grant the user the "ERP Integrations Administrator" role in Oracle Fusion.</small>
</div>"""

    # Escape reason for HTML (it may contain log lines with < > &)
    import html as _html
    safe_reason = _html.escape(reason).replace("\n", "<br/>")
    html = f"""
<html><body style="font-family:'Segoe UI',sans-serif;max-width:680px;margin:auto">
<div style="background:#d93025;color:white;padding:22px 28px;border-radius:8px 8px 0 0">
<h2 style="margin:0">❌ GL Import Failed</h2></div>
<div style="border:1px solid #ddd;padding:22px;border-radius:0 0 8px 8px">
<div style="background:#fce8e6;border:1px solid #f28b82;border-radius:4px;padding:14px;margin-bottom:14px">
<strong>Reason:</strong><br/><pre style="white-space:pre-wrap;font-family:monospace;font-size:12px;margin:6px 0 0 0">{safe_reason}</pre></div>
{child_status_html}
{oracle_link}
<table style="width:100%;border-collapse:collapse">
<tr><td style="padding:8px;background:#fef0ef;font-weight:bold;border:1px solid #e0e0e0">File</td><td style="padding:8px;border:1px solid #e0e0e0">{req.file_name}</td></tr>
<tr><td style="padding:8px;font-weight:bold;border:1px solid #e0e0e0">Stage</td><td style="padding:8px;border:1px solid #e0e0e0">{req.current_stage}</td></tr>
<tr><td style="padding:8px;background:#fef0ef;font-weight:bold;border:1px solid #e0e0e0">ESS Request ID</td><td style="padding:8px;border:1px solid #e0e0e0">{req.fusion_request_id or "N/A"}</td></tr>
<tr><td style="padding:8px;font-weight:bold;border:1px solid #e0e0e0">Bad Rows</td><td style="padding:8px;border:1px solid #e0e0e0">{req.bad_rows}</td></tr>
</table>
<p style="color:#555;font-size:13px;margin-top:12px">See attached log files for details.</p>
</div></body></html>"""
    atts = []
    # 1-4: Files we generated — pulled from MongoDB by kind, fallback to disk
    for kind, virtual_path in (
        ("bad_csv",  req.bad_data_csv_path),
        ("fbdi_csv", req.fbdi_csv_path),
        ("fbdi_zip", req.fbdi_zip_path),
        ("ess_log",  req.ess_log_path),
    ):
        a = _attachment(req_id, kind, virtual_path)
        if a: atts.append(a)
    # 5. Any standalone .txt/.log files saved during analysis (legacy local-disk path)
    log_dir = STORAGE / "logs" / req_id
    if log_dir.exists():
        for f in log_dir.iterdir():
            if f.is_file() and f.suffix in (".txt", ".log"):
                atts.append(str(f))
    subj = f"❌ GL Import Failed — {req.file_name} | {req.current_stage}"
    if req.period_name == "Closed" or "Period" in reason:
        subj = f"Fusion GL Upload Failed - Period Closed — {req.ledger_name}"
    from services.gmail_service import send_email
    send_email(cfg.notification_email, subj, html, atts)


# ── FBDI detection & ZIP CSV validation ──────────────────────────────────────

def _is_fbdi_zip(file_path: str) -> bool:
    """True if the uploaded file is already a formatted GlInterface.zip."""
    if not file_path.lower().endswith(".zip"):
        return False
    try:
        with _zipfile.ZipFile(file_path) as zf:
            names_lower = [n.lower() for n in zf.namelist()]
            return "glinterface.csv" in names_lower or "manifest.xml" in names_lower
    except Exception:
        return False


def _validate_zip_csv(req_id: str, zip_path: str) -> tuple[bool, list[str], int]:
    """
    Extract and validate the CSV inside a ZIP.
    Handles both:
    - Regular data CSV with column headers
    - Headerless Oracle GlInterface.csv (positional, ends with END)
    Returns (is_valid, error_list, row_count).
    """
    import io
    import pandas as pd
    from utils.fbdi_generator import DATA_COLS

    errors: list[str] = []
    row_count = 0
    try:
        with _zipfile.ZipFile(zip_path) as zf:
            csv_names = [n for n in zf.namelist()
                         if n.lower().endswith(".csv") and not n.startswith("__")]
            if not csv_names:
                return False, ["No CSV file found inside ZIP"], 0
            csv_name = next((n for n in csv_names if "glinterface" in n.lower()), csv_names[0])
            raw_bytes = zf.read(csv_name)

        # Detect if headerless FBDI CSV (first cell = "NEW")
        try:
            first_line = raw_bytes.decode("utf-8", errors="replace").splitlines()[0]
            first_val  = first_line.split(",")[0].strip().strip('"').upper()
            is_headerless = first_val == "NEW"
        except Exception:
            is_headerless = False

        if is_headerless:
            # Positional format — assign Oracle column names, check DR/CR by position
            try:
                df = pd.read_csv(io.BytesIO(raw_bytes), header=None, dtype=str,
                                 on_bad_lines="skip")
                # Assign Oracle column names up to available columns
                n = min(df.shape[1], len(DATA_COLS))
                df.columns = list(DATA_COLS[:n]) + list(range(n, df.shape[1]))
                dr_col = "Entered Debit Amount"
                cr_col = "Entered Credit Amount"
            except Exception as e:
                return False, [f"Could not parse headerless CSV in ZIP: {e}"], 0
        else:
            # Regular CSV with headers
            try:
                df = pd.read_csv(io.BytesIO(raw_bytes), dtype=str, on_bad_lines="skip")
            except Exception:
                return False, [f"Could not parse {csv_name} inside ZIP"], 0
            cols_lower = {c.lower(): c for c in df.columns}
            dr_col = next((c for k, c in cols_lower.items()
                           if "debit" in k or "dr_amount" in k or "entered_dr" in k), None)
            cr_col = next((c for k, c in cols_lower.items()
                           if "credit" in k or "cr_amount" in k or "entered_cr" in k), None)

        row_count = len(df)
        if row_count == 0:
            return False, ["CSV inside ZIP has no data rows"], 0

        if dr_col and cr_col and dr_col in df.columns and cr_col in df.columns:
            total_dr = pd.to_numeric(df[dr_col], errors="coerce").fillna(0).sum()
            total_cr = pd.to_numeric(df[cr_col], errors="coerce").fillna(0).sum()
            if abs(total_dr - total_cr) > 0.01:
                errors.append(
                    f"Journal does not balance: Dr={total_dr:.2f} Cr={total_cr:.2f}")
            else:
                append_log(req_id, "INFO",
                           f"ZIP balance OK: Dr={total_dr:.2f} Cr={total_cr:.2f}")
        else:
            append_log(req_id, "WARNING",
                       "Debit/credit columns not found in ZIP CSV — balance check skipped")

        append_log(req_id, "INFO",
                   f"ZIP validation: {row_count} rows in {csv_name}, "
                   f"headerless={is_headerless}, errors={len(errors)}")
    except Exception as e:
        return False, [f"ZIP validation error: {e}"], 0

    return len(errors) == 0, errors, row_count


def _direct_submit(request_id: str, file_path: str, zip_path: Path):
    """
    Submit an already-formatted GlInterface.zip directly to Oracle.
    Validates the CSV data inside before submitting, persists the ZIP+CSV
    bytes to MongoDB so the /download endpoints work, then runs the same
    JI correlation + log download as the full pipeline.
    """
    logger.info("Direct FBDI submission for %s", request_id)
    _db_update(request_id, status="PROCESSING", current_stage="VALIDATING_ZIP",
               fbdi_zip_path=zip_path.name)

    # Validate CSV inside the ZIP before submitting
    is_valid, val_errors, row_count = _validate_zip_csv(request_id, str(zip_path))
    _db_update(request_id, total_rows=row_count)
    if not is_valid and val_errors:
        for e in val_errors:
            append_log(request_id, "WARNING", f"ZIP validation: {e}")
        if row_count == 0:
            _db_update(request_id, status="FAILED", current_stage="VALIDATION_FAILED",
                       error_message="; ".join(val_errors))
            _send_failure(request_id, "ZIP file contains no valid data: " + "; ".join(val_errors))
            return
    else:
        append_log(request_id, "INFO",
                   f"ZIP validated OK: {row_count} rows, {len(val_errors)} warnings")

    # Persist the FBDI ZIP and the CSV inside it into MongoDB so the
    # request detail page's Downloads card can serve them.
    try:
        zip_bytes = zip_path.read_bytes()
        store_generated_file(request_id, "fbdi_zip", "GlInterface.zip", zip_bytes)
        import zipfile as _zf, io
        with _zf.ZipFile(io.BytesIO(zip_bytes)) as zf:
            csv_name = next((n for n in zf.namelist()
                             if n.lower().endswith(".csv") and not n.startswith("__")), None)
            if csv_name:
                store_generated_file(request_id, "fbdi_csv", "GlInterface.csv", zf.read(csv_name))
        _db_update(request_id,
                   fbdi_zip_hash=hash_file(zip_path),
                   fbdi_csv_path="GlInterface.csv")
    except Exception as e:
        logger.warning("Could not persist pre-built FBDI to MongoDB: %s", e)

    _db_update(request_id, current_stage="DIRECT_SUBMIT")

    # group_id: same hash-derived approach as the full pipeline so JI correlation works
    group_id = str(abs(hash(request_id)) % 999999999)
    _db_update(request_id, fusion_group_id=group_id)
    eid = _stage_submit(request_id, zip_path, group_id=group_id)
    if eid is None:
        return

    cfg = get_settings()
    _send_started(request_id, row_count, cfg.fusion_ledger_name)

    final_status = _stage_monitor(request_id, eid)

    # Find OUR Import Journals jobs (group_id-filtered, same as full pipeline)
    ji_jobs: list = []
    if eid not in ("-1", "QUEUED", ""):
        for _ in range(5):
            time.sleep(3)
            parents = find_journal_import_jobs(cfg, eid, scan_range=30, group_id=group_id)
            parents = [p for p in parents if "child" not in p.get("name", "").lower()]
            children: list = []
            for p in parents:
                for d in get_descendant_requests(cfg, p["request_id"]):
                    nm = d.get("name", "")
                    if ("Import Journals" in nm or "JournalImport" in nm) \
                       and "child" in nm.lower():
                        children.append({"request_id": d["request_id"], "name": nm,
                                         "status": d.get("status", ""), "path": ""})
            ji_jobs = parents + children
            if ji_jobs:
                break
        for j in ji_jobs:
            claim_ji_request(str(j["request_id"]), request_id, j.get("name", ""))

        # Download + store ESS logs (same code path as the full pipeline)
        try:
            logs = download_ess_logs(cfg, eid, group_id=group_id, ji_jobs=ji_jobs)
            if logs.get("zip_bytes"):
                import hashlib as _hl
                short_id = request_id[:8]
                log_zip_name = f"{short_id}_ESS_Logs_{eid}.zip"
                store_generated_file(request_id, "ess_log", log_zip_name, logs["zip_bytes"])
                store_log_file(request_id, log_zip_name, logs["zip_bytes"])
                _db_update(request_id, ess_log_path=log_zip_name,
                           ess_log_hash=_hl.sha256(logs["zip_bytes"]).hexdigest())
                import re as _re
                rid_to_name = logs.get("rid_to_name", {})
                seen_hashes: set[str] = set()
                for fname, content in logs["files"].items():
                    if not ("ImportJournals" in fname or "JournalImport" in fname
                            or fname.endswith(".log") or fname.endswith(".out")):
                        continue
                    body_bytes = content.encode("utf-8", errors="replace")
                    body_hash = _hl.sha256(body_bytes).hexdigest()
                    if body_hash in seen_hashes: continue
                    seen_hashes.add(body_hash)
                    parts = fname.split("/", 1)
                    download_rid = parts[0] if len(parts) > 1 else ""
                    file_part = parts[1] if len(parts) > 1 else fname
                    m = _re.search(r"(\d{6,})", file_part)
                    real_rid = m.group(1) if m else download_rid
                    proc_name = rid_to_name.get(real_rid) or rid_to_name.get(download_rid) or "ess"
                    new_name = f"{short_id}_{proc_name}_{real_rid or download_rid}.log"
                    store_log_file(request_id, new_name, body_bytes)
        except Exception as e:
            logger.warning("Could not download ESS log zip in direct_submit: %s", e)

    if eid in ("-1", "QUEUED", "") or final_status in ("SUCCEEDED", "WARNING", "QUEUED"):
        if eid == "-1":
            _db_update(request_id, status="FAILED", current_stage="SUBMIT_REJECTED",
                       stop_reason="Oracle returned ReqstId=-1. Check the ZIP format, ledger name, and period in Oracle Fusion.")
            _send_failure(request_id, "Oracle returned ReqstId=-1. The ZIP was rejected — verify the ledger and period are correct in Oracle Fusion.")
        else:
            _db_update(request_id, status="SUCCEEDED", current_stage="COMPLETED")
            _send_success(request_id)
    else:
        _db_update(request_id, status="FAILED", current_stage="ESS_FAILED",
                   stop_reason=f"ESS job ended with status: {final_status}")
        _send_failure(request_id, f"Oracle Fusion ESS job status: {final_status}")


# ── Main workflow entry point ─────────────────────────────────────────────────

def process_request(request_id: str):
    """Public entry point — guarantees temp dir cleanup."""
    try:
        _process_request_impl(request_id)
    finally:
        _cleanup_temp(request_id)


def _process_request_impl(request_id: str):
    """
    Full processing pipeline for one journal file.
    Called in a background thread by the FastAPI app.
    """
    logger.info("=== Starting workflow for %s ===", request_id)
    append_log(request_id, "INFO", f"Workflow started for request {request_id}")
    with SessionLocal() as db:
        req = db.get(JournalRequest, request_id)
        if not req:
            logger.error("Request not found: %s", request_id)
            return
        file_name = req.file_path or req.file_name or "uploaded.csv"
        # File lives in MongoDB. Materialise it into the per-request temp dir.
        restored = get_uploaded_file(request_id)
        if restored is None:
            _db_update(request_id, status="FAILED",
                       error_message="Source file not found in MongoDB")
            return
        td = _temp_dir(request_id)
        file_path = str(td / Path(file_name).name)
        Path(file_path).write_bytes(restored)
        append_log(request_id, "INFO",
                   f"Source file restored from MongoDB to temp dir ({len(restored)} bytes)")

    # Shortcut: already-formatted GlInterface.zip → validate CSV inside + submit directly
    if _is_fbdi_zip(file_path):
        logger.info("Detected pre-built FBDI ZIP — validating CSV inside, then submitting")
        _direct_submit(request_id, file_path, Path(file_path))
        return

    # Shortcut: headerless GlInterface.csv uploaded directly → package to ZIP + submit
    if file_path.lower().endswith((".csv", ".txt")):
        if detect_file_format(file_path) == "fbdi_headerless":
            logger.info("Detected headerless FBDI CSV — packaging to ZIP and submitting directly")
            append_log(request_id, "INFO", "Headerless FBDI CSV detected — packaging as ZIP")
            from utils.fbdi_generator import package_zip as _pkg_zip
            out_dir  = _temp_dir(request_id)
            csv_dest = out_dir / "GlInterface.csv"
            shutil.copy2(file_path, csv_dest)
            cfg_s = get_settings()
            zip_path = _pkg_zip(csv_dest, {"request_id": request_id}, cfg_s.fusion_document_account)
            store_generated_file(request_id, "fbdi_csv", csv_dest.name, csv_dest.read_bytes())
            store_generated_file(request_id, "fbdi_zip", zip_path.name, zip_path.read_bytes())
            _db_update(request_id, fbdi_csv_path=csv_dest.name, fbdi_zip_path=zip_path.name,
                       fbdi_csv_hash=hash_file(csv_dest), fbdi_zip_hash=hash_file(zip_path))
            _direct_submit(request_id, file_path, zip_path)
            return

    # 1. Parse
    result = _stage_parse(request_id, file_path)
    if not result: return
    records, cols, file_fmt = result

    # 2. Discover metadata
    meta = _stage_discover(request_id, records, cols)

    # 3. Map columns
    mappings = _stage_map(request_id, cols, meta, file_fmt)

    # 4. Validate
    cfg = get_settings()
    balance_ok, bad_indices = _stage_validate(request_id, records, cols, mappings)

    # Update meta with bad indices for FBDI generator
    meta["bad_row_indices"] = bad_indices
    meta.setdefault("ledger_name", cfg.fusion_ledger_name)

    # 4b. Resolve & validate the ledger against Oracle's REST API — ONCE per file.
    #
    # Rules:
    #   - All rows in the file must agree on *Ledger ID and on Ledger Name.
    #     Mixed values within one file = bad data → fail the whole submission.
    #   - When the data has one ID OR one name, make exactly ONE REST call to
    #     validate / resolve it. Inject the result into meta so build_rows
    #     writes the same ID into every row.
    _ids   = {str(r.get("*Ledger ID","")).strip()  for r in records}
    _names = {str(r.get("Ledger Name","")).strip() for r in records}
    _ids.discard("");  _names.discard("")

    if len(_ids) > 1:
        msg = f"Data file has mixed *Ledger ID values across rows: {sorted(_ids)}"
        _db_update(request_id, status="FAILED", current_stage="VALIDATION_FAILED",
                   error_message=msg, stop_reason=msg)
        append_log(request_id, "ERROR", msg)
        _send_failure(request_id, msg)
        return
    if len(_names) > 1:
        msg = f"Data file has mixed Ledger Name values across rows: {sorted(_names)}"
        _db_update(request_id, status="FAILED", current_stage="VALIDATION_FAILED",
                   error_message=msg, stop_reason=msg)
        append_log(request_id, "ERROR", msg)
        _send_failure(request_id, msg)
        return

    _data_id   = next(iter(_ids),   "")
    _data_name = next(iter(_names), "") or (cfg.fusion_ledger_name or "").strip()

    resolved = None
    if _data_id:
        resolved = lookup_ledger(cfg, ledger_id=_data_id)
        if not resolved:
            append_log(request_id, "WARNING",
                       f"Data file *Ledger ID '{_data_id}' not found in Oracle — "
                       "falling back to Ledger Name lookup")
    if not resolved and _data_name:
        resolved = lookup_ledger(cfg, name=_data_name)

    if resolved and resolved.get("ledger_id"):
        meta["ledger_id"]   = resolved["ledger_id"]
        meta["ledger_name"] = resolved["name"] or _data_name
        append_log(request_id, "INFO",
                   f"Resolved ledger once via REST: name='{resolved['name']}' "
                   f"id={resolved['ledger_id']} (applied to all {len(records)} rows)")
    elif _data_name:
        msg = (f"Ledger Name '{_data_name}' is not a valid Oracle ledger "
               "and the data file has no usable *Ledger ID")
        _db_update(request_id, status="FAILED", current_stage="VALIDATION_FAILED",
                   error_message=msg, stop_reason=msg)
        append_log(request_id, "ERROR", msg)
        _send_failure(request_id, msg)
        return

    # 5. Generate FBDI files
    csv_path, zip_path, bad_csv_path = _stage_generate(request_id, records, mappings, meta, bad_indices)

    good_count = len(records) - len(bad_indices)
    bad_count  = len(bad_indices)

    # Read period from generated CSV meta
    from utils.fbdi_generator import _period
    period = _period(meta.get("accounting_date","")) or cfg.fusion_ledger_name

    # Update journal metadata
    _db_update(request_id,
               journal_name=meta.get("journal_name") or Path(file_path).stem,
               ledger_name=meta.get("ledger_name") or cfg.fusion_ledger_name,
               accounting_date=meta.get("accounting_date",""),
               currency_code=meta.get("currency_code","USD"),
               journal_category=meta.get("journal_category","Manual"),
               journal_source=meta.get("journal_source","Manual"),
               period_name=period)

    # 6. Approval check if mixed good/bad rows
    if bad_count > 0:
        if good_count == 0:
            _db_update(request_id, status="FAILED",
                       stop_reason="All rows failed validation — nothing to import.",
                       current_stage="VALIDATION_FAILED")
            _send_failure(request_id, "All rows failed validation — nothing to import.")
            save_mapping_to_history(mappings)
            return

        # Mixed — send approval email and wait
        token = str(uuid.uuid4())
        _db_update(request_id, approval_token=token, approval_status="PENDING",
                   current_stage="AWAITING_APPROVAL")
        _send_approval_email(request_id, token, good_count, bad_count)

        # Wait for approval (poll DB every 10s, up to 24h)
        decision = _wait_for_approval(request_id, timeout=86400)
        if decision != "APPROVED":
            _db_update(request_id, status="FAILED", current_stage="REJECTED",
                       stop_reason=f"Import {decision.lower()} by finance user.")
            _send_failure(request_id, f"Import was {decision.lower()} by finance user.")
            return

    # 7. Period status check (non-blocking — warn but don't stop)
    period_status = check_period_status(cfg, meta.get("ledger_name",""), period)
    logger.info("Period status for %s: %s", period, period_status)
    if period_status == "Closed":
        _db_update(request_id, status="FAILED", current_stage="PERIOD_CLOSED",
                   stop_reason=f"Period {period} is closed for ledger {meta.get('ledger_name','')}.")
        _send_failure(request_id, f"Period {period} is closed. Journal cannot be imported.")
        return
    elif period_status == "Error":
        logger.warning("Period status check failed — proceeding anyway")

    # 8. Submit to Oracle Fusion
    # Always generate a unique numeric identifier from our internal request_id.
    # Any Interface Group Identifier column in the user's data file is ignored;
    # we control this value so concurrent submissions stay correlated.
    group_id = str(abs(hash(request_id)) % 999999999)
    _db_update(request_id, fusion_group_id=group_id)
    eid = _stage_submit(request_id, zip_path, group_id=group_id)
    if eid is None: return  # failure already handled inside _stage_submit

    # 9. Send "FBDI Started" notification (only if no bad rows OR approved)
    _send_started(request_id, good_count, period)

    # 10. Monitor ESS job
    final_status = _stage_monitor(request_id, eid)

    # Visible progress: ESS done, now fetching JI jobs + logs
    if eid not in ("-1", "QUEUED", ""):
        _db_update(request_id, current_stage="FETCHING_LOGS")

    # 11. Inspect inner job statuses + try downloading logs
    log_summary = ""
    inner_failed = False
    if eid not in ("-1", "QUEUED", ""):
        # 11a. Direct children of the submission ID
        details = get_execution_details(cfg, eid)
        worst = details.get("worst_child_status", "")
        children = details.get("child_jobs", [])

        # 11b. Find the separately-spawned "Import Journals" requests.
        #
        # DEFINITIVE correlation (verified against live Oracle Fusion):
        # - Each JI parent's `requestParameters` contains `submit.argument4` = our
        #   group_id (the 4th arg of our JournalImportLauncher ParameterList).
        # - Each JI Child links to its JI parent via `absParentRequestId` (and
        #   `parentRequestId`), reachable through the Scheduler REST API.
        # Two-step lookup: filter parents by group_id, then pull children of those parents.
        ji_jobs: list = []
        for attempt in range(2):
            time.sleep(3)
            # Step 1: JI parents whose submit.argument4 equals our group_id
            parents = find_journal_import_jobs(cfg, eid, scan_range=30, group_id=group_id)
            # Drop any "Child" rows here — find_journal_import_jobs rejects them via
            # group_id mismatch anyway, but be explicit.
            parents = [p for p in parents if "child" not in p.get("name","").lower()]

            # Step 2: pull each parent's descendants → finds the Import Journals: Child
            children = []
            for p in parents:
                for d in get_descendant_requests(cfg, p["request_id"]):
                    nm = d.get("name", "")
                    if ("Import Journals" in nm or "JournalImport" in nm) \
                       and "child" in nm.lower():
                        children.append({
                            "request_id": d["request_id"],
                            "name":       nm,
                            "status":     d.get("status", ""),
                            "path":       "",
                        })

            ji_jobs = parents + children
            if ji_jobs:
                break

        # Belt-and-braces: claim each so any concurrent fallback path elsewhere
        # can never pick up our jobs by accident.
        for j in ji_jobs:
            claim_ji_request(str(j["request_id"]), request_id, j.get("name", ""))

        _ji_summary = ", ".join(f"{j.get('name','?')}={j.get('request_id','?')}"
                                 for j in ji_jobs)
        append_log(request_id, "INFO",
                   f"Found {len(ji_jobs)} Import Journals job(s) for this submission "
                   f"({_ji_summary})")
        for j in ji_jobs:
            j_copy = {"name": j["name"], "request_id": j["request_id"],
                       "status": j["status"], "path": ""}
            children.append(j_copy)
            order = {"SUCCEEDED":0,"RUNNING":1,"WARNING":2,"ERROR":3,"FAILED":3}
            if order.get(j["status"], -1) > order.get(worst, -1):
                worst = j["status"]

        if children:
            jobs_summary = " | ".join(f"{j['name']}={j['status']}" for j in children)
            logger.info("Child jobs: %s", jobs_summary)
            has_failures = worst in ("WARNING","ERROR","FAILED","CANCELLED")
            if has_failures:
                inner_failed = True
                log_summary = f"Import Journals status: {worst}. Jobs: {jobs_summary}"
                _db_update(request_id, stop_reason=log_summary)

        # 11b. Download log+output zip — pass our CLAIMED JI jobs explicitly so
        # download_ess_logs doesn't re-scan and pick up other submissions' jobs.
        try:
            logs = download_ess_logs(cfg, eid, group_id=group_id, ji_jobs=ji_jobs)
            if logs.get("zip_bytes"):
                import hashlib as _hl
                short_id = request_id[:8]
                # Combined zip name follows the {request_id}_{process_name} convention
                log_zip_name = f"{short_id}_ESS_Logs_{eid}.zip"
                store_generated_file(request_id, "ess_log", log_zip_name, logs["zip_bytes"])
                store_log_file(request_id, log_zip_name, logs["zip_bytes"])
                _db_update(request_id,
                           ess_log_path=log_zip_name,
                           ess_log_hash=_hl.sha256(logs["zip_bytes"]).hexdigest())
                # Rename per-job log files to {request_id}_{process_name}_{rid}.log
                # - Detect which Oracle request_id the FILE is actually about by parsing
                #   the embedded rid in the filename (Oracle names them like "9722401.log").
                # - Deduplicate by SHA-256 of content so the same log doesn't get stored
                #   twice when it appears in both the parent's zip and the child's zip.
                import re as _re
                rid_to_name = logs.get("rid_to_name", {})
                seen_hashes: set[str] = set()
                for fname, content in logs["files"].items():
                    if not ("ImportJournals" in fname or "JournalImport" in fname
                            or fname.endswith(".log") or fname.endswith(".out")):
                        continue
                    body_bytes = content.encode("utf-8", errors="replace")
                    body_hash  = _hl.sha256(body_bytes).hexdigest()
                    if body_hash in seen_hashes:
                        continue          # exact duplicate of a file already stored
                    seen_hashes.add(body_hash)

                    parts        = fname.split("/", 1)
                    download_rid = parts[0] if len(parts) > 1 else ""
                    file_part    = parts[1] if len(parts) > 1 else fname

                    # Prefer the rid embedded in the FILENAME (it identifies the actual
                    # process that wrote this log); fall back to the download container's rid.
                    m = _re.search(r"(\d{6,})", file_part)
                    real_rid = m.group(1) if m else download_rid
                    proc_name = rid_to_name.get(real_rid) \
                                or rid_to_name.get(download_rid) \
                                or "ess"

                    new_name = f"{short_id}_{proc_name}_{real_rid or download_rid}.log"
                    store_log_file(request_id, new_name, body_bytes)

                # If we got logs, parse for granular error codes
                analysis = analyze_ess_logs(logs)
                if analysis["has_errors"]:
                    inner_failed = True
                    detail = "\n".join(analysis["detail_lines"][:10])
                    log_summary = f"{analysis['summary']}\n\n{detail}"
                    _db_update(request_id, stop_reason=log_summary)
        except Exception as e:
            logger.warning("Could not download ESS log zip: %s", e)

    # 12. Final notification — treat inner-job errors as failures even if ESS SUCCEEDED
    ess_succeeded = final_status in ("SUCCEEDED", "WARNING", "QUEUED")
    if ess_succeeded and not inner_failed:
        _db_update(request_id, status="SUCCEEDED", current_stage="COMPLETED")
        save_mapping_to_history(mappings)
        _send_success(request_id)
    elif ess_succeeded and inner_failed:
        _db_update(request_id, status="FAILED", current_stage="IMPORT_ERRORS",
                   stop_reason=f"Oracle Journal Import rejected rows. {log_summary}")
        _send_failure(request_id,
                      f"Oracle Fusion accepted the file but the Journal Import job rejected rows.\n\n{log_summary}")
    else:
        _db_update(request_id, status="FAILED", current_stage="ESS_FAILED",
                   stop_reason=f"ESS job ended with status: {final_status}. {log_summary}")
        _send_failure(request_id,
                      f"Oracle Fusion ESS job status: {final_status}. {log_summary}")

    logger.info("=== Workflow complete: %s → %s (inner_failed=%s) ===",
                request_id, final_status, inner_failed)
    append_log(request_id, "INFO", f"Workflow complete: final_status={final_status} inner_failed={inner_failed}")


def _wait_for_approval(request_id: str, timeout: int = 86400) -> str:
    """Poll DB every 10 seconds until approved/rejected or timeout."""
    elapsed = 0
    while elapsed < timeout:
        with SessionLocal() as db:
            req = db.get(JournalRequest, request_id)
            if req and req.approval_status in ("APPROVED","REJECTED"):
                return req.approval_status
        time.sleep(10)
        elapsed += 10
    _db_update(request_id, approval_status="TIMEOUT")
    return "TIMEOUT"
