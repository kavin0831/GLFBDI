"""
AP Invoice processing workflow.

Mirrors the GL workflow (workflow.py) but for Accounts Payable invoices:
parse → map → validate (BU/Supplier/Site/Terms/Distribution) → generate
ApInvoicesInterface.csv + ApInvoiceLinesInterface.csv → submit via
importBulkData / APXIIMPT → poll → find spawned "Import Payables Invoices"
jobs → download logs → analyze → notify.
"""

from __future__ import annotations

import logging
import time
import uuid
import zipfile as _zipfile
from datetime import datetime, timezone
from pathlib import Path

from database import (
    JournalRequest, append_log, get_settings, store_generated_file,
    has_generated_file, get_generated_file,
)
from services.fusion_service import (
    analyze_ap_bip_xml, analyze_ess_logs, download_ess_logs,
    find_ap_import_jobs, get_ess_status, get_execution_details,
    lookup_supplier, lookup_supplier_site,
    scheduled_processes_url, submit_ap_fbdi,
)
from services.ml_mapper import map_all_ap_columns
from utils.ap_fbdi_generator import (
    AP_HEADER_COLUMNS, AP_LINE_COLUMNS,
    build_ap_rows, package_ap_zip, split_multi_invoice_zip,
    write_ap_bad_csv, write_ap_header_csv, write_ap_lines_csv,
)
from utils.file_parser import parse_to_records

logger = logging.getLogger(__name__)

STORAGE = Path(__file__).parent / "storage"


# ── DB helpers ───────────────────────────────────────────────────────────────

def _db_update(request_id: str, **kwargs) -> None:
    """Update fields on a JournalRequest atomically."""
    from database import _mdb
    kwargs["updated_at"] = datetime.now(timezone.utc)
    _mdb()["journal_requests"].update_one({"_id": request_id}, {"$set": kwargs})


def _log(request_id: str, level: str, msg: str) -> None:
    try:
        append_log(request_id, level, msg)
    except Exception:
        pass
    logger.log(getattr(logging, level.upper(), logging.INFO), "[%s] %s", request_id[:8], msg)


# ── AP-specific pre-validations ──────────────────────────────────────────────

def validate_ap_invoices(
    records: list[dict], mappings: list[dict], meta: dict,
) -> tuple[list[int], list[str]]:
    """
    Returns (bad_row_indices, error_messages).

    Checks:
      - Each line must have either Distribution Combination OR Distribution Set
      - Invoice Amount > 0
      - For each unique Invoice Number, sum of line amounts == invoice amount (±0.01)
      - Date columns parseable
    """
    bad_idx: set[int] = set()
    errs: list[str] = []

    src_to_tgt = {m["source_field"]: m["target_field"]
                   for m in mappings if m.get("target_field")}

    def _val(rec, target_field):
        for s, t in src_to_tgt.items():
            if t == target_field and rec.get(s) not in (None, ""):
                return str(rec[s]).strip()
        return ""

    invoice_amounts: dict[str, float] = {}
    line_sums:       dict[str, float] = {}

    for i, r in enumerate(records):
        inv_num = _val(r, "*Invoice Number") or _val(r, "Invoice Number")
        line_amt = _val(r, "*Amount") or _val(r, "Amount")
        inv_amt  = _val(r, "*Invoice Amount") or _val(r, "Invoice Amount")
        dist_set = _val(r, "Distribution Set")
        dist_cmb = _val(r, "Distribution Combination")

        if not inv_num:
            bad_idx.add(i); errs.append(f"Row {i+1}: missing Invoice Number")
            continue

        # Line-level: must have Distribution Combination OR Distribution Set
        if line_amt:
            if not dist_set and not dist_cmb:
                bad_idx.add(i)
                errs.append(f"Row {i+1} (Invoice {inv_num}): line must have either "
                            f"Distribution Combination or Distribution Set")
            try:
                amt = float(line_amt.replace(",", ""))
                line_sums[inv_num] = line_sums.get(inv_num, 0.0) + amt
            except (ValueError, TypeError):
                bad_idx.add(i); errs.append(f"Row {i+1}: bad line Amount '{line_amt}'")

        # Header-level: capture invoice amount
        if inv_amt and inv_num not in invoice_amounts:
            try:
                invoice_amounts[inv_num] = float(inv_amt.replace(",", ""))
                if invoice_amounts[inv_num] <= 0:
                    errs.append(f"Invoice {inv_num}: amount must be > 0 (got {invoice_amounts[inv_num]})")
                    bad_idx.add(i)
            except (ValueError, TypeError):
                bad_idx.add(i); errs.append(f"Row {i+1}: bad Invoice Amount '{inv_amt}'")

    # Cross-check: line sum == invoice amount per invoice.
    # Also mark all rows for that invoice as bad so the FBDI skips them.
    for inv_num, inv_amt in invoice_amounts.items():
        line_sum = line_sums.get(inv_num, 0.0)
        if abs(inv_amt - line_sum) > 0.01:
            errs.append(
                f"Invoice {inv_num}: lines sum to {line_sum:.2f} "
                f"but header amount is {inv_amt:.2f} (difference {abs(inv_amt-line_sum):.2f})"
            )
            # Mark every row belonging to this invoice as bad
            for i, r in enumerate(records):
                n = _val(r, "*Invoice Number") or _val(r, "Invoice Number")
                if n == inv_num:
                    bad_idx.add(i)

    return sorted(bad_idx), errs


def validate_ap_master_data(cfg, meta: dict, records: list[dict],
                             mappings: list[dict]) -> tuple[list[int], list[str]]:
    """
    REST-based master-data validation against Oracle Fusion before submission.

    Validates per unique (Supplier Number/Name, Supplier Site) combination:
      1. Supplier exists via GET /suppliers?q=SupplierNumber='{n}'
      2. Supplier Site exists via GET /suppliers/{id}/child/sites?q=SupplierSite='{s}'

    Returns (bad_row_indices, error_messages).
    Rows for invoices with invalid supplier/site are flagged so they are
    excluded from the FBDI file (same as structural validation failures).
    """
    src_to_tgt = {m["source_field"]: m["target_field"]
                  for m in mappings if m.get("target_field")}

    def _val(rec, *targets):
        for tgt in targets:
            for s, t in src_to_tgt.items():
                if t == tgt and rec.get(s) not in (None, ""):
                    return str(rec[s]).strip()
        return ""

    # Collect unique (supplier_num_or_name, site) pairs and which invoice numbers
    # they belong to — so we can batch the REST calls.
    # Structure: {(sup_num, sup_name, site): {invoice_nums}}
    combo_invoices: dict[tuple, set] = {}
    inv_to_row_indices: dict[str, list[int]] = {}

    for i, r in enumerate(records):
        inv_num    = _val(r, "*Invoice Number", "Invoice Number")
        sup_num    = _val(r, "**Supplier Number", "Supplier Number")
        sup_name   = _val(r, "**Supplier Name",   "Supplier Name")
        site       = _val(r, "*Supplier Site",     "Supplier Site")
        key = (sup_num, sup_name, site)
        combo_invoices.setdefault(key, set()).add(inv_num)
        inv_to_row_indices.setdefault(inv_num, []).append(i)

    bad_idx: set[int] = set()
    errs: list[str] = []

    # Cache lookup results to avoid hitting Oracle multiple times per supplier
    supplier_cache: dict[str, dict] = {}   # sup_num or sup_name → {SupplierId, ...}
    site_cache: dict[tuple, bool]   = {}   # (supplier_id, site) → valid?

    for (sup_num, sup_name, site), inv_nums in combo_invoices.items():
        inv_list = ", ".join(sorted(inv_nums)[:5])

        # 1. Supplier lookup
        cache_key = sup_num or sup_name
        if cache_key not in supplier_cache:
            supplier_cache[cache_key] = lookup_supplier(
                cfg,
                supplier_number=sup_num,
                supplier_name=sup_name if not sup_num else "",
            )
        sup_info = supplier_cache[cache_key]

        if not sup_info:
            label = f"#{sup_num}" if sup_num else f'"{sup_name}"'
            msg = (f"Supplier {label} not found in Oracle — "
                   f"invoices: {inv_list}")
            errs.append(msg)
            for inv in inv_nums:
                for idx in inv_to_row_indices.get(inv, []):
                    bad_idx.add(idx)
            continue   # no point checking site if supplier invalid

        supplier_id = sup_info.get("SupplierId", "")

        # 2. Supplier Site lookup (only if site is provided in the data)
        if site and supplier_id:
            site_key = (supplier_id, site)
            if site_key not in site_cache:
                site_info = lookup_supplier_site(cfg, supplier_id, site)
                site_cache[site_key] = bool(site_info)
            if not site_cache[(supplier_id, site)]:
                msg = (f"Supplier site '{site}' not found for supplier "
                       f"{sup_info.get('SupplierName','?')} (#{sup_num}) — "
                       f"invoices: {inv_list}")
                errs.append(msg)
                for inv in inv_nums:
                    for idx in inv_to_row_indices.get(inv, []):
                        bad_idx.add(idx)

    return sorted(bad_idx), errs


# ── Multi-invoice ZIP handling ───────────────────────────────────────────────

def _maybe_split_zip(file_path: str) -> list[tuple[bytes, bytes]] | None:
    """
    If the upload is a ZIP, see if it contains pre-built FBDI CSVs. Returns a
    list of (hdr_bytes, line_bytes) tuples — one per invoice batch — or None
    if the upload isn't a multi-invoice ZIP.
    """
    if not file_path.lower().endswith(".zip"):
        return None
    try:
        zb = Path(file_path).read_bytes()
        pairs = split_multi_invoice_zip(zb)
        return pairs or None
    except Exception:
        return None


# ── Stage helpers ────────────────────────────────────────────────────────────

def _stage_parse(req_id: str, file_path: str):
    _db_update(req_id, current_stage="PARSING", status="PROCESSING")
    _log(req_id, "INFO", f"Parsing AP file: {Path(file_path).name}")
    records, cols = parse_to_records(file_path)
    _log(req_id, "INFO", f"Parsed {len(records)} rows, {len(cols)} columns")
    return records, cols


def _stage_map(req_id: str, cols: list[str]) -> list[dict]:
    _db_update(req_id, current_stage="MAPPING")
    mappings = map_all_ap_columns(cols, target_set="both")
    _db_update(req_id, mapping_json=mappings)
    mapped = sum(1 for m in mappings if m.get("target_field"))
    _log(req_id, "INFO", f"Mapped {mapped}/{len(cols)} AP columns")
    return mappings


def _stage_validate(req_id: str, records: list[dict], mappings: list[dict],
                    meta: dict) -> tuple[list[int], list[str]]:
    # Step 1: structural checks (amount, balance, distribution)
    _db_update(req_id, current_stage="VALIDATING")
    bad_idx, errs = validate_ap_invoices(records, mappings, meta)

    # Step 2: REST-based master-data checks (supplier + site) — blocks bad rows
    _db_update(req_id, current_stage="VALIDATING (supplier/site check)")
    _log(req_id, "INFO", "Validating supplier numbers and site codes against Oracle REST…")
    cfg = get_settings()
    md_bad_idx, md_errs = validate_ap_master_data(cfg, meta, records, mappings)
    bad_idx = sorted(set(bad_idx) | set(md_bad_idx))
    errs.extend(md_errs)

    # Log all errors at correct level
    for e in errs[:30]:
        level = "ERROR" if any(k in e for k in ("not found", "does not exist",
                               "Supplier", "must have", "amount must be > 0")) else "WARN"
        _log(req_id, level, e)

    _db_update(req_id, total_rows=len(records), bad_rows=len(bad_idx),
               good_rows=len(records) - len(bad_idx),
               validation_json={"errors": errs, "bad_row_indices": bad_idx})
    _log(req_id, "INFO" if not bad_idx else "WARN",
         f"Validation: {len(records)} rows, {len(bad_idx)} bad, {len(errs)} error(s)")
    return bad_idx, errs


def _stage_generate(req_id: str, records: list[dict], mappings: list[dict],
                    meta: dict, bad_indices: list[int]):
    _db_update(req_id, current_stage="GENERATING")
    out_dir = STORAGE / "fbdi" / req_id
    out_dir.mkdir(parents=True, exist_ok=True)

    # Read current version for versioned artifact keys (same pattern as GL)
    from database import _mdb as _ap_mdb
    _ver_doc = _ap_mdb()["journal_requests"].find_one({"_id": req_id}, {"version": 1})
    ver = int((_ver_doc.get("version") or 0) if _ver_doc else 0)

    meta = {**meta, "request_id": req_id, "bad_row_indices": bad_indices}
    hdrs, lines, bad = build_ap_rows(records, mappings, meta)

    hdr_csv = out_dir / "ApInvoicesInterface.csv"
    ln_csv  = out_dir / "ApInvoiceLinesInterface.csv"
    write_ap_header_csv(hdrs, hdr_csv)
    write_ap_lines_csv(lines, ln_csv)
    bad_csv: Path | None = None
    if bad:
        bad_csv = out_dir / "ap_bad_data.csv"
        write_ap_bad_csv(bad, bad_csv)

    zip_path = package_ap_zip(hdr_csv, ln_csv, out_dir)

    # Persist all to MongoDB with version tags (same pattern as GL)
    store_generated_file(req_id, f"ap_hdr_csv_v{ver}", "ApInvoicesInterface.csv", hdr_csv.read_bytes())
    store_generated_file(req_id, f"ap_line_csv_v{ver}", "ApInvoiceLinesInterface.csv", ln_csv.read_bytes())
    store_generated_file(req_id, f"fbdi_zip_v{ver}",    "ApInvoicesImport.zip", zip_path.read_bytes())
    if bad_csv:
        store_generated_file(req_id, f"bad_csv_v{ver}", "ap_bad_data.csv", bad_csv.read_bytes())

    _db_update(req_id,
               fbdi_csv_path=hdr_csv.name,
               fbdi_zip_path=zip_path.name,
               bad_data_csv_path=bad_csv.name if bad_csv else None,
               total_rows=len(records),
               good_rows=len(hdrs) + len(lines),
               bad_rows=len(bad),
               ap_header_count=len(hdrs),
               ap_line_count=len(lines))
    _log(req_id, "INFO", f"Generated FBDI v{ver}: {len(hdrs)} header(s), {len(lines)} line(s), "
                          f"{len(bad)} bad row(s)")
    return zip_path


def _stage_submit(req_id: str, zip_path: Path, meta: dict) -> str | None:
    _db_update(req_id, current_stage="SUBMITTING")
    cfg = get_settings()
    try:
        # Use resolved_import_set (set by build_ap_rows from data then config then auto)
        # so ParameterList arg9 matches what was written into the CSV's Import Set column.
        effective_group = (meta.get("resolved_import_set")
                           or meta.get("ap_invoice_group")
                           or f"BATCH_{req_id[:8]}")
        _log(req_id, "INFO", f"Import Set / Invoice Group: {effective_group}")
        resp = submit_ap_fbdi(
            cfg, str(zip_path),
            invoice_group=effective_group,
            accounting_date=meta.get("accounting_date") or
                             datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            business_unit_name=meta.get("ap_business_unit_name") or cfg.ap_business_unit_name,
            business_unit_id=meta.get("ap_business_unit_id") or cfg.ap_business_unit_id,
            ledger_id=meta.get("ap_ledger_id") or cfg.ap_ledger_id,
            source=meta.get("ap_source") or cfg.ap_source,
            pay_group=meta.get("ap_pay_group") or cfg.ap_pay_group,
        )
    except Exception as e:
        _log(req_id, "ERROR", f"submit_ap_fbdi raised: {e}")
        _db_update(req_id, status="FAILED",
                   current_stage="SUBMIT_ERROR", error_message=str(e),
                   stop_reason=f"Oracle AP submission failed: {e}")
        return None

    eid = str(resp.get("ReqstId") or "")
    _db_update(req_id, fusion_request_id=eid or "UNKNOWN")
    if eid and eid != "-1":
        _log(req_id, "INFO", f"Submitted to Oracle: ReqstId={eid}")
        return eid

    _log(req_id, "ERROR", f"Oracle returned ReqstId={eid} — rejected")
    reason = (f"Oracle rejected the AP submission (ReqstId={eid}). "
              "Check Business Unit ID, Ledger ID, Source, and Document Account "
              "in /settings (AP tab).")
    _db_update(req_id, status="FAILED", current_stage="SUBMIT_REJECTED",
               stop_reason=reason)
    return None


def _stage_monitor(req_id: str, eid: str) -> str:
    """
    Poll Oracle ESS for the parent submission. Uses adaptive cadence so the
    user sees fast feedback during the short interactive phase, then backs off
    to the configured interval for long-running imports.
    """
    if not eid or eid in ("-1", "QUEUED", ""):
        return "QUEUED"
    _db_update(req_id, current_stage="MONITORING")
    cfg = get_settings()
    base_interval = max(int(cfg.ess_poll_seconds or 5), 2)
    max_seconds   = int(cfg.ess_max_minutes or 30) * 60
    _log(req_id, "INFO", f"Polling ESS request {eid} (adaptive cadence)...")

    last = "WAIT"
    elapsed = 0
    poll_n = 0
    last_log_status = None
    while elapsed < max_seconds:
        poll_n += 1
        st = get_ess_status(cfg, eid)
        last = st
        _db_update(req_id, ess_final_status=st,
                   current_stage=f"MONITORING ({st})")
        # Only log when the status changes — keeps the timeline readable
        if st != last_log_status:
            _log(req_id, "INFO", f"ESS {eid} → {st}  (poll #{poll_n}, {elapsed}s)")
            last_log_status = st
        if st in ("SUCCEEDED", "ERROR", "WARNING", "CANCELLED", "BLOCKED"):
            return st
        # Adaptive: 3s for first 30s (10 polls), 6s next 60s, then configured interval
        if elapsed < 30:    delay = 3
        elif elapsed < 90:  delay = 6
        else:               delay = base_interval
        time.sleep(delay)
        elapsed += delay
    _log(req_id, "WARN", f"ESS poll timed out after {elapsed}s — last status: {last}")
    return last or "TIMEOUT"


# ── Main entry ────────────────────────────────────────────────────────────────

def process_ap_request(request_id: str) -> None:
    """
    Full AP Invoice Import pipeline.
    Called in a background thread by app.py for requests where
    transaction_type == "AP".
    """
    logger.info("=== AP workflow start: %s ===", request_id)
    from database import _mdb
    req_doc = _mdb()["journal_requests"].find_one({"_id": request_id})
    if not req_doc:
        logger.error("AP request not found: %s", request_id); return

    file_path = req_doc.get("file_path", "")
    if not file_path or not Path(file_path).exists():
        # Try to materialize from MongoDB
        from database import get_uploaded_file
        b = get_uploaded_file(request_id)
        if not b:
            _db_update(request_id, status="FAILED",
                       current_stage="MISSING_FILE",
                       error_message="Source file not available (disk + DB miss)")
            return
        # Recreate temp path
        tmp_dir = STORAGE / "uploads" / request_id
        tmp_dir.mkdir(parents=True, exist_ok=True)
        file_path = str(tmp_dir / (req_doc.get("file_name") or f"{request_id}.csv"))
        Path(file_path).write_bytes(b)

    # Shortcut: pre-built AP FBDI ZIP (contains ApInvoicesInterface.csv etc.)
    pairs = _maybe_split_zip(file_path)
    if pairs:
        _log(request_id, "INFO", f"Pre-built AP FBDI ZIP detected — {len(pairs)} invoice batch(es)")
        return _process_prebuilt_ap_zip(request_id, file_path, pairs)

    try:
        records, cols = _stage_parse(request_id, file_path)
    except Exception as e:
        _log(request_id, "ERROR", f"Parse failed: {e}")
        _db_update(request_id, status="FAILED",
                   current_stage="PARSE_ERROR", error_message=str(e))
        return

    mappings = _stage_map(request_id, cols)

    cfg = get_settings()
    meta = {
        "ap_business_unit_id":   req_doc.get("ap_business_unit_id")   or cfg.ap_business_unit_id,
        "ap_business_unit_name": req_doc.get("ap_business_unit_name") or cfg.ap_business_unit_name,
        "ap_ledger_id":          req_doc.get("ap_ledger_id")          or cfg.ap_ledger_id,
        "ap_source":             req_doc.get("ap_source")             or cfg.ap_source,
        "ap_pay_group":          req_doc.get("ap_pay_group")          or cfg.ap_pay_group,
        "ap_invoice_group":      req_doc.get("ap_invoice_group")      or cfg.ap_invoice_group,
        "accounting_date":       req_doc.get("accounting_date")       or "",
        "legal_entity":          req_doc.get("legal_entity")          or "",
    }

    # Add version suffix to import set so each reprocess creates a unique
    # invoice group in Oracle — prevents collisions from incomplete purges
    try:
        import re as _re_ap
        _ver_for_grp = int((req_doc.get("version") or 0))
        if _ver_for_grp > 0:
            _base_grp = (meta["ap_invoice_group"] or f"BATCH_{request_id[:8]}").rstrip()
            # Remove any stale _vN suffix before stamping with current version
            _base_grp = _re_ap.sub(r"_v\d+$", "", _base_grp)
            meta["ap_invoice_group"] = f"{_base_grp}_v{_ver_for_grp}"
    except Exception:
        pass

    _db_update(request_id,
               ap_business_unit_name=meta["ap_business_unit_name"],
               ap_invoice_group=meta["ap_invoice_group"])

    bad_idx, errs = _stage_validate(request_id, records, mappings, meta)
    # Hard-fail if ALL rows are bad
    if len(bad_idx) >= len(records) and records:
        _log(request_id, "ERROR", "All AP rows failed validation — nothing to import")
        # Still generate FBDI CSVs with bad_indices=[] so the edit wizard has
        # data to show — user can fix values and reprocess from the edit page.
        try:
            _stage_generate(request_id, records, mappings, meta, [])
        except Exception as _gen_err:
            _log(request_id, "WARN",
                 f"Could not pre-generate edit draft: {_gen_err}")
        _db_update(request_id, status="FAILED",
                   current_stage="VALIDATION_FAILED",
                   stop_reason="All rows failed validation. See process logs.")
        return

    zip_path = _stage_generate(request_id, records, mappings, meta, list(bad_idx))
    eid = _stage_submit(request_id, zip_path, meta)
    if eid is None:
        return

    final = _stage_monitor(request_id, eid)

    # Discover downstream "Import Payables Invoices" requests. Poll actively
    # up to 90s — both the Import Payables Invoices AND Report jobs must appear.
    _db_update(request_id, current_stage="FINDING_AP_JOBS")
    _log(request_id, "INFO", "Searching for downstream AP Import jobs…")
    ap_jobs: list[dict] = []
    has_report_job = False
    for attempt in range(18):      # 18 × 5s = 90s max
        ap_jobs = find_ap_import_jobs(get_settings(), eid, scan_range=40)
        has_report_job = any("Report" in (j.get("name") or "") for j in ap_jobs)
        if ap_jobs and has_report_job:
            _log(request_id, "INFO",
                 f"Found {len(ap_jobs)} AP job(s) incl. Report after {attempt*5}s")
            break
        if ap_jobs and not has_report_job:
            _log(request_id, "INFO",
                 f"Found {len(ap_jobs)} AP job(s) — waiting for Report job…")
        time.sleep(5)
    if not ap_jobs:
        _log(request_id, "WARN", "AP child jobs not found in scan range — proceeding anyway")
    for j in ap_jobs:
        _log(request_id, "INFO",
             f"AP child job: {j['name']} rid={j['request_id']} status={j['status']}")

    # Download logs from EVERY related ESS request (parent + JI + Report)
    _download_all_ap_logs(request_id, eid, ap_jobs)

    # Render BIP report PDF + analyze the data XML for actual outcome
    bip = {}
    _db_update(request_id, current_stage="RENDERING_REPORT")
    _log(request_id, "INFO", "Rendering Import Payables Invoices report (BIP)…")
    try:
        bip = _save_ap_report_pdf(request_id, ap_jobs) or {}
    except Exception as e:
        _log(request_id, "WARN", f"Report extraction failed: {e}")

    # Persist the headline counts for the UI
    if bip.get("fetched") is not None:
        _db_update(request_id,
                   ap_invoices_fetched  = bip.get("fetched", 0),
                   ap_invoices_created  = bip.get("created", 0),
                   ap_invoices_rejected = bip.get("rejected", 0),
                   ap_rejections_json   = bip.get("rejections", []))

    # ── Special case: ESS says SUCCEEDED but BIP found 0 fetched/created/rejected ──
    # This means the Import Set parameter in the ESS submission didn't match the
    # value written in the FBDI CSV, so Oracle found nothing to import.
    if bip.get("no_data") and bip.get("fetched") is not None:
        _log(request_id, "ERROR",
             "BIP report shows 0 invoices fetched, 0 created, 0 rejected — "
             "no data was imported. Import Set mismatch or empty file.")
        _db_update(request_id, status="FAILED",
                   current_stage="NO_DATA_IMPORTED",
                   stop_reason=(
                       "Oracle processed the submission but found 0 invoices to import. "
                       "The Import Set parameter submitted to ESS may not match the "
                       "Import Set written in the FBDI CSV file. Check the AP Import Set "
                       "in /settings (AP tab) and reprocess."
                   ))
        _send_ap_email(request_id, "failure", ap_jobs,
                       "0 invoices fetched — Import Set mismatch or empty file")
        return

    # Inner failure detection — ESS job status OR BIP rejection count
    inner_failed = any(j["status"] in ("ERROR","WARNING","FAILED","CANCELLED")
                       for j in ap_jobs) or bool(bip.get("has_rejections"))
    ess_succeeded = final in ("SUCCEEDED","WARNING","QUEUED")
    bip_summary = bip.get("summary", "")

    if ess_succeeded and not inner_failed:
        _db_update(request_id, status="SUCCEEDED", current_stage="COMPLETED")
        _log(request_id, "INFO", "AP import completed successfully")
        _log(request_id, "INFO", bip_summary or "AP import completed")
        _send_ap_email(request_id, "success", ap_jobs)
    else:
        # Compose a detailed failure reason from BIP summary + ESS statuses
        parts: list[str] = []
        if bip_summary and bip.get("has_rejections"):
            parts.append(bip_summary)
        if any(j["status"] in ("ERROR","WARNING","FAILED","CANCELLED") for j in ap_jobs):
            parts.append("AP child job(s): " +
                          ", ".join(f"{j['name']}={j['status']}" for j in ap_jobs))
        if not ess_succeeded:
            parts.append(f"ESS request ended with status: {final}")
        reason = " | ".join(parts) or f"Import did not succeed (final={final})"
        _db_update(request_id, status="FAILED",
                   current_stage="IMPORT_REJECTED" if bip.get("has_rejections")
                                 else ("IMPORT_ERRORS" if inner_failed else "ESS_FAILED"),
                   stop_reason=reason)
        _send_ap_email(request_id, "failure", ap_jobs, reason)

    logger.info("=== AP workflow done: %s → %s ===", request_id, final)


# ── Pre-built ZIP path ───────────────────────────────────────────────────────

def _process_prebuilt_ap_zip(request_id: str, file_path: str,
                              pairs: list[tuple[bytes, bytes]]) -> None:
    """If user uploaded a pre-built AP FBDI ZIP, submit each invoice batch."""
    from database import _mdb as _pb_mdb
    import re as _re_pb
    cfg = get_settings()

    # Read current version for versioned artifact keys and unique import set
    _pb_vdoc = _pb_mdb()["journal_requests"].find_one({"_id": request_id}, {"version": 1})
    _pb_ver = int((_pb_vdoc.get("version") or 0) if _pb_vdoc else 0)

    # Build version-unique invoice group
    _pb_base_grp = (cfg.ap_invoice_group or f"BATCH_{request_id[:8]}").rstrip()
    _pb_base_grp = _re_pb.sub(r"_v\d+$", "", _pb_base_grp)
    _pb_invoice_group = (f"{_pb_base_grp}_v{_pb_ver}" if _pb_ver > 0 else _pb_base_grp)

    _db_update(request_id, status="PROCESSING", current_stage="SUBMITTING",
               ap_business_unit_name=cfg.ap_business_unit_name,
               ap_invoice_group=_pb_invoice_group,
               ap_source=cfg.ap_source)
    _log(request_id, "INFO",
         f"Pre-built AP FBDI: {len(pairs)} batch(es), import group={_pb_invoice_group}")

    # If multiple pairs, submit them sequentially with the same overall request.
    eids: list[str] = []
    for i, (hdr_b, line_b) in enumerate(pairs):
        out_dir = STORAGE / "fbdi" / f"{request_id}-pair-{i}"
        out_dir.mkdir(parents=True, exist_ok=True)
        hdr_csv = out_dir / "ApInvoicesInterface.csv"
        ln_csv  = out_dir / "ApInvoiceLinesInterface.csv"
        hdr_csv.write_bytes(hdr_b); ln_csv.write_bytes(line_b)
        zp = package_ap_zip(hdr_csv, ln_csv, out_dir)
        # Use version-based keys; pair-suffix for multi-batch to keep keys unique
        pair_sfx = f"_p{i}" if i > 0 else ""
        store_generated_file(request_id, f"fbdi_zip_v{_pb_ver}{pair_sfx}",
                              "ApInvoicesImport.zip", zp.read_bytes())
        store_generated_file(request_id, f"ap_hdr_csv_v{_pb_ver}{pair_sfx}",
                              "ApInvoicesInterface.csv", hdr_b)
        store_generated_file(request_id, f"ap_line_csv_v{_pb_ver}{pair_sfx}",
                              "ApInvoiceLinesInterface.csv", line_b)
        pair_grp = f"{_pb_invoice_group}_p{i}" if i > 0 else _pb_invoice_group
        try:
            resp = submit_ap_fbdi(
                cfg, str(zp),
                invoice_group=pair_grp,
                accounting_date=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                business_unit_name=cfg.ap_business_unit_name,
            )
        except Exception as e:
            _log(request_id, "ERROR", f"Pair {i} submit failed: {e}")
            continue
        eid = str(resp.get("ReqstId") or "")
        if eid and eid != "-1":
            eids.append(eid)
            _log(request_id, "INFO", f"Pair {i}: ReqstId={eid}")
        else:
            _log(request_id, "ERROR", f"Pair {i}: rejected (ReqstId={eid})")

    if not eids:
        _db_update(request_id, status="FAILED",
                   current_stage="SUBMIT_REJECTED",
                   stop_reason="All pre-built ZIPs were rejected.")
        _send_ap_email(request_id, "failure", [], "All pre-built ZIPs rejected by Oracle")
        return

    # Use the FIRST batch as the primary fusion_request_id; record extras
    _db_update(request_id, fusion_request_id=eids[0],
               ap_extra_request_ids=eids[1:],
               fbdi_zip_path="ApInvoicesImport.zip")

    final = _stage_monitor(request_id, eids[0])

    # Active poll for downstream AP jobs (instead of fixed 30s sleep)
    _db_update(request_id, current_stage="FINDING_AP_JOBS")
    _log(request_id, "INFO", "Searching for downstream AP Import jobs…")
    all_ap_jobs: list[dict] = []
    for attempt in range(12):
        all_ap_jobs = []
        for eid in eids:
            all_ap_jobs.extend(find_ap_import_jobs(get_settings(), eid, scan_range=30))
        if all_ap_jobs:
            _log(request_id, "INFO",
                 f"Found {len(all_ap_jobs)} AP job(s) after {attempt*5}s")
            break
        time.sleep(5)
    for j in all_ap_jobs:
        _log(request_id, "INFO",
             f"AP child job: {j['name']} rid={j['request_id']} status={j['status']}")

    # Download logs from EVERY related ESS request
    _download_all_ap_logs(request_id, eids[0], all_ap_jobs)

    # Render Import Payables Invoices Report (real Oracle BIP PDF) + analyze
    bip = {}
    _db_update(request_id, current_stage="RENDERING_REPORT")
    _log(request_id, "INFO", "Rendering Import Payables Invoices report (BIP)…")
    try:
        bip = _save_ap_report_pdf(request_id, all_ap_jobs) or {}
    except Exception as e:
        _log(request_id, "WARN", f"Report rendering failed: {e}")

    if bip.get("fetched") is not None:
        _db_update(request_id,
                   ap_invoices_fetched  = bip.get("fetched", 0),
                   ap_invoices_created  = bip.get("created", 0),
                   ap_invoices_rejected = bip.get("rejected", 0),
                   ap_rejections_json   = bip.get("rejections", []))

    inner_failed = any(j["status"] in ("ERROR","WARNING","FAILED","CANCELLED")
                       for j in all_ap_jobs) or bool(bip.get("has_rejections"))
    bip_summary = bip.get("summary", "")
    ess_succeeded = final in ("SUCCEEDED", "WARNING")

    if ess_succeeded and not inner_failed:
        _db_update(request_id, status="SUCCEEDED", current_stage="COMPLETED")
        _log(request_id, "INFO", bip_summary or "Pre-built AP import completed")
        _send_ap_email(request_id, "success", all_ap_jobs)
    else:
        parts: list[str] = []
        if bip_summary and bip.get("has_rejections"):
            parts.append(bip_summary)
        if any(j["status"] in ("ERROR","WARNING","FAILED","CANCELLED") for j in all_ap_jobs):
            parts.append("AP child job(s): " +
                          ", ".join(f"{j['name']}={j['status']}" for j in all_ap_jobs))
        if not ess_succeeded:
            parts.append(f"ESS request ended with status: {final}")
        reason = " | ".join(parts) or f"Import did not succeed (final={final})"
        _db_update(request_id, status="FAILED",
                   current_stage="IMPORT_REJECTED" if bip.get("has_rejections")
                                 else ("IMPORT_ERRORS" if inner_failed else "ESS_FAILED"),
                   stop_reason=reason)
        _send_ap_email(request_id, "failure", all_ap_jobs, reason)


# ── Save AP Report PDF (Import Payables Invoices Report output) ─────────────

def _download_all_ap_logs(request_id: str, parent_eid: str,
                            ap_jobs: list[dict]) -> None:
    """
    Fetch ESS logs for every related request in PARALLEL — the parent
    submission, plus every "Import Payables Invoices" / "Report" job spawned
    downstream. Combines them into a single ess_logs_<id>.zip with one
    subdirectory per job.
    """
    import io as _io
    from concurrent.futures import ThreadPoolExecutor, as_completed
    cfg = get_settings()

    # Build the list of request IDs (de-dup'd) — parent + every ap_job id
    ids: list[str] = [str(parent_eid)]
    for j in ap_jobs:
        rid = str(j.get("request_id") or "")
        if rid and rid not in ids: ids.append(rid)

    _db_update(request_id, current_stage="FETCHING_LOGS")
    _log(request_id, "INFO",
         f"Fetching logs for {len(ids)} ESS request(s) in parallel: {ids}")

    combined: dict[str, bytes] = {}
    def _fetch(rid):
        try:
            return rid, download_ess_logs(cfg, rid)
        except Exception as e:
            logger.warning("Log fetch failed for %s: %s", rid, e)
            return rid, None

    # Up to 6 in flight — Oracle handles this comfortably
    with ThreadPoolExecutor(max_workers=min(6, len(ids))) as pool:
        for fut in as_completed([pool.submit(_fetch, rid) for rid in ids]):
            rid, logs = fut.result()
            if not logs: continue
            zb = logs.get("zip_bytes")
            if not zb: continue
            try:
                with _zipfile.ZipFile(_io.BytesIO(zb)) as zf:
                    for name in zf.namelist():
                        key = f"{rid}/{name}" if not name.startswith(f"{rid}/") else name
                        if key not in combined:
                            combined[key] = zf.read(name)
                _log(request_id, "INFO",
                     f"  ↳ {rid}: {len(zb)} bytes ({len(zf.namelist())} files)")
            except Exception as e:
                logger.warning("Bad log zip from %s: %s", rid, e)

    if not combined:
        _log(request_id, "WARN", "No ESS logs retrievable from Oracle")
        return

    out_buf = _io.BytesIO()
    with _zipfile.ZipFile(out_buf, "w", _zipfile.ZIP_DEFLATED) as zf:
        for name, data in combined.items():
            zf.writestr(name, data)
    zip_bytes = out_buf.getvalue()

    # Read current version so each reprocess stores under a unique key
    from database import _mdb as _log_mdb, store_log_file as _store_log_file
    import re as _re_log
    _log_vdoc = _log_mdb()["journal_requests"].find_one({"_id": request_id}, {"version": 1})
    v_num = int((_log_vdoc.get("version") or 0) if _log_vdoc else 0)
    v_tag = f"v{v_num}"

    log_dir = STORAGE / "logs" / request_id
    log_dir.mkdir(parents=True, exist_ok=True)
    log_zip = log_dir / f"ess_logs_{parent_eid}_{v_tag}.zip"
    log_zip.write_bytes(zip_bytes)
    store_generated_file(request_id, f"ess_log_{v_tag}", log_zip.name, zip_bytes)
    _db_update(request_id, ess_log_path=log_zip.name)

    # Store individual log files for display in the Log Files card (like GL does)
    short_id = request_id[:8]
    for name, data in combined.items():
        try:
            parts = name.split("/", 1)
            eid_part = parts[0]
            file_part = parts[1] if len(parts) > 1 else name
            leaf = file_part.rsplit("/", 1)[-1]
            if leaf.endswith((".log", ".out", ".xml", ".txt")):
                safe_leaf = _re_log.sub(r"[^A-Za-z0-9._-]", "_", leaf)
                new_name = f"{short_id}_{v_tag}_ap_{eid_part}_{safe_leaf}"
                _store_log_file(request_id, new_name, data)
        except Exception:
            pass

    _log(request_id, "INFO",
         f"Combined {len(zip_bytes)} bytes of logs ({v_tag}) across {len(ids)} job(s)")


def _save_ap_report_pdf(request_id: str, ap_jobs: list[dict]) -> dict:
    """
    Persist the AP Import Report.

    Strategy (in order):
      1. Find the "Import Payables Invoices" job's request ID — that's the
         P_REQUEST_ID parameter the seeded BIP report expects
      2. Call BIP runReport SOAP with the seeded report path to get the REAL
         Oracle-rendered PDF (10KB+ on success)
      3. As a fallback, save the BIP data XML from the Report job for audit

    Resulting MongoDB artifacts:
      - ap_report_pdf  : real Oracle PDF (from BIP runReport)
      - ap_report_xml  : BIP data XML (from ESS Report job output)
    """
    from services.fusion_service import (BIP_REPORT_PATHS, render_bip_report_pdf)

    # Find the Import Payables Invoices job — that's the one whose ID is the
    # P_REQUEST_ID parameter for the BIP report
    ji_req_id = ""
    report_job_id = ""
    for j in ap_jobs:
        name = (j.get("name") or "").strip()
        if name == "Import Payables Invoices":
            ji_req_id = j.get("request_id") or ji_req_id
        elif "Report" in name:
            report_job_id = j.get("request_id") or report_job_id

    # 1. Fetch the real Oracle PDF via BIP SOAP
    if ji_req_id:
        cfg = get_settings()
        report_path = (cfg.ap_bip_report_path
                        or BIP_REPORT_PATHS.get("APXIIMPT")
                        or "/Financials/Payables/Invoices/ImportPayablesInvoices.xdo")
        param_name  = cfg.ap_bip_report_param or "P_REQUEST_ID"
        pdf = render_bip_report_pdf(cfg, report_path, ji_req_id, parameter_name=param_name)
        if pdf and pdf.startswith(b"%PDF"):
            store_generated_file(request_id, "ap_report_pdf",
                                  f"ap_import_report_{ji_req_id}.pdf", pdf)
            _log(request_id, "INFO",
                 f"Saved REAL Oracle PDF from BIP runReport ({len(pdf)} bytes, P_REQUEST_ID={ji_req_id})")
        else:
            _log(request_id, "WARN",
                 f"BIP runReport returned no PDF for P_REQUEST_ID={ji_req_id} — "
                 "user may lack BI Publisher access")
    else:
        _log(request_id, "WARN",
             "Could not find 'Import Payables Invoices' job ID; skipping BIP PDF render")

    # 2. Save the BIP data XML for audit AND analyze it to detect rejections.
    # Strategy: first try the Report job directly; if not found, scan ALL
    # downloaded log files in the combined ZIP — ensures we catch rejections
    # even when ap_jobs discovery was incomplete.
    bip_analysis: dict = {}

    def _extract_bip_xml_from_zip(zb: bytes) -> bytes:
        """Return first APXIIMPT BIP data XML found in a log ZIP, or b''."""
        try:
            import io as _io
            with _zipfile.ZipFile(_io.BytesIO(zb)) as zf:
                for name in sorted(zf.namelist()):  # sort for consistency
                    if name.lower().endswith(".xml"):
                        data = zf.read(name)
                        if b"<APXIIMPT" in data[:2000]:
                            return data
        except Exception:
            pass
        return b""

    # Try Report job first
    if report_job_id:
        try:
            rpt_logs = download_ess_logs(get_settings(), report_job_id)
            rpt_zb   = rpt_logs.get("zip_bytes")
            xml_data = _extract_bip_xml_from_zip(rpt_zb) if rpt_zb else b""
            if xml_data:
                store_generated_file(request_id, "ap_report_xml",
                                      f"ap_report_data_{report_job_id}.xml", xml_data)
                bip_analysis = analyze_ap_bip_xml(xml_data)
        except Exception as e:
            logger.warning("BIP XML extraction failed (report job %s): %s", report_job_id, e)

    # Fallback: scan the combined log ZIP stored in MongoDB for any APXIIMPT XML
    # Find the highest-versioned ess_log_vN key so reprocesses use the right log
    if not bip_analysis:
        try:
            import re as _re_bip
            from database import _mdb as _bip_mdb, get_generated_file as _get_gf
            _bip_doc = _bip_mdb()["generated_files"].find_one({"_id": request_id},
                                                               {"files": 1})
            _bip_files = (_bip_doc or {}).get("files", {})
            _best_log_n, _best_log_key = -1, ""
            for _k in _bip_files:
                _m = _re_bip.match(r"^ess_log_v(\d+)$", _k)
                if _m:
                    _n = int(_m.group(1))
                    if _n > _best_log_n:
                        _best_log_n, _best_log_key = _n, _k
            if _best_log_key:
                _log_result = _get_gf(request_id, _best_log_key)
                if _log_result:
                    fallback_zb, _ = _log_result
                    xml_data = _extract_bip_xml_from_zip(fallback_zb)
                    if xml_data:
                        store_generated_file(request_id, "ap_report_xml",
                                              "ap_report_data_fallback.xml", xml_data)
                        bip_analysis = analyze_ap_bip_xml(xml_data)
                        _log(request_id, "INFO",
                             f"BIP XML found via fallback scan of {_best_log_key}")
        except Exception as e:
            logger.warning("BIP XML fallback scan failed: %s", e)

    # Log the BIP analysis results for visibility in HF logs
    if bip_analysis:
        _log(request_id, "INFO",
             f"BIP XML: fetched={bip_analysis.get('fetched')}, "
             f"created={bip_analysis.get('created')}, "
             f"rejected={bip_analysis.get('rejected')}")
        for inv in bip_analysis.get("rejections", [])[:10]:
            reasons = "; ".join(inv.get("reasons") or []) or "—"
            _log(request_id, "ERROR",
                 f"REJECTED  {inv.get('invoice_num','?')} "
                 f"(Supplier {inv.get('supplier','?')} #{inv.get('supplier_num','?')}, "
                 f"{inv.get('currency','?')} {inv.get('amount','?')}) — {reasons}")

    return bip_analysis


def _render_ap_report_from_bip_xml(xml_bytes: bytes, request_id: str,
                                     report_rid: str) -> tuple[str, bytes]:
    """Parse Oracle's BI Publisher data XML for APXIIMPT and render readable HTML+PDF."""
    try:
        from xml.etree import ElementTree as ET
        root = ET.fromstring(xml_bytes)
    except Exception:
        return "", b""

    def t(tag):
        el = root.find(f".//{tag}")
        return (el.text or "").strip() if el is not None and el.text else ""

    fetched  = t("G_INVOICES_FETCHED") or t("C_INVOICES_FETCHED") or "0"
    created  = t("G_INVOICES_CREATED") or t("C_INVOICES_CREATED") or "0"
    rejected = t("C_INVOICES_REJECTED") or "0"
    total    = t("C_TOTAL_INVOICE_AMOUNT") or ""
    err_flag = t("C_ERROR_FLAG") or "N"
    err_msg  = t("C_ERROR_MESSAGE") or ""
    company  = t("C_COMPANY_NAME_HEADER") or ""
    bu_name  = t("BUSINESS_UNIT_NAME") or ""
    source   = t("C_SOURCE") or ""
    group_id = t("P_GROUP_ID") or ""
    acct_d   = t("P_ACCOUNTING_DATE") or ""

    # Collect any rejection / audit detail elements
    rejections: list[dict] = []
    for r in root.findall(".//LIST_G_BUSINESS_UNIT_REJECTION/G_BUSINESS_UNIT_REJECTION"):
        rejections.append({c.tag: (c.text or "") for c in r})
    for r in root.findall(".//LIST_G_BU_REJECTION/G_BU_REJECTION"):
        rejections.append({c.tag: (c.text or "") for c in r})

    # ── HTML ──────────────────────────────────────────────────────────────
    html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>AP Import Report — {request_id[:8]}</title>
<style>
  body {{ font-family:'Segoe UI',sans-serif; max-width:880px; margin:24px auto; color:#222 }}
  h1 {{ color:#c74634; margin:0 0 4px }}
  .sub {{ color:#6b7280; font-size:13px; margin-bottom:24px }}
  table {{ width:100%; border-collapse:collapse; margin-bottom:18px; font-size:13.5px }}
  th, td {{ padding:8px 12px; border:1px solid #ddd; text-align:left }}
  th {{ background:#f5f5f5; font-weight:600 }}
  .ok {{ color:#1e7e34 }} .bad {{ color:#c0392b }}
  .stat {{ display:inline-block; padding:10px 18px; margin-right:10px; border-radius:8px;
           background:#f0f2f5; font-size:14px }}
  .stat strong {{ font-size:20px; display:block; color:#222 }}
</style></head><body>
<h1>📑 Import Payables Invoices Report</h1>
<div class="sub">Generated from Oracle BI Publisher data model — Request {report_rid}</div>

<div style="margin-bottom:20px">
  <span class="stat">Invoices Fetched <strong>{fetched}</strong></span>
  <span class="stat" style="background:{'#e6f4ea' if int(created or 0) > 0 else '#f0f2f5'}">
    Invoices Created <strong class="{'ok' if int(created or 0) > 0 else ''}">{created}</strong>
  </span>
  <span class="stat" style="background:{'#fce8e6' if int(rejected or 0) > 0 else '#f0f2f5'}">
    Invoices Rejected <strong class="{'bad' if int(rejected or 0) > 0 else ''}">{rejected}</strong>
  </span>
</div>

<table>
  <tr><th>Ledger</th><td>{company}</td>
      <th>Business Unit</th><td>{bu_name}</td></tr>
  <tr><th>Source</th><td>{source}</td>
      <th>Invoice Group</th><td>{group_id}</td></tr>
  <tr><th>Accounting Date</th><td>{acct_d}</td>
      <th>Total Invoice Amount</th><td>{total or '—'}</td></tr>
  <tr><th>Error Flag</th><td class="{'bad' if err_flag == 'Y' else 'ok'}">{err_flag}</td>
      <th>Error Message</th><td>{err_msg}</td></tr>
</table>
"""
    if rejections:
        html += "<h3>Rejections</h3><table><tr>"
        keys = list(rejections[0].keys())
        for k in keys: html += f"<th>{k}</th>"
        html += "</tr>"
        for r in rejections:
            html += "<tr>" + "".join(f"<td>{r.get(k,'')}</td>" for k in keys) + "</tr>"
        html += "</table>"
    html += "</body></html>"

    # ── PDF (reportlab) ───────────────────────────────────────────────────
    pdf_bytes = b""
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.lib import colors
        from reportlab.platypus import (SimpleDocTemplate, Paragraph, Table,
                                          TableStyle, Spacer)
        from reportlab.lib.styles import getSampleStyleSheet
        import io as _io
        buf = _io.BytesIO()
        doc = SimpleDocTemplate(buf, pagesize=letter, title="AP Import Report")
        ss = getSampleStyleSheet()
        story = [
            Paragraph("<b>Import Payables Invoices Report</b>", ss["Title"]),
            Paragraph(f"Request {report_rid} &nbsp;·&nbsp; {company}", ss["Normal"]),
            Spacer(1, 14),
            Table([["Invoices Fetched", "Invoices Created", "Invoices Rejected"],
                   [fetched, created, rejected]],
                  colWidths=[160, 160, 160],
                  style=TableStyle([
                      ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#f5f5f5")),
                      ("GRID", (0,0), (-1,-1), 0.5, colors.grey),
                      ("FONTSIZE", (0,0), (-1,-1), 11),
                      ("ALIGN", (0,0), (-1,-1), "CENTER"),
                      ("FONTNAME", (0,1), (-1,1), "Helvetica-Bold"),
                      ("TEXTCOLOR", (1,1), (1,1), colors.HexColor("#1e8e3e") if int(created or 0) > 0 else colors.black),
                      ("TEXTCOLOR", (2,1), (2,1), colors.HexColor("#c0392b") if int(rejected or 0) > 0 else colors.black),
                  ])),
            Spacer(1, 18),
            Table([
                ["Ledger", company, "Business Unit", bu_name],
                ["Source", source, "Invoice Group", group_id],
                ["Accounting Date", acct_d, "Total Invoice Amount", total or "—"],
                ["Error Flag", err_flag, "Error Message", err_msg or "—"],
            ],
                  colWidths=[110, 170, 110, 170],
                  style=TableStyle([
                      ("GRID", (0,0), (-1,-1), 0.5, colors.grey),
                      ("FONTSIZE", (0,0), (-1,-1), 9),
                      ("BACKGROUND", (0,0), (0,-1), colors.HexColor("#f5f5f5")),
                      ("BACKGROUND", (2,0), (2,-1), colors.HexColor("#f5f5f5")),
                  ])),
        ]
        if rejections:
            story += [Spacer(1, 16), Paragraph("<b>Rejections</b>", ss["Heading3"])]
            keys = list(rejections[0].keys())
            rows = [keys] + [[r.get(k, "") for k in keys] for r in rejections]
            story.append(Table(rows, style=TableStyle([
                ("GRID", (0,0), (-1,-1), 0.4, colors.grey),
                ("FONTSIZE", (0,0), (-1,-1), 8),
                ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#f5f5f5")),
            ])))
        doc.build(story)
        pdf_bytes = buf.getvalue()
    except Exception as e:
        logger.warning("PDF render failed: %s", e)

    return html, pdf_bytes


# ── Email ────────────────────────────────────────────────────────────────────

def _send_ap_email(request_id: str, kind: str, ap_jobs: list[dict],
                    reason: str = "") -> None:
    """Send AP success/failure email."""
    from database import _mdb
    req = _mdb()["journal_requests"].find_one({"_id": request_id})
    if not req: return
    cfg = get_settings()
    fusion_rid = req.get("fusion_request_id") or ""
    sched_url = scheduled_processes_url(cfg, fusion_rid) if fusion_rid else ""

    rows_html = "".join(
        f'<tr><td style="padding:6px;border:1px solid #ddd">{j["name"]}</td>'
        f'<td style="padding:6px;border:1px solid #ddd"><code>{j["request_id"]}</code></td>'
        f'<td style="padding:6px;border:1px solid #ddd;'
        f'color:{"#1e8e3e" if j["status"]=="SUCCEEDED" else "#d93025" if j["status"] in ("ERROR","FAILED") else "#f9ab00"};'
        f'font-weight:bold">{j["status"]}</td></tr>'
        for j in ap_jobs
    )
    headers = req.get("ap_header_count", req.get("good_rows", 0))
    lines   = req.get("ap_line_count",   req.get("good_rows", 0))

    if kind == "success":
        title = "✅ AP Invoice Import Succeeded"
        bg = "#1e8e3e"
        body = f"""
<p>{headers} invoice header(s), {lines} line(s) imported.</p>
<table style="width:100%;border-collapse:collapse">
<tr><td style="padding:8px;background:#e6f4ea;font-weight:bold;border:1px solid #e0e0e0">ESS Request ID</td><td style="padding:8px;border:1px solid #e0e0e0">{fusion_rid}</td></tr>
<tr><td style="padding:8px;font-weight:bold;border:1px solid #e0e0e0">Business Unit</td><td style="padding:8px;border:1px solid #e0e0e0">{cfg.ap_business_unit_name}</td></tr>
<tr><td style="padding:8px;background:#e6f4ea;font-weight:bold;border:1px solid #e0e0e0">File</td><td style="padding:8px;border:1px solid #e0e0e0">{req.get('file_name','')}</td></tr>
</table>"""
    else:
        title = "❌ AP Invoice Import Failed"
        bg = "#d93025"
        body = f"""
<div style="background:#fce8e6;border:1px solid #f28b82;border-radius:4px;padding:14px;margin-bottom:14px">
<strong>Reason:</strong><br/>{reason or req.get('stop_reason','')}</div>
<table style="width:100%;border-collapse:collapse">
<tr><td style="padding:8px;background:#fef0ef;font-weight:bold;border:1px solid #e0e0e0">ESS Request ID</td><td style="padding:8px;border:1px solid #e0e0e0">{fusion_rid}</td></tr>
<tr><td style="padding:8px;font-weight:bold;border:1px solid #e0e0e0">Stage</td><td style="padding:8px;border:1px solid #e0e0e0">{req.get('current_stage','')}</td></tr>
</table>"""

    html = f"""
<html><body style="font-family:'Segoe UI',sans-serif;max-width:680px;margin:auto">
<div style="background:{bg};color:white;padding:22px 28px;border-radius:8px 8px 0 0">
<h2 style="margin:0">{title}</h2></div>
<div style="border:1px solid #ddd;padding:22px;border-radius:0 0 8px 8px">
{body}
{"<h3 style='font-size:14px;margin-top:18px'>Oracle ESS Sub-Jobs</h3><table style='width:100%;border-collapse:collapse;font-size:13px'><tr style='background:#f5f5f5'><th style='padding:6px;border:1px solid #ddd;text-align:left'>Job</th><th style='padding:6px;border:1px solid #ddd;text-align:left'>Request</th><th style='padding:6px;border:1px solid #ddd;text-align:left'>Status</th></tr>" + rows_html + "</table>" if rows_html else ""}
{f'<p style="margin-top:14px"><a href="{sched_url}">View full report in Oracle Fusion →</a></p>' if sched_url else ""}
</div></body></html>"""

    try:
        from services.gmail_service import send_email
        # Attach the zip and report if present
        atts = []
        log_zip = STORAGE / "logs" / request_id / f"ess_logs_{fusion_rid}.zip"
        if log_zip.exists(): atts.append(str(log_zip))
        if has_generated_file(request_id, "fbdi_zip_v0"):
            data, fname = get_generated_file(request_id, "fbdi_zip_v0") or (None, "")
            if data: atts.append((fname, data))
        if has_generated_file(request_id, "ap_report_pdf"):
            data, fname = get_generated_file(request_id, "ap_report_pdf") or (None, "")
            if data: atts.append((fname, data))
        subj_kind = "Succeeded" if kind == "success" else "Failed"
        send_email(cfg.notification_email,
                   f"{title} — {req.get('file_name','AP Import')}", html, atts)
    except Exception as e:
        logger.warning("AP email send failed: %s", e)
