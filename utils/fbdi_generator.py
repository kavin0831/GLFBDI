"""
GlInterface.csv generator — exact Oracle Fusion GL FBDI format.
Every data row ends with END as the last column value.
Files are named GlInterface.csv and GlInterface.zip.
"""

from __future__ import annotations
import csv, zipfile, logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

# Exact column order from Oracle's official GlInterface.csv template
COLUMNS = [
    "*Status Code","*Ledger ID","*Effective Date of Transaction","*Journal Source",
    "*Journal Category","*Currency Code","*Journal Entry Creation Date","*Actual Flag",
    "Segment1","Segment2","Segment3","Segment4","Segment5","Segment6","Segment7",
    "Segment8","Segment9","Segment10","Segment11","Segment12","Segment13","Segment14",
    "Segment15","Segment16","Segment17","Segment18","Segment19","Segment20","Segment21",
    "Segment22","Segment23","Segment24","Segment25","Segment26","Segment27","Segment28",
    "Segment29","Segment30",
    "Entered Debit Amount","Entered Credit Amount",
    "Converted Debit Amount","Converted Credit Amount",
    "REFERENCE1 (Batch Name)","REFERENCE2 (Batch Description)","REFERENCE3",
    "REFERENCE4 (Journal Entry Name)","REFERENCE5 (Journal Entry Description)",
    "REFERENCE6 (Journal Entry Reference)","REFERENCE7 (Journal Entry Reversal flag)",
    "REFERENCE8 (Journal Entry Reversal Period)","REFERENCE9 (Journal Reversal Method)",
    "REFERENCE10 (Journal Entry Line Description)",
    "Reference column 1","Reference column 2","Reference column 3","Reference column 4",
    "Reference column 5","Reference column 6","Reference column 7","Reference column 8",
    "Reference column 9","Reference column 10",
    "Statistical Amount","Currency Conversion Type","Currency Conversion Date",
    "Currency Conversion Rate","Interface Group Identifier",
    "Context field for Journal Entry Line DFF",
    "ATTRIBUTE1 Value for Journal Entry Line DFF","ATTRIBUTE2 Value for Journal Entry Line DFF",
    "Attribute3 Value for Journal Entry Line DFF","Attribute4 Value for Journal Entry Line DFF",
    "Attribute5 Value for Journal Entry Line DFF","Attribute6 Value for Journal Entry Line DFF",
    "Attribute7 Value for Journal Entry Line DFF","Attribute8 Value for Journal Entry Line DFF",
    "Attribute9 Value for Journal Entry Line DFF","Attribute10 Value for Journal Entry Line DFF",
    "Attribute11 Value for Captured Information DFF","Attribute12 Value for Captured Information DFF",
    "Attribute13 Value for Captured Information DFF","Attribute14 Value for Captured Information DFF",
    "Attribute15 Value for Captured Information DFF","Attribute16 Value for Captured Information DFF",
    "Attribute17 Value for Captured Information DFF","Attribute18 Value for Captured Information DFF",
    "Attribute19 Value for Captured Information DFF","Attribute20 Value for Captured Information DFF",
    "Context field for Captured Information DFF",
    "Average Journal Flag","Clearing Company","Ledger Name","Encumbrance Type ID",
    "Reconciliation Reference","Period Name",
    "REFERENCE 18","REFERENCE 19","REFERENCE 20",
    "Attribute Date 1","Attribute Date 2","Attribute Date 3","Attribute Date 4","Attribute Date 5",
    "Attribute Date 6","Attribute Date 7","Attribute Date 8","Attribute Date 9","Attribute Date 10",
    "Attribute Number 1","Attribute Number 2","Attribute Number 3","Attribute Number 4",
    "Attribute Number 5","Attribute Number 6","Attribute Number 7","Attribute Number 8",
    "Attribute Number 9","Attribute Number 10",
    "Global Attribute Category",
    "Global Attribute 1","Global Attribute 2","Global Attribute 3","Global Attribute 4",
    "Global Attribute 5","Global Attribute 6","Global Attribute 7","Global Attribute 8",
    "Global Attribute 9","Global Attribute 10","Global Attribute 11","Global Attribute 12",
    "Global Attribute 13","Global Attribute 14","Global Attribute 15","Global Attribute 16",
    "Global Attribute 17","Global Attribute 18","Global Attribute 19","Global Attribute 20",
    "Global Attribute Date 1","Global Attribute Date 2","Global Attribute Date 3",
    "Global Attribute Date 4","Global Attribute Date 5",
    "Global Attribute Number 1","Global Attribute Number 2","Global Attribute Number 3",
    "Global Attribute Number 4","Global Attribute Number 5",
    "END",
]

DATA_COLS = COLUMNS[:-1]  # all except END


def _fmt_date(s: str) -> str:
    """Any date string → YYYY/MM/DD (the format SQL*Loader expects for GL_INTERFACE)."""
    if not s: return ""
    for fmt in ("%Y-%m-%d","%Y/%m/%d","%m/%d/%Y","%d/%m/%Y","%d-%m-%Y","%Y%m%d","%d-%b-%Y","%m-%d-%Y"):
        try: return datetime.strptime(str(s).strip(),fmt).strftime("%Y/%m/%d")
        except ValueError: pass
    return str(s)


def _period(s: str) -> str:
    """Any date string → Oracle period name e.g. May-26."""
    if not s: return ""
    for fmt in ("%Y-%m-%d","%m/%d/%Y","%Y/%m/%d","%d-%m-%Y"):
        try: return datetime.strptime(str(s).strip(),fmt).strftime("%b-%y")
        except ValueError: pass
    return s


def verify_csv(path: Path) -> tuple[bool, list[str]]:
    """
    Verify GlInterface.csv output:
    - Every row must have exactly len(COLUMNS) values
    - Last value must be 'END'
    - Required fields (*) must be non-empty
      Exception: '*Ledger ID' is optional when 'Ledger Name' is populated
                 (Oracle resolves Ledger ID from Ledger Name at import time)
    Returns (ok, error_list).
    """
    errors: list[str] = []
    ledger_id_idx   = DATA_COLS.index("*Ledger ID") if "*Ledger ID" in DATA_COLS else -1
    ledger_name_idx = DATA_COLS.index("Ledger Name") if "Ledger Name" in DATA_COLS else -1
    # Required indices: all * fields except *Ledger ID (soft-required)
    required_indices = [i for i, c in enumerate(DATA_COLS)
                        if c.startswith("*") and i != ledger_id_idx]
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row_num, row in enumerate(reader, 1):
                if len(row) != len(COLUMNS):
                    errors.append(f"Row {row_num}: {len(row)} cols (expected {len(COLUMNS)})")
                    continue
                if row[-1] != "END":
                    errors.append(f"Row {row_num}: last value '{row[-1]}' is not 'END'")
                    continue
                for ri in required_indices:
                    if not row[ri].strip():
                        errors.append(f"Row {row_num}: required field '{DATA_COLS[ri]}' is empty")
                        break
                # Soft-check: *Ledger ID empty is OK only when Ledger Name is set
                if (ledger_id_idx >= 0 and not row[ledger_id_idx].strip()
                        and ledger_name_idx >= 0 and not row[ledger_name_idx].strip()):
                    errors.append(f"Row {row_num}: both *Ledger ID and Ledger Name are empty")
    except Exception as e:
        return False, [f"verify_csv failed: {e}"]
    return len(errors) == 0, errors


def build_rows(records: list[dict], mappings: list[dict], meta: dict) -> tuple[list[dict], list[dict]]:
    """
    Transform source records into GlInterface rows using field mappings.
    Returns (good_rows, bad_rows).
    bad_rows are source records that have no debit/credit after mapping.
    """
    # Build source → target lookup from mappings
    src_to_tgt: dict[str,str] = {m["source_field"]: m["target_field"]
                                  for m in mappings if m.get("target_field")}
    # Identify bad row indices (from validation)
    bad_indices: set[int] = set(meta.get("bad_row_indices",[]))

    acct_date  = _fmt_date(meta.get("accounting_date",""))
    period     = _period(meta.get("accounting_date","")) or meta.get("period_name","")
    currency   = (meta.get("currency_code") or "USD").upper()
    ledger     = meta.get("ledger_name","")
    ledger_id  = str(meta.get("ledger_id","")).strip()  # numeric ledger ID if known
    jnl_name   = meta.get("journal_name","") or "GL_IMPORT"
    category   = meta.get("journal_category","Manual")
    source     = meta.get("journal_source","Manual")
    # Must be numeric — Oracle matches this to ParameterList arg4
    _rid = meta.get("request_id", "") or ""
    group_id = str(abs(hash(_rid)) % 999999999) if _rid else "1"
    now       = datetime.now(timezone.utc).strftime("%Y/%m/%d")

    # Fields that must be reformatted to YYYY/MM/DD if the source has them
    DATE_FIELDS = {
        "*Effective Date of Transaction",
        "*Journal Entry Creation Date",
        "Currency Conversion Date",
    }

    good, bad = [], []

    for i, row in enumerate(records):
        gl: dict = {c:"" for c in DATA_COLS}
        # Mandatory constants
        gl["*Status Code"]                    = "NEW"
        gl["*Actual Flag"]                    = "A"
        gl["*Effective Date of Transaction"]  = acct_date
        gl["*Currency Code"]                  = currency
        gl["*Journal Category"]               = category
        gl["*Journal Source"]                 = source
        # Creation date defaults to the accounting date (matches Oracle FBDI samples)
        gl["*Journal Entry Creation Date"]    = acct_date or now
        if ledger_id:
            gl["*Ledger ID"]                  = ledger_id
        gl["Ledger Name"]                     = ledger
        gl["Period Name"]                     = period
        gl["REFERENCE1 (Batch Name)"]         = jnl_name
        gl["REFERENCE4 (Journal Entry Name)"] = jnl_name
        gl["Currency Conversion Type"]        = "Corporate"
        gl["Currency Conversion Date"]        = acct_date
        # Conversion rate: USD = 1.00 (ledger currency). For foreign currency we
        # prefer meta["currency_conversion_rate"] (set by the workflow via Oracle
        # Daily Rates REST + hardcoded fallback). If absent, mapping below may fill
        # it from the source data; otherwise leave blank so Oracle JI flags it.
        if currency == "USD":
            gl["Currency Conversion Rate"]    = "1.00"
        elif meta.get("currency_conversion_rate"):
            gl["Currency Conversion Rate"]    = str(meta["currency_conversion_rate"])
        # else: rate will be filled by the source→target mapping below if provided
        gl["Interface Group Identifier"]      = group_id

        # Map source → target. When two source columns map to the same target
        # (ambiguous ML), keep the FIRST non-empty value rather than overwriting
        # — prevents e.g. EXPENDITURE_TYPE="000" clobbering ACTUAL_FLAG="A".
        # Also: NEVER let a user-supplied Interface Group Identifier override
        # the value we generated — we own that field for JI correlation.
        for src, tgt in src_to_tgt.items():
            if tgt == "Interface Group Identifier":
                continue
            val = row.get(src,"")
            if not val or str(val).lower() in ("nan","none","null",""):
                continue
            v = str(val).strip()
            if tgt in DATE_FIELDS:
                v = _fmt_date(v)
            existing = gl.get(tgt, "")
            # Allow overwriting our own constant defaults but never a value that
            # came from an earlier source column mapped to the same target.
            if existing and existing not in ("NEW", "A", "Manual",
                                              "Corporate", "1.00", "USD",
                                              acct_date, now, ledger,
                                              period, jnl_name):
                continue
            gl[tgt] = v

        dr = gl.get("Entered Debit Amount","").strip()
        cr = gl.get("Entered Credit Amount","").strip()

        if i in bad_indices or (not dr and not cr):
            bad.append({**row, "_reason": "No debit or credit amount after mapping"})
        else:
            good.append(gl)

    return good, bad


def write_csv(rows: list[dict], path: Path) -> None:
    """
    Write headerless Oracle GlInterface.csv.
    Column order is fixed (positional). Every row ends with END.
    """
    with open(path,"w",newline="",encoding="utf-8") as f:
        w = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        for gl in rows:
            row_vals = [gl.get(c,"") for c in DATA_COLS]
            row_vals.append("END")
            w.writerow(row_vals)
    logger.info("GlInterface.csv written: %d rows → %s", len(rows), path)

    # Post-write verification
    ok, errs = verify_csv(path)
    if not ok:
        logger.warning("GlInterface.csv verification warnings (%d): %s", len(errs), "; ".join(errs[:3]))
    else:
        logger.info("GlInterface.csv verified OK — every row ends with END")


def write_bad_csv(bad_rows: list[dict], path: Path) -> None:
    if not bad_rows: return
    all_keys = list(bad_rows[0].keys())
    with open(path,"w",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
        w.writeheader()
        w.writerows(bad_rows)
    logger.info("bad_data.csv written: %d rows → %s", len(bad_rows), path)


def package_zip(csv_path: Path, meta: dict, doc_account: str) -> Path:
    """
    Build GlInterface.zip containing only GlInterface.csv (no properties file).
    Oracle's JournalImportLauncher job reads column-positional CSV — no manifest needed.
    """
    zip_path = csv_path.parent / "GlInterface.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(csv_path, arcname="GlInterface.csv")
    logger.info("GlInterface.zip created: %.1f KB → %s", zip_path.stat().st_size/1024, zip_path)
    return zip_path
