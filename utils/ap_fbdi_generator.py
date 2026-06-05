"""
AP Invoice FBDI generator — exact Oracle Fusion AP Invoice Import format.

Produces two HEADERLESS positional CSVs packaged into a single ZIP:
  - ApInvoicesInterface.csv       (header records, ends with END)
  - ApInvoiceLinesInterface.csv   (line records, ends with END)

The column order below matches Oracle's AP Invoice Import FBDI template
(based on the sample at apinvoiceimporttest/ + SQL*Loader control file evidence
captured from request 9737419: INVOICE_ID FIRST, OPERATING_UNIT NEXT,
SOURCE NEXT, INVOICE_NUM NEXT, INVOICE_AMOUNT NEXT, INVOICE_DATE NEXT,
VENDOR_NAME NEXT, VENDOR_NUM NEXT, VENDOR_SITE_CODE NEXT, …).
"""

from __future__ import annotations

import csv
import logging
import zipfile
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


# ── Header CSV column order (ApInvoicesInterface.csv) ────────────────────────
# Matches the user's sample headers verbatim (from apinvoiceimporttest).
AP_HEADER_COLUMNS = [
    "*Invoice ID", "*Business Unit", "*Source", "*Invoice Number",
    "*Invoice Amount", "*Invoice Date",
    "**Supplier Name", "**Supplier Number", "*Supplier Site",
    "Invoice Currency", "Payment Currency", "Description", "Import Set",
    "*Invoice Type", "Legal Entity",
    "Customer Tax Registration Number", "Customer Registration Code",
    "First-Party Tax Registration Number", "Supplier Tax Registration Number",
    "*Payment Terms", "Terms Date", "Goods Received Date",
    "Invoice Received Date", "Accounting Date",
    "Payment Method", "Pay Group", "Pay Alone", "Discountable Amount",
    "Prepayment Number", "Prepayment Line Number",
    "Prepayment Application Amount", "Prepayment Accounting Date",
    "Invoice Includes Prepayment",
    "Conversion Rate Type", "Conversion Date", "Conversion Rate",
    "Liability Combination", "Document Category Code", "Voucher Number",
    "Requester First Name", "Requester Last Name", "Requester Employee Number",
    "Delivery Channel Code", "Bank Charge Bearer",
    "Remit-to Supplier", "Remit-to Supplier Number", "Remit-to Address Name",
    "Payment Priority", "Settlement Priority",
    "Unique Remittance Identifier", "Unique Remittance Identifier Check Digit",
    "Payment Reason Code", "Payment Reason Comments",
    "Remittance Message 1", "Remittance Message 2", "Remittance Message 3",
    "Withholding Tax Group", "Ship-to Location", "Taxation Country",
    "Document Sub Type",
    "Tax Invoice Internal Sequence Number", "Supplier Tax Invoice Number",
    "Tax Invoice Recording Date", "Supplier Tax Invoice Date",
    "Supplier Tax Invoice Conversion Rate",
    "Port Of Entry Code", "Correction Year", "Correction Period",
    "Import Document Number", "Import Document Date",
    "Tax Control Amount",
    "Calculate Tax During Import", "Add Tax To Invoice Amount",
    "Attribute Category",
    *[f"Attribute {i}" for i in range(1, 16)],
    *[f"Attribute Number {i}" for i in range(1, 6)],
    *[f"Attribute Date {i}" for i in range(1, 6)],
    "Global Attribute Category",
    *[f"Global Attribute {i}" for i in range(1, 21)],
    *[f"Global Attribute Number {i}" for i in range(1, 6)],
    *[f"Global Attribute Date {i}" for i in range(1, 6)],
    "URL Attachments",
    "END",
]
HDR_DATA_COLS = AP_HEADER_COLUMNS[:-1]   # all except the END sentinel


# ── Line CSV column order (ApInvoiceLinesInterface.csv) ──────────────────────
AP_LINE_COLUMNS = [
    "*Invoice ID", "Line Number", "*Line Type", "*Amount",
    "Invoice Quantity", "Unit Price", "UOM", "Description",
    "PO Number", "PO Line Number", "PO Schedule Number", "PO Distribution Number",
    "Item Description", "PO Release Number", "Purchasing Category",
    "Receipt Number", "Receipt Line Number",
    "Consumption Advice Number", "Consumption Advice Line Number",
    "Packing Slip", "Final Match",
    "Distribution Combination", "Distribution Set",
    "Accounting Date",
    "Overlay Account Segment", "Overlay Primary Balancing Segment",
    "Overlay Cost Center Segment",
    "Tax Classification Code", "Ship-to Location", "Ship-from Location",
    "Location of Final Discharge",
    "Transaction Business Category", "Product Fiscal Classification",
    "Intended Use", "User-Defined Fiscal Classification", "Product Type",
    "Assessable Value", "Product Category", "Tax Control Amount",
    "Tax Regime Code", "Tax", "Tax Status Code", "Tax Jurisdiction Code",
    "Tax Rate Code", "Tax Rate",
    "Withholding Tax Group", "Income Tax Type", "Income Tax Region",
    "Prorate Across All Item Lines", "Line Group Number",
    "Cost Factor Name", "Statistical Quantity",
    "Track as Asset", "Asset Book Type Code", "Asset Category ID",
    "Serial Number", "Manufacturer", "Model Number", "Warranty Number",
    "Price Correction Line", "Price Correction Invoice Number",
    "Price Correction Invoice Line Number",
    "Requester First Name", "Requester Last Name", "Requester Employee Number",
    "Attribute Category",
    *[f"Attribute {i}" for i in range(1, 16)],
    *[f"Attribute Number {i}" for i in range(1, 6)],
    *[f"Attribute Date {i}" for i in range(1, 6)],
    "Global Attribute Category",
    *[f"Global Attribute {i}" for i in range(1, 21)],
    *[f"Global Attribute Number {i}" for i in range(1, 6)],
    *[f"Global Attribute Date {i}" for i in range(1, 6)],
    "Project ID", "Task ID", "Expenditure Type ID",
    "Expenditure Item Date", "Expenditure Organization ID",
    "Project Number", "Task Number",
    "Expenditure Type", "Expenditure Organization",
    "Funding Source Id",
    *[f"PJC Reserved Attribute {i}" for i in range(2, 11)],
    *[f"PJC User Defined Attribute {i}" for i in range(1, 11)],
    "Fiscal Charge Type",
    "Multiperiod Accounting Start Date",
    "Multiperiod Accounting End Date",
    "Multiperiod Accounting Accrual Account ",
    "Project Name", "Task Name",
    "END",
]
LINE_DATA_COLS = AP_LINE_COLUMNS[:-1]


# Column positions that contain DATE values — must be reformatted to YYYY/MM/DD
# (Oracle SQL*Loader uses TO_DATE(:col, 'YYYY/MM/DD'))
_HDR_DATE_FIELDS = {
    "*Invoice Date", "Terms Date", "Goods Received Date",
    "Invoice Received Date", "Accounting Date",
    "Prepayment Accounting Date", "Conversion Date",
    "Tax Invoice Recording Date", "Supplier Tax Invoice Date",
    "Import Document Date",
}
_LINE_DATE_FIELDS = {
    "Accounting Date", "Expenditure Item Date",
    "Multiperiod Accounting Start Date", "Multiperiod Accounting End Date",
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def _fmt_date(s: str) -> str:
    """Any date string → YYYY/MM/DD (Oracle SQL*Loader format)."""
    if not s: return ""
    s = str(s).strip()
    if not s or s.lower() in ("nan", "none", "null"): return ""
    for fmt in ("%Y/%m/%d", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y",
                "%d-%m-%Y", "%m-%d-%Y", "%Y%m%d", "%d-%b-%Y", "%d-%b-%y"):
        try: return datetime.strptime(s, fmt).strftime("%Y/%m/%d")
        except ValueError: pass
    return s


def _clean_id(s: str, fallback: str) -> str:
    """Strip Excel scientific notation (e.g. 2.39239E+11) to a clean integer."""
    s = (s or "").strip()
    if not s: return fallback
    if "e" in s.lower():
        try: return str(int(float(s)))
        except (ValueError, OverflowError): pass
    return s


def _to_amount(s: str) -> str:
    """Strip thousand separators, keep two decimals as string."""
    if s is None: return ""
    s = str(s).strip().replace(",", "")
    if not s or s.lower() in ("nan", "none", "null"): return ""
    try:
        return f"{float(s):.2f}"
    except (ValueError, TypeError):
        return s


def _row_dict(record: dict, mappings: list[dict],
              date_targets: set[str]) -> dict:
    """Apply ML mapping then build a target→value dict for one source record."""
    out: dict[str, str] = {}
    src_to_tgt = {m["source_field"]: m["target_field"]
                   for m in mappings if m.get("target_field")}
    for src, tgt in src_to_tgt.items():
        v = record.get(src, "")
        if v in (None, "") or str(v).lower() in ("nan", "none", "null"):
            continue
        v = str(v).strip()
        if tgt in date_targets:
            v = _fmt_date(v)
        # Allow first non-empty value to win (don't overwrite with later mapping)
        if not out.get(tgt):
            out[tgt] = v
    return out


# ── Build rows ───────────────────────────────────────────────────────────────

def _gen_invoice_id_base(request_id: str) -> int:
    """Deterministic 9-digit Invoice ID base derived from request_id."""
    # Use SHA-1 — Python's hash() is salted per-process, would mean the same
    # request_id maps to different Invoice IDs after a server restart, breaking
    # header↔line linking in re-submitted edits.
    import hashlib as _h
    n = int(_h.sha1((request_id or "").encode("utf-8")).hexdigest()[:9], 16)
    return (n % 900000000) + 100000000


def build_ap_rows(records: list[dict], mappings: list[dict],
                  meta: dict) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Transform source records into header + line dicts for AP FBDI.

    Records can include EITHER:
      - a "row_type" column with values "H"/"L" or "Header"/"Line"  (explicit), OR
      - a presence/absence of an Invoice Number column to imply Header rows
        and a Line Number column to imply Line rows.

    If a single record contains both invoice-level AND line-level data (typical
    of flat-file uploads where each row is a line), the function emits ONE header
    per unique Invoice Number + one line per row.

    Returns (header_rows, line_rows, bad_rows).
      header_rows / line_rows are dicts keyed by Oracle FBDI column names.
      bad_rows are source records that failed validation.
    """
    base       = _gen_invoice_id_base(str(meta.get("request_id", "")))
    bu_name    = meta.get("ap_business_unit_name") or meta.get("business_unit", "")
    source     = meta.get("ap_source") or "External"
    pay_group  = meta.get("ap_pay_group") or "1000"
    import_set = meta.get("ap_invoice_group") or ""
    legal_ent  = meta.get("legal_entity") or ""
    bad_indices = set(meta.get("bad_row_indices", []))

    # Detect mode: flat (one source row = one line, headers grouped by Invoice #)
    # vs explicit (row_type column distinguishes header from line)
    has_row_type = any("row_type" in r or "Row Type" in r for r in records)

    # ── Mode A: flat — each row is a line; group by Invoice Number ───────────
    if not has_row_type:
        return _build_flat(records, mappings, meta, base, bu_name, source,
                            pay_group, import_set, legal_ent, bad_indices)

    # ── Mode B: explicit row_type ─────────────────────────────────────────────
    return _build_with_row_type(records, mappings, meta, base, bu_name, source,
                                 pay_group, import_set, legal_ent, bad_indices)


def _build_flat(records, mappings, meta, base, bu_name, source, pay_group,
                import_set, legal_ent, bad_indices):
    """Each source row = one invoice line; group by Invoice Number for headers."""
    # First map every source row to canonical fields
    mapped = [_row_dict(r, mappings, _HDR_DATE_FIELDS | _LINE_DATE_FIELDS)
              for r in records]

    headers: list[dict] = []
    lines:   list[dict] = []
    bad:     list[dict] = []
    invnum_to_id: dict[str, str] = {}
    invnum_to_line_no: dict[str, int] = {}

    for i, (src, m) in enumerate(zip(records, mapped)):
        # Pull the invoice number — required to group lines under a header
        inv_num = m.get("*Invoice Number") or m.get("Invoice Number") or ""
        amount  = _to_amount(m.get("*Amount") or m.get("Amount") or "")
        if i in bad_indices or not inv_num or not amount:
            bad.append({**src, "_reason": "Missing invoice number or line amount"})
            continue

        # Header — create once per unique Invoice Number
        if inv_num not in invnum_to_id:
            inv_id = str(base + len(headers))
            invnum_to_id[inv_num] = inv_id
            invnum_to_line_no[inv_num] = 0

            hdr = {c: "" for c in HDR_DATA_COLS}
            hdr["*Invoice ID"]      = inv_id
            hdr["*Business Unit"]   = bu_name
            hdr["*Source"]          = source
            hdr["*Invoice Number"]  = inv_num
            hdr["*Invoice Amount"]  = _to_amount(m.get("*Invoice Amount") or m.get("Invoice Amount") or "")
            hdr["*Invoice Date"]    = _fmt_date(m.get("*Invoice Date") or m.get("Invoice Date") or "")
            hdr["**Supplier Name"]   = m.get("**Supplier Name") or m.get("Supplier Name") or ""
            hdr["**Supplier Number"] = m.get("**Supplier Number") or m.get("Supplier Number") or ""
            hdr["*Supplier Site"]   = m.get("*Supplier Site") or m.get("Supplier Site") or ""
            hdr["Invoice Currency"] = m.get("Invoice Currency") or m.get("Currency") or "USD"
            hdr["Payment Currency"] = m.get("Payment Currency") or hdr["Invoice Currency"]
            hdr["Description"]      = m.get("Description") or ""
            hdr["Import Set"]       = m.get("Import Set") or import_set
            hdr["*Invoice Type"]    = m.get("*Invoice Type") or m.get("Invoice Type") or "STANDARD"
            hdr["Legal Entity"]     = m.get("Legal Entity") or legal_ent
            hdr["*Payment Terms"]   = m.get("*Payment Terms") or m.get("Payment Terms") or "Immediate"
            hdr["Terms Date"]       = _fmt_date(m.get("Terms Date") or hdr["*Invoice Date"])
            hdr["Accounting Date"]  = _fmt_date(m.get("Accounting Date") or hdr["*Invoice Date"])
            hdr["Payment Method"]   = m.get("Payment Method") or "CHECK"
            hdr["Pay Group"]        = m.get("Pay Group") or "Standard"
            hdr["Conversion Rate"]  = "1"
            hdr["Calculate Tax During Import"] = "N"
            headers.append(hdr)

        # Line — increment line number for this invoice
        invnum_to_line_no[inv_num] += 1
        line = {c: "" for c in LINE_DATA_COLS}
        line["*Invoice ID"]    = invnum_to_id[inv_num]
        line["Line Number"]    = str(invnum_to_line_no[inv_num])
        line["*Line Type"]     = m.get("*Line Type") or m.get("Line Type") or "ITEM"
        line["*Amount"]        = amount
        line["Description"]    = m.get("Description") or m.get("Line Description") or ""
        line["Final Match"]    = "N"
        line["Distribution Combination"] = m.get("Distribution Combination") or ""
        line["Distribution Set"]         = m.get("Distribution Set") or ""
        line["Accounting Date"] = _fmt_date(m.get("Accounting Date") or "")
        line["Prorate Across All Item Lines"] = "N"
        lines.append(line)

    return headers, lines, bad


def _build_with_row_type(records, mappings, meta, base, bu_name, source,
                          pay_group, import_set, legal_ent, bad_indices):
    """Explicit row_type='H' / row_type='L' rows. Lines link to nearest preceding H."""
    headers: list[dict] = []
    lines:   list[dict] = []
    bad:     list[dict] = []
    cur_inv_id: str | None = None
    cur_line_no = 0

    for i, src in enumerate(records):
        if i in bad_indices:
            bad.append({**src, "_reason": "Failed pre-validation"})
            continue
        rt = (src.get("row_type") or src.get("Row Type") or "").upper().strip()
        m  = _row_dict(src, mappings, _HDR_DATE_FIELDS | _LINE_DATE_FIELDS)

        if rt in ("H", "HEADER"):
            inv_id = str(base + len(headers))
            cur_inv_id = inv_id
            cur_line_no = 0
            hdr = {c: "" for c in HDR_DATA_COLS}
            hdr["*Invoice ID"]      = inv_id
            hdr["*Business Unit"]   = bu_name
            hdr["*Source"]          = source
            hdr["*Invoice Number"]  = m.get("*Invoice Number") or ""
            hdr["*Invoice Amount"]  = _to_amount(m.get("*Invoice Amount") or "")
            hdr["*Invoice Date"]    = _fmt_date(m.get("*Invoice Date") or "")
            hdr["**Supplier Name"]   = m.get("**Supplier Name") or ""
            hdr["**Supplier Number"] = m.get("**Supplier Number") or ""
            hdr["*Supplier Site"]   = m.get("*Supplier Site") or ""
            hdr["Invoice Currency"] = m.get("Invoice Currency") or "USD"
            hdr["Payment Currency"] = m.get("Payment Currency") or hdr["Invoice Currency"]
            hdr["Description"]      = m.get("Description") or ""
            hdr["Import Set"]       = m.get("Import Set") or import_set
            hdr["*Invoice Type"]    = m.get("*Invoice Type") or "STANDARD"
            hdr["Legal Entity"]     = m.get("Legal Entity") or legal_ent
            hdr["*Payment Terms"]   = m.get("*Payment Terms") or "Immediate"
            hdr["Terms Date"]       = _fmt_date(m.get("Terms Date") or hdr["*Invoice Date"])
            hdr["Accounting Date"]  = _fmt_date(m.get("Accounting Date") or hdr["*Invoice Date"])
            hdr["Payment Method"]   = m.get("Payment Method") or "CHECK"
            hdr["Pay Group"]        = m.get("Pay Group") or "Standard"
            hdr["Conversion Rate"]  = "1"
            hdr["Calculate Tax During Import"] = "N"
            headers.append(hdr)
        elif rt in ("L", "LINE"):
            if not cur_inv_id:
                bad.append({**src, "_reason": "Line row before any header"})
                continue
            cur_line_no += 1
            line = {c: "" for c in LINE_DATA_COLS}
            line["*Invoice ID"]    = cur_inv_id
            line["Line Number"]    = str(cur_line_no)
            line["*Line Type"]     = m.get("*Line Type") or "ITEM"
            line["*Amount"]        = _to_amount(m.get("*Amount") or "")
            line["Description"]    = m.get("Description") or ""
            line["Final Match"]    = "N"
            line["Distribution Combination"] = m.get("Distribution Combination") or ""
            line["Distribution Set"]         = m.get("Distribution Set") or ""
            line["Accounting Date"] = _fmt_date(m.get("Accounting Date") or "")
            line["Prorate Across All Item Lines"] = "N"
            lines.append(line)
        else:
            bad.append({**src, "_reason": f"Unknown row_type '{rt}'"})

    return headers, lines, bad


# ── Write CSVs ───────────────────────────────────────────────────────────────

def write_ap_header_csv(rows: list[dict], path: Path) -> None:
    """Headerless ApInvoicesInterface.csv with END sentinel per row."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        for h in rows:
            w.writerow([h.get(c, "") for c in HDR_DATA_COLS] + ["END"])
    logger.info("ApInvoicesInterface.csv written: %d invoice header(s) → %s",
                len(rows), path)


def write_ap_lines_csv(rows: list[dict], path: Path) -> None:
    """Headerless ApInvoiceLinesInterface.csv with END sentinel per row."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        for l in rows:
            w.writerow([l.get(c, "") for c in LINE_DATA_COLS] + ["END"])
    logger.info("ApInvoiceLinesInterface.csv written: %d line(s) → %s",
                len(rows), path)


def write_ap_bad_csv(bad_rows: list[dict], path: Path) -> None:
    if not bad_rows: return
    keys = list(bad_rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        w.writeheader()
        w.writerows(bad_rows)
    logger.info("ap_bad_data.csv written: %d row(s) → %s", len(bad_rows), path)


def package_ap_zip(hdr_csv: Path, line_csv: Path, out_dir: Path) -> Path:
    """Package header + line CSVs into a single ZIP for importBulkData submission."""
    zip_path = out_dir / "ApInvoicesImport.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(hdr_csv,  arcname="ApInvoicesInterface.csv")
        zf.write(line_csv, arcname="ApInvoiceLinesInterface.csv")
    logger.info("ApInvoicesImport.zip created: %.1f KB → %s",
                zip_path.stat().st_size / 1024, zip_path)
    return zip_path


# ── Split multi-invoice uploaded ZIPs ────────────────────────────────────────

def split_multi_invoice_zip(zip_bytes: bytes) -> list[tuple[bytes, bytes]]:
    """
    If an uploaded ZIP contains multiple ApInvoicesInterface.csv +
    ApInvoiceLinesInterface.csv pairs (e.g., from a batched upload), pair them
    up by their relative directory or by ordinal index, and return a list of
    (header_csv_bytes, line_csv_bytes) tuples — one per invoice batch.

    Returns [(hdr_bytes, line_bytes), …]. If no AP CSVs found, returns [].
    """
    import io as _io
    pairs: list[tuple[bytes, bytes]] = []
    with zipfile.ZipFile(_io.BytesIO(zip_bytes)) as zf:
        names = [n for n in zf.namelist() if not n.endswith("/")]

        # Group by parent dir
        groups: dict[str, dict[str, bytes]] = {}
        for n in names:
            parent = "/".join(n.split("/")[:-1])
            fname = n.split("/")[-1].lower()
            if "apinvoicesinterface" in fname:
                groups.setdefault(parent, {})["hdr"] = zf.read(n)
            elif "apinvoicelinesinterface" in fname:
                groups.setdefault(parent, {})["line"] = zf.read(n)

        # Build pair list, preserving directory order
        for parent in sorted(groups.keys()):
            g = groups[parent]
            if "hdr" in g and "line" in g:
                pairs.append((g["hdr"], g["line"]))
    return pairs
