"""
Local ML mapping engine — no API key required.
Uses sentence-transformers (all-MiniLM-L6-v2, ~22 MB, downloaded once from HuggingFace)
to map any source column name to the correct Oracle GlInterface.csv field.

The model is pre-loaded once at startup. Mapping history from SQLite is used to
boost confidence for known mappings, making the system learn over time.
"""

from __future__ import annotations

import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)

# ── Exact Oracle GlInterface.csv target fields with rich descriptions ──────────
ORACLE_FIELDS: dict[str, str] = {
    "*Status Code":                           "Import status code, always NEW for new imports",
    "*Ledger ID":                             "Numeric ledger identifier, set of books ID, ledger number",
    "*Effective Date of Transaction":         "GL accounting date, effective date, transaction date, posting date, value date, journal date",
    "*Journal Source":                        "Journal source name, source of journal, e.g. Manual, Payables, Receivables",
    "*Journal Category":                      "Journal category, journal type category, e.g. Manual, Accrual, Adjustment",
    "*Currency Code":                         "ISO currency code, e.g. USD EUR GBP INR, transaction currency",
    "*Journal Entry Creation Date":           "Date the journal entry record was created",
    "*Actual Flag":                           "Balance type flag: A=Actual, B=Budget, E=Encumbrance",
    "Segment1":                               "Chart of accounts segment 1, company code, legal entity, business unit, entity",
    "Segment2":                               "Chart of accounts segment 2, cost center, department, division, cost centre, CC",
    "Segment3":                               "Chart of accounts segment 3, GL account, natural account, account code, account number",
    "Segment4":                               "Chart of accounts segment 4, sub account, product, product line, sub-account",
    "Segment5":                               "Chart of accounts segment 5, intercompany, IC, affiliate",
    "Segment6":                               "Chart of accounts segment 6, project, project code",
    "Segment7":                               "Chart of accounts segment 7, future use",
    "Entered Debit Amount":                   "Debit amount in entered currency, DR amount, debit, amount debit, debit value",
    "Entered Credit Amount":                  "Credit amount in entered currency, CR amount, credit, amount credit, credit value",
    "Converted Debit Amount":                 "Accounted debit amount in ledger currency, functional debit, base currency debit",
    "Converted Credit Amount":               "Accounted credit amount in ledger currency, functional credit, base currency credit",
    "REFERENCE1 (Batch Name)":               "Journal batch name, batch, batch identifier, batch reference",
    "REFERENCE2 (Batch Description)":        "Journal batch description, batch description text",
    "REFERENCE4 (Journal Entry Name)":       "Journal entry name, journal entry identifier, JE name",
    "REFERENCE5 (Journal Entry Description)":"Journal entry description text, JE description",
    "REFERENCE6 (Journal Entry Reference)":  "Journal entry reference, document number, voucher number, reference number, ref no",
    "REFERENCE10 (Journal Entry Line Description)": "Journal line description, line narration, remarks, memo, line detail, transaction description",
    "Ledger Name":                            "Ledger name text, set of books name, accounting book name",
    "Period Name":                            "Accounting period name, e.g. Jan-24, Feb-25, Dec-25",
    "Currency Conversion Type":              "Exchange rate type, conversion type, e.g. Corporate, Spot, User",
    "Currency Conversion Date":             "Exchange rate date, conversion date, rate date",
    "Currency Conversion Rate":             "Exchange rate value, conversion rate, forex rate",
    "Interface Group Identifier":           "Group identifier for batching, interface group ID",
    "Statistical Amount":                   "Statistical amount for non-monetary statistical journals",
    "Average Journal Flag":                  "Average journal flag Y or N",
    "Clearing Company":                      "Clearing company code, intercompany clearing company",
    "Encumbrance Type ID":                   "Encumbrance type identifier",
    "Reconciliation Reference":              "Reconciliation reference text",
}

# ── AP Invoice Import target fields (header + line) ──────────────────────────
AP_HEADER_FIELDS: dict[str, str] = {
    "*Invoice ID":          "Client-generated unique invoice identifier for matching header to lines",
    "*Business Unit":       "Business unit name, BU, operating unit, organization",
    "*Source":              "Invoice source, e.g. External, INVOICE GATEWAY, ERS, SPREADSHEET",
    "*Invoice Number":      "Invoice number, voucher number, supplier invoice reference",
    "*Invoice Amount":      "Invoice total amount, gross amount, invoice value",
    "*Invoice Date":        "Invoice date, billing date, document date",
    "**Supplier Name":      "Supplier name, vendor name, payee name",
    "**Supplier Number":    "Supplier number, vendor number, supplier code, vendor id",
    "*Supplier Site":       "Supplier site code, vendor site, address code, payment site",
    "Invoice Currency":     "Invoice currency code, ISO currency, e.g. USD, INR",
    "Payment Currency":     "Payment currency code",
    "Description":          "Invoice description, narration, remarks, memo",
    "Import Set":           "Import set token, batch identifier for grouping invoices",
    "*Invoice Type":        "Invoice type, STANDARD, CREDIT, DEBIT, PREPAYMENT, MIXED",
    "Legal Entity":         "Legal entity name",
    "*Payment Terms":       "Payment terms name, e.g. Net 30, Immediate, 2/10 Net 30",
    "Terms Date":           "Payment terms start date",
    "Goods Received Date":  "Date goods received",
    "Invoice Received Date":"Invoice received date",
    "Accounting Date":      "Accounting date for the invoice",
    "Payment Method":       "Payment method, e.g. CHECK, EFT, WIRE",
    "Pay Group":            "Payment grouping, e.g. Standard, Employee",
    "Pay Alone":            "Pay alone flag Y or N",
    "Discountable Amount":  "Discountable amount on the invoice",
    "Conversion Rate Type": "FX conversion rate type",
    "Conversion Date":      "FX conversion date",
    "Conversion Rate":      "FX conversion rate value",
    "Liability Combination":"Liability account combination flexfield",
    "Document Category Code":"Document category, e.g. Standard Invoice",
    "Voucher Number":       "Voucher number for sequencing",
    "Calculate Tax During Import":"Calculate tax during import flag Y or N",
}

AP_LINE_FIELDS: dict[str, str] = {
    "*Invoice ID":           "Link back to invoice header — must match a header *Invoice ID",
    "Line Number":           "Line number within the invoice, 1-based",
    "*Line Type":            "Line type, ITEM, TAX, FREIGHT, MISCELLANEOUS, PREPAY, RETAINAGE RELEASE",
    "*Amount":               "Line amount, line value, line total, expense amount",
    "Invoice Quantity":      "Quantity invoiced",
    "Unit Price":            "Unit price per item",
    "UOM":                   "Unit of measure",
    "Description":           "Line description, line narration, item description",
    "PO Number":             "Purchase order number for PO match",
    "PO Line Number":        "PO line number",
    "PO Schedule Number":    "PO shipment number",
    "PO Distribution Number":"PO distribution number",
    "Distribution Combination":"Charge account code combination — explicit GL account string",
    "Distribution Set":      "Distribution set name — predefined account split rule",
    "Accounting Date":       "Line accounting date",
    "Tax Classification Code":"Tax classification code, e.g. STANDARD, EXEMPT",
    "Final Match":           "Final match flag Y or N",
    "Withholding Tax Group": "Withholding tax group name",
    "Project Number":        "Project number",
    "Task Number":           "Task number",
    "Expenditure Type":      "Project expenditure type",
    "Expenditure Organization":"Project expenditure organization",
}

AP_ALIASES: dict[str, list[str]] = {
    "*Invoice ID":      ["invoice id","invoice_id","inv id","invoiceid"],
    "*Business Unit":   ["business unit","bu","business_unit","operating unit","org","organization","ou"],
    "*Source":          ["source","invoice source","src","origin"],
    "*Invoice Number":  ["invoice number","invoice no","inv no","inv number","invoice num","voucher","voucher number","bill number","supplier invoice number"],
    "*Invoice Amount":  ["invoice amount","total","gross amount","invoice total","total amount","amount","gross","bill amount"],
    "*Invoice Date":    ["invoice date","bill date","document date","inv date","billing date"],
    "**Supplier Name":  ["supplier name","vendor name","supplier","vendor","payee","payee name"],
    "**Supplier Number":["supplier number","vendor number","supplier no","vendor no","supplier code","vendor code","supplier id","vendor id"],
    "*Supplier Site":   ["supplier site","vendor site","site","site code","supplier site code","address code","pay site","payment site"],
    "Invoice Currency": ["invoice currency","currency","ccy","curr","inv currency"],
    "Payment Currency": ["payment currency","pay currency","pay ccy"],
    "Description":      ["description","memo","remarks","narration","desc","line description","comments","line_description"],
    "Import Set":       ["import set","invoice group","import group","group","batch","invoice batch"],
    "*Invoice Type":    ["invoice type","inv type","type","document type"],
    "Legal Entity":     ["legal entity","le","entity name"],
    "*Payment Terms":   ["payment terms","payment term","terms","term","payterms","pmt terms","payment_terms","payment_term"],
    "Terms Date":       ["terms date","payment terms date","pay terms date"],
    "Accounting Date":  ["accounting date","gl date","accounting_date"],
    "Payment Method":   ["payment method","pay method","payment_method","method of payment"],
    "Pay Group":        ["pay group","payment group","paygroup"],
    "Conversion Rate":  ["conversion rate","fx rate","exchange rate","rate"],
    "Line Number":      ["line number","line no","line","line_no","line_number"],
    "*Line Type":       ["line type","line_type","type"],
    "*Amount":          ["amount","line amount","line total","expense amount","line_amount","amt","value"],
    "Invoice Quantity": ["quantity","qty","invoice quantity","quantity invoiced"],
    "Unit Price":       ["unit price","price","unit_price","rate per unit"],
    "UOM":              ["uom","unit of measure","unit","measure"],
    "PO Number":        ["po number","purchase order","po","po_number","purchase order number"],
    "Distribution Combination":["distribution combination","charge account","gl account","account combination","dist combination","dist_combination","expense account","account"],
    "Distribution Set":["distribution set","dist set","dist_set","distribution_set","accounting distribution"],
    "Tax Classification Code":["tax classification","tax classification code","tax code","tax class"],
}

_AP_HEADER_SET = set(AP_HEADER_FIELDS.keys())
_AP_LINE_SET   = set(AP_LINE_FIELDS.keys())
_AP_NOSTAR     = {f.lstrip("*").strip().lower(): f
                   for f in (_AP_HEADER_SET | _AP_LINE_SET)}

# AP DB column aliases (Oracle's AP_INVOICES_INTERFACE / AP_INVOICE_LINES_INTERFACE)
_AP_DB_COLUMN_ALIASES: dict[str, str] = {
    "invoice_id":           "*Invoice ID",
    "operating_unit":       "*Business Unit",
    "source":               "*Source",
    "invoice_num":          "*Invoice Number",
    "invoice_amount":       "*Invoice Amount",
    "invoice_date":         "*Invoice Date",
    "vendor_name":          "**Supplier Name",
    "vendor_num":           "**Supplier Number",
    "vendor_site_code":     "*Supplier Site",
    "invoice_currency_code":"Invoice Currency",
    "payment_currency_code":"Payment Currency",
    "description":          "Description",
    "invoice_type_lookup_code":"*Invoice Type",
    "terms_name":           "*Payment Terms",
    "terms_date":           "Terms Date",
    "accounting_date":      "Accounting Date",
    "payment_method_code":  "Payment Method",
    "pay_group_lookup_code":"Pay Group",
    "exchange_rate":        "Conversion Rate",
    "exchange_rate_type":   "Conversion Rate Type",
    "exchange_date":        "Conversion Date",
    "legal_entity_name":    "Legal Entity",
    # Line columns
    "line_number":          "Line Number",
    "line_type_lookup_code":"*Line Type",
    "amount":               "*Amount",
    "quantity_invoiced":    "Invoice Quantity",
    "unit_price":           "Unit Price",
    "po_number":            "PO Number",
    "po_line_number":       "PO Line Number",
    "dist_code_concatenated":"Distribution Combination",
    "distribution_set_name":"Distribution Set",
    "tax_classification_code":"Tax Classification Code",
}

# Canonical field lookup: strip leading * for user-friendly matches
_ORACLE_FIELD_SET = set(ORACLE_FIELDS.keys())
_ORACLE_FIELD_NOSTAR = {f.lstrip("*").strip().lower(): f for f in _ORACLE_FIELD_SET}

# Priority alias lookup — exact matches before embeddings
ALIASES: dict[str, list[str]] = {
    "*Ledger ID":                            ["ledger id","ledger_id","set of books id","sob id","sob_id","ledger number","ledger no","ledger identifier"],
    "Segment1":                              ["company","entity","legal entity","co","company code","legal_entity","company_code","bus unit","business unit"],
    "Segment2":                              ["cost center","cost centre","costcenter","cc","department","dept","division","cost_center","costcentre"],
    "Segment3":                              ["account","gl account","account code","natural account","coa","gl_account","acct","account number","account_code","acc"],
    # Order matches the user's US Primary Ledger COA (confirmed via JI error report 2026-05-30):
    #   Seg4=Product, Seg5=Future, Seg6=Intercompany
    "Segment4":                              ["product","sub account","subaccount","sub-account","product line","sub_account","segment4","seg4"],
    "Segment5":                              ["future","futureuse","future use","future_use","futuresegment","reserved","spare","segment5","seg5"],
    "Segment6":                              ["intercompany","ic","affiliate","inter company","intco","intercom","ic_seg","segment6","seg6"],
    "Entered Debit Amount":                  ["debit","dr","debit amount","entered debit","amount dr","amount_dr","drcr dr","debit_amount","dr amount"],
    "Entered Credit Amount":                 ["credit","cr","credit amount","entered credit","amount cr","amount_cr","drcr cr","credit_amount","cr amount"],
    "*Effective Date of Transaction":        ["date","gl date","accounting date","transaction date","posting date","value date","trx_date","eff date","effective date","acctg date","journal date","entry date"],
    "*Currency Code":                        ["currency","ccy","curr","iso currency","currency_code","currency code","txn currency"],
    "Ledger Name":                           ["ledger","book","set of books","sob","ledger name","ledger_name","accounting book"],
    "REFERENCE1 (Batch Name)":              ["journal name","batch name","batch","journal_name","batch_name","jnl name"],
    "REFERENCE2 (Batch Description)":       ["batch description","batch desc","batch_description","batch_desc","batch detail","batch_detail"],
    "REFERENCE4 (Journal Entry Name)":      ["journal entry","entry name","je name","je_name","journal entry name","journal_entry_name","journal entry id"],
    "REFERENCE5 (Journal Entry Description)": ["journal entry description","je description","je_description","journal_entry_description","entry description","journal description","journal desc"],
    "REFERENCE6 (Journal Entry Reference)": ["reference","ref","document number","voucher","doc no","reference no","ref_no","doc_no","document ref","voucher no"],
    "REFERENCE10 (Journal Entry Line Description)": ["description","narration","remarks","comments","memo","line description","line_desc","detail","particulars","transaction description","desc"],
    "*Journal Category":                    ["category","journal category","je category","je_category"],
    "*Journal Source":                      ["source","journal source","je source","je_source"],
    "Period Name":                           ["period","period name","accounting period","gl period","period_name"],
    "Currency Conversion Rate":              ["rate","conversion rate","exchange rate","fx rate","forex rate","currency rate","conv rate","conv_rate","exchange_rate","rate_value","currency conversion rate"],
    "Currency Conversion Type":              ["conversion type","exchange type","rate type","conv type","rate_type","user currency conversion type","user_currency_conversion_type"],
    "Currency Conversion Date":              ["conversion date","exchange date","rate date","conv date","rate_date","currency conversion date"],
}

# Oracle's internal GL_INTERFACE table column names — exact aliases.
# These are unambiguous (one-to-one) and override any embedding guess.
_DB_COLUMN_ALIASES: dict[str, str] = {
    "status":                       "*Status Code",
    "status_code":                  "*Status Code",
    "ledger_id":                    "*Ledger ID",
    "accounting_date":              "*Effective Date of Transaction",
    "user_je_source_name":          "*Journal Source",
    "user_je_category_name":        "*Journal Category",
    "currency_code":                "*Currency Code",
    "date_created":                 "*Journal Entry Creation Date",
    "actual_flag":                  "*Actual Flag",
    "entered_dr":                   "Entered Debit Amount",
    "entered_cr":                   "Entered Credit Amount",
    "accounted_dr":                 "Converted Debit Amount",
    "accounted_cr":                 "Converted Credit Amount",
    "batch_name":                   "REFERENCE1 (Batch Name)",
    "batch_description":            "REFERENCE2 (Batch Description)",
    "journal_entry_name":           "REFERENCE4 (Journal Entry Name)",
    "journal_entry_description":    "REFERENCE5 (Journal Entry Description)",
    "journal_entry_reference":      "REFERENCE6 (Journal Entry Reference)",
    "journal_entry_line_description": "REFERENCE10 (Journal Entry Line Description)",
    "stat_amount":                  "Statistical Amount",
    "group_id":                     "Interface Group Identifier",
    "ledger_name":                  "Ledger Name",
    "period_name":                  "Period Name",
    "average_journal_flag":         "Average Journal Flag",
    "encumbrance_type_id":          "Encumbrance Type ID",
    "user_currency_conversion_type":"Currency Conversion Type",
    "currency_conversion_date":     "Currency Conversion Date",
    "currency_conversion_rate":     "Currency Conversion Rate",
}

_model = None
_field_names: list[str] = []
_field_embeddings: np.ndarray = None


def _get_model():
    global _model, _field_names, _field_embeddings
    if _model is None:
        logger.info("Loading sentence-transformers model (first time takes ~10 seconds)...")
        from sentence_transformers import SentenceTransformer
        _model = SentenceTransformer("all-MiniLM-L6-v2")
        _field_names = list(ORACLE_FIELDS.keys())
        # Build embeddings for all Oracle fields (descriptions + aliases)
        field_texts = []
        for f in _field_names:
            aliases = " ".join(ALIASES.get(f, []))
            field_texts.append(f"{f.lower()} {ORACLE_FIELDS[f]} {aliases}")
        _field_embeddings = _model.encode(field_texts, normalize_embeddings=True)
        logger.info("ML mapper ready, %d Oracle fields indexed", len(_field_names))
    return _model


def _normalize(s: str) -> str:
    return s.lower().replace("_", " ").replace("-", " ").strip()


def _alias_match(source_col: str) -> Optional[str]:
    """Exact alias lookup — returns Oracle field name or None."""
    normalized = _normalize(source_col)
    for field, aliases in ALIASES.items():
        if normalized in aliases:
            return field
    return None


def map_column(source_col: str, history_boost: dict[str, tuple[str, float]] | None = None) -> dict:
    """
    Map a single source column name to the best Oracle GlInterface.csv field.
    Returns: {source_field, target_field, confidence, reason, method}
    """
    # 0. Exact Oracle field name match against the FULL 150-column FBDI layout
    # (handles all Segments, References, Attributes, etc. — not just the
    # subset documented in ORACLE_FIELDS).
    try:
        from utils.fbdi_generator import COLUMNS as _FBDI_COLS
        _all_fbdi_cols = set(_FBDI_COLS) - {"END"}
    except Exception:
        _all_fbdi_cols = _ORACLE_FIELD_SET
    if source_col in _all_fbdi_cols:
        return {"source_field": source_col, "target_field": source_col,
                "confidence": 1.0, "reason": "Exact Oracle FBDI column name.", "method": "exact"}
    if source_col in _ORACLE_FIELD_SET:
        return {"source_field": source_col, "target_field": source_col,
                "confidence": 1.0, "reason": "Exact Oracle GL field name.", "method": "exact"}
    # Also handle names without the leading * (e.g. "Ledger ID" → "*Ledger ID")
    nostar = source_col.lstrip("*").strip().lower()
    if nostar in _ORACLE_FIELD_NOSTAR:
        canonical = _ORACLE_FIELD_NOSTAR[nostar]
        return {"source_field": source_col, "target_field": canonical,
                "confidence": 0.99, "reason": "Matched Oracle field name (without prefix).", "method": "exact"}
    # 0b. Oracle GL_INTERFACE DB column name (e.g. ENTERED_DR, ACTUAL_FLAG)
    db_key = source_col.strip().lower()
    if db_key in _DB_COLUMN_ALIASES:
        return {"source_field": source_col, "target_field": _DB_COLUMN_ALIASES[db_key],
                "confidence": 0.99, "reason": "Matched Oracle GL_INTERFACE table column name.",
                "method": "exact"}

    # 1. History boost — known good mappings from previous imports
    if history_boost and source_col.lower() in history_boost:
        target, conf = history_boost[source_col.lower()]
        return {"source_field": source_col, "target_field": target,
                "confidence": conf, "reason": f"Learned from {int(conf*100)}% historical mapping.", "method": "history"}

    # 2. Exact alias match
    alias_hit = _alias_match(source_col)
    if alias_hit:
        return {"source_field": source_col, "target_field": alias_hit,
                "confidence": 0.95, "reason": f"Matched via field alias dictionary.", "method": "alias"}

    # 3. Embedding cosine similarity
    model = _get_model()
    col_vec = model.encode([_normalize(source_col)], normalize_embeddings=True)[0]
    sims = _field_embeddings.dot(col_vec)
    best_idx = int(np.argmax(sims))
    best_sim = float(sims[best_idx])
    best_field = _field_names[best_idx]

    return {
        "source_field": source_col,
        "target_field": best_field if best_sim >= 0.40 else None,
        "confidence": round(best_sim, 4),
        "reason": f"Semantic similarity {best_sim:.0%} → '{best_field}'." if best_sim >= 0.40 else "No confident match found.",
        "method": "embedding",
    }


# ── AP Invoice mapping ───────────────────────────────────────────────────────

def _ap_alias_match(source_col: str) -> Optional[str]:
    norm = _normalize(source_col)
    for field, aliases in AP_ALIASES.items():
        if norm in aliases:
            return field
    return None


def map_ap_column(source_col: str, target_set: str = "both") -> dict:
    """
    Map one source column to an AP Invoice FBDI field.
    target_set: "header" | "line" | "both"
    Returns same dict shape as map_column().
    """
    pool: set[str] = set()
    if target_set in ("header", "both"): pool |= _AP_HEADER_SET
    if target_set in ("line",   "both"): pool |= _AP_LINE_SET

    # Exact (full name with stars)
    if source_col in pool:
        return {"source_field": source_col, "target_field": source_col,
                "confidence": 1.0, "reason": "Exact AP FBDI column name.", "method": "exact"}

    nostar = source_col.lstrip("*").strip().lower()
    if nostar in _AP_NOSTAR and _AP_NOSTAR[nostar] in pool:
        return {"source_field": source_col, "target_field": _AP_NOSTAR[nostar],
                "confidence": 0.99, "reason": "Matched AP field name (without prefix).",
                "method": "exact"}

    db_key = source_col.strip().lower()
    if db_key in _AP_DB_COLUMN_ALIASES and _AP_DB_COLUMN_ALIASES[db_key] in pool:
        return {"source_field": source_col, "target_field": _AP_DB_COLUMN_ALIASES[db_key],
                "confidence": 0.99, "reason": "Matched AP interface table column.",
                "method": "exact"}

    hit = _ap_alias_match(source_col)
    if hit and hit in pool:
        return {"source_field": source_col, "target_field": hit,
                "confidence": 0.95, "reason": "Matched via AP alias dictionary.", "method": "alias"}

    # Embedding fallback against AP field descriptions only
    descs = {**AP_HEADER_FIELDS, **AP_LINE_FIELDS}
    fields_in_pool = [f for f in descs if f in pool]
    if not fields_in_pool:
        return {"source_field": source_col, "target_field": None,
                "confidence": 0.0, "reason": "No AP target pool.", "method": "embedding"}
    model = _get_model()
    field_texts = [f"{f.lower()} {descs[f]} {' '.join(AP_ALIASES.get(f, []))}" for f in fields_in_pool]
    embs = model.encode(field_texts, normalize_embeddings=True)
    col_vec = model.encode([_normalize(source_col)], normalize_embeddings=True)[0]
    sims = embs.dot(col_vec)
    best_idx = int(np.argmax(sims))
    best_sim = float(sims[best_idx])
    best_field = fields_in_pool[best_idx]
    return {
        "source_field": source_col,
        "target_field": best_field if best_sim >= 0.40 else None,
        "confidence": round(best_sim, 4),
        "reason": f"Semantic match {best_sim:.0%} → '{best_field}'." if best_sim >= 0.40 else "No confident AP match.",
        "method": "embedding",
    }


def map_all_ap_columns(source_columns: list[str], target_set: str = "both") -> list[dict]:
    """Map every column for an AP invoice file."""
    return [map_ap_column(c, target_set) for c in source_columns]


def map_all_columns(
    source_columns: list[str],
    discovery_hints: dict[str, str] | None = None,
    history_boost: dict[str, tuple[str, float]] | None = None,
) -> list[dict]:
    """
    Map a list of source columns.
    discovery_hints: {source_col: oracle_field_name} from the AI discovery agent.
    """
    results = []
    for col in source_columns:
        # Discovery hint takes top priority
        if discovery_hints and col in discovery_hints:
            tgt = discovery_hints[col]
            results.append({"source_field": col, "target_field": tgt,
                             "confidence": 0.97, "reason": "Identified by data structure analysis.", "method": "discovery"})
            continue
        results.append(map_column(col, history_boost))
    return results


def load_history_boost() -> dict[str, tuple[str, float]]:
    """Load top confirmed mappings from SQLite mapping history."""
    try:
        from database import SessionLocal, MappingHistory
        with SessionLocal() as db:
            rows = db.query(MappingHistory).order_by(("times_used", -1)).limit(500).all()
            return {r.source_column.lower(): (r.target_field, min(0.99, 0.80 + r.times_used * 0.01))
                    for r in rows}
    except Exception:
        return {}


def save_mapping_to_history(mappings: list[dict]) -> None:
    """Persist confirmed mappings to SQLite for future learning."""
    try:
        from database import SessionLocal, MappingHistory
        with SessionLocal() as db:
            for m in mappings:
                if not m.get("target_field"):
                    continue
                key = m["source_field"].lower()
                existing = db.query(MappingHistory).filter_by(source_column=key, target_field=m["target_field"]).first()
                if existing:
                    existing.times_used += 1
                    existing.confidence = m.get("confidence", 0.9)
                else:
                    db.add(MappingHistory(source_column=key, target_field=m["target_field"],
                                         confidence=m.get("confidence", 0.9)))
            db.commit()
    except Exception as e:
        logger.warning("Could not save mapping history: %s", e)
