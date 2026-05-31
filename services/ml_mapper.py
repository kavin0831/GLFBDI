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
    "Currency Conversion Rate":              ["rate","conversion rate","exchange rate","fx rate","forex rate","currency rate","conv rate","conv_rate","exchange_rate","rate_value"],
    "Currency Conversion Type":              ["conversion type","exchange type","rate type","conv type","rate_type"],
    "Currency Conversion Date":              ["conversion date","exchange date","rate date","conv date","rate_date"],
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
    # 0. Exact Oracle field name match (user uploaded file with Oracle column names)
    if source_col in _ORACLE_FIELD_SET:
        return {"source_field": source_col, "target_field": source_col,
                "confidence": 1.0, "reason": "Exact Oracle GL field name.", "method": "exact"}
    # Also handle names without the leading * (e.g. "Ledger ID" → "*Ledger ID")
    nostar = source_col.lstrip("*").strip().lower()
    if nostar in _ORACLE_FIELD_NOSTAR:
        canonical = _ORACLE_FIELD_NOSTAR[nostar]
        return {"source_field": source_col, "target_field": canonical,
                "confidence": 0.99, "reason": "Matched Oracle field name (without prefix).", "method": "exact"}

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
