"""Universal file parser — converts any supported format into a list of dicts."""

from __future__ import annotations
import csv as _csv_mod
import json, zipfile, logging
from pathlib import Path
import chardet, pandas as pd, xmltodict

logger = logging.getLogger(__name__)

SUPPORTED = {".xlsx",".xls",".csv",".txt",".json",".xml",".zip",".pdf"}

# First 8 required Oracle FBDI column names (used for header detection)
_FBDI_HEADER_SIGNALS = {
    "*status code", "*ledger id", "*effective date of transaction",
    "*journal source", "*currency code", "*actual flag",
    "segment1", "entered debit amount", "entered credit amount",
}


def _encoding(path: str) -> str:
    with open(path,"rb") as f: raw=f.read(50000)
    return chardet.detect(raw).get("encoding") or "utf-8"


def detect_file_format(file_path: str) -> str:
    """
    Returns one of:
    - 'fbdi_headerless' : no header row, first cell is 'NEW', positional Oracle format
    - 'fbdi_headers'    : CSV already has Oracle FBDI column names as headers
    - 'raw_data'        : regular user data file, needs ML mapping
    """
    ext = Path(file_path).suffix.lower()
    if ext not in (".csv", ".txt"):
        return "raw_data"
    try:
        enc = _encoding(file_path)
        for sep in (",", "\t", ";", "|"):
            try:
                peek = pd.read_csv(file_path, sep=sep, encoding=enc, nrows=3,
                                   dtype=str, header=None, on_bad_lines="skip",
                                   engine="python")
                if peek.shape[1] >= 5:
                    break
            except Exception:
                continue

        first_row_vals = [str(v).strip().lower() for v in peek.iloc[0].tolist()]
        # Headerless: first value is 'new' (Oracle status) and last non-empty is 'end'
        last_val = next((v for v in reversed(first_row_vals) if v), "")
        if first_row_vals[0] == "new" and last_val == "end":
            return "fbdi_headerless"

        # Has Oracle headers: read the header row and check for Oracle field names
        with_header = pd.read_csv(file_path, sep=sep, encoding=enc, nrows=0,
                                  dtype=str, on_bad_lines="skip", engine="python")
        header_lower = {c.strip().lower() for c in with_header.columns}
        overlap = header_lower & _FBDI_HEADER_SIGNALS
        if len(overlap) >= 3:
            return "fbdi_headers"
    except Exception:
        pass
    return "raw_data"


def parse_to_records(file_path: str) -> tuple[list[dict], list[str]]:
    """Returns (records, column_names). Raises ValueError for unsupported types."""
    ext = Path(file_path).suffix.lower()
    if ext in (".xlsx",".xls"):   df = _excel(file_path)
    elif ext in (".csv",".txt"):  df = _smart_csv(file_path)
    elif ext == ".json":          df = _json(file_path)
    elif ext == ".xml":           df = _xml(file_path)
    elif ext == ".pdf":           df = _pdf(file_path)
    elif ext == ".zip":           df = _zip(file_path)
    else: raise ValueError(f"Unsupported: {ext}")
    df = df.dropna(how="all").reset_index(drop=True)
    df = df.astype(str).replace("nan","").replace("<NA>","")
    return df.to_dict(orient="records"), list(df.columns)


def _smart_csv(path: str) -> pd.DataFrame:
    """
    CSV/TXT reader that handles three scenarios:
    1. Headerless Oracle FBDI format (first value = NEW, last = END)
    2. CSV with Oracle FBDI column headers
    3. Regular data CSV
    """
    fmt = detect_file_format(path)

    if fmt == "fbdi_headerless":
        from utils.fbdi_generator import COLUMNS
        enc = _encoding(path)
        for sep in (",", "\t", ";", "|"):
            try:
                df = pd.read_csv(path, sep=sep, encoding=enc, dtype=str,
                                 header=None, on_bad_lines="skip", engine="python")
                if df.shape[1] >= 10:
                    n = min(df.shape[1], len(COLUMNS))
                    df = df.iloc[:, :n]
                    df.columns = COLUMNS[:n]
                    # Drop the END column if present
                    if "END" in df.columns:
                        df = df.drop(columns=["END"])
                    logger.info("Parsed headerless FBDI CSV: %d rows, %d cols", len(df), len(df.columns))
                    return df
            except Exception:
                continue

    return _csv(path)


def _csv(path):
    enc = _encoding(path)
    for sep in (",",";","\t","|"):
        try:
            df=pd.read_csv(path,sep=sep,encoding=enc,dtype=str,engine="python",on_bad_lines="skip")
            if len(df.columns)>1: return df
        except Exception: pass
    return pd.read_csv(path,dtype=str,on_bad_lines="skip")


def _excel(path):
    xl = pd.ExcelFile(path)
    best = pd.DataFrame()
    for s in xl.sheet_names:
        df = pd.read_excel(path,sheet_name=s,dtype=str)
        if len(df)>len(best): best=df
    return best

def _csv(path):
    enc = _encoding(path)
    for sep in (",",";","\t","|"):
        try:
            df=pd.read_csv(path,sep=sep,encoding=enc,dtype=str,engine="python",on_bad_lines="skip")
            if len(df.columns)>1: return df
        except Exception: pass
    return pd.read_csv(path,dtype=str,on_bad_lines="skip")

def _json(path):
    with open(path,encoding="utf-8") as f: data=json.load(f)
    if isinstance(data,list): return pd.DataFrame(data)
    for v in (data.values() if isinstance(data,dict) else []):
        if isinstance(v,list) and v: return pd.DataFrame(v)
    return pd.json_normalize(data)

def _xml(path):
    with open(path,encoding="utf-8") as f: content=f.read()
    data=xmltodict.parse(content)
    def find_list(d,depth=0):
        if depth>5: return None
        if isinstance(d,dict):
            for v in d.values():
                if isinstance(v,list): return v
                r=find_list(v,depth+1)
                if r: return r
        return None
    rows=find_list(data)
    return pd.DataFrame(rows) if rows else pd.json_normalize(data)

def _pdf(path):
    import pdfplumber
    all_rows,headers=[],[]
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            for tbl in page.extract_tables():
                if not tbl: continue
                if not headers:
                    headers=[str(h).strip() if h else f"col_{i}" for i,h in enumerate(tbl[0])]
                    rows=tbl[1:]
                else: rows=tbl
                for row in rows:
                    if row: all_rows.append(dict(zip(headers,[str(c).strip() if c else "" for c in row])))
    if not all_rows: raise ValueError("No tables found in PDF")
    return pd.DataFrame(all_rows)

def _zip(path):
    with zipfile.ZipFile(path,"r") as zf:
        for name in zf.namelist():
            ext=Path(name).suffix.lower()
            if ext in (".xlsx",".xls",".csv",".txt",".json",".xml"):
                out=Path(path).parent/"zip_extracted"
                out.mkdir(exist_ok=True)
                extracted=zf.extract(name,str(out))
                # Return a DataFrame (not records) so the caller can call .dropna() on it
                if ext in (".xlsx",".xls"): return _excel(extracted)
                elif ext in (".csv",".txt"): return _csv(extracted)
                elif ext == ".json": return _json(extracted)
                elif ext == ".xml": return _xml(extracted)
    raise ValueError("No parseable file in ZIP")
