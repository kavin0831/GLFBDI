"""
MongoDB database layer — all state, logs, files, and secrets in MongoDB Atlas.
"""

import base64
import hashlib
import logging
import os
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pymongo import MongoClient

logger = logging.getLogger(__name__)

# MongoDB connection — set MONGO_URI in your environment (or .env).
# Default points at a local Mongo for development; production MUST set the env var.
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME   = os.environ.get("MONGO_DB",  "oracle_fbdi")

_client = None
_lock   = threading.Lock()
_CORRECT_JOB = "oracle/apps/ess/financials/generalLedger/programs/common,JournalImportLauncher"


def _get_client() -> MongoClient:
    global _client
    if _client is None:
        with _lock:
            if _client is None:
                _client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15000)
    return _client


def _mdb():
    return _get_client()[DB_NAME]


# ── Encryption helpers (Fernet symmetric) ────────────────────────────────────

_fernet_inst   = None
_fernet_lock   = threading.Lock()
_KEY_FILE_PATH = Path(__file__).parent / "config" / "secret.key"


def _get_fernet():
    global _fernet_inst
    if _fernet_inst is None:
        with _fernet_lock:
            if _fernet_inst is None:
                try:
                    from cryptography.fernet import Fernet
                    # Priority: env var (HF Spaces / Docker) > local key file > generate new
                    env_key = os.environ.get("FERNET_KEY", "").strip()
                    if env_key:
                        key = env_key.encode()
                        logger.info("Using Fernet key from FERNET_KEY env var")
                    else:
                        _KEY_FILE_PATH.parent.mkdir(exist_ok=True)
                        if _KEY_FILE_PATH.exists():
                            key = _KEY_FILE_PATH.read_bytes().strip()
                        else:
                            key = Fernet.generate_key()
                            _KEY_FILE_PATH.write_bytes(key)
                            logger.info("Generated new encryption key: %s", _KEY_FILE_PATH)
                    _fernet_inst = Fernet(key)
                except ImportError:
                    logger.warning("cryptography not installed — encryption disabled")
                    _fernet_inst = None
    return _fernet_inst


def encrypt_value(plaintext: str) -> str:
    f = _get_fernet()
    if f is None:
        return plaintext
    return f.encrypt(plaintext.encode()).decode()


def decrypt_value(ciphertext: str) -> str:
    f = _get_fernet()
    if f is None:
        return ciphertext
    return f.decrypt(ciphertext.encode()).decode()


_ENCRYPT_FIELDS = frozenset(["fusion_password", "gmail_app_password"])


# ── File hashing ──────────────────────────────────────────────────────────────

def hash_file(path) -> str:
    """Return SHA-256 hex digest of a file (empty string on error)."""
    try:
        h = hashlib.sha256()
        with open(path, "rb") as fp:
            for chunk in iter(lambda: fp.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return ""


# ── Attribute-accessible document wrapper ────────────────────────────────────

class _AttrDoc:
    def __init__(self, data: dict):
        normalized = {("id" if k == "_id" else k): v for k, v in data.items()}
        object.__setattr__(self, "_data",  normalized)
        object.__setattr__(self, "_dirty", {})

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        return object.__getattribute__(self, "_data").get(name)

    def __setattr__(self, name, value):
        if name.startswith("_"):
            object.__setattr__(self, name, value)
            return
        data  = object.__getattribute__(self, "_data")
        dirty = object.__getattribute__(self, "_dirty")
        data[name]  = value
        dirty[name] = value

    def _raw(self) -> dict:
        return object.__getattribute__(self, "_data")

    def _pop_dirty(self) -> dict:
        dirty = object.__getattribute__(self, "_dirty")
        result = dict(dirty)
        object.__setattr__(self, "_dirty", {})
        return result


class AppSettings(_AttrDoc):
    """Settings model — sensitive fields auto-encrypted at rest."""

    @property
    def id(self):
        return 1  # compatibility: db.get(AppSettings, 1)

    def __setattr__(self, name, value):
        if name in _ENCRYPT_FIELDS and value and not str(value).startswith("enc:"):
            try:
                value = "enc:" + encrypt_value(str(value))
            except Exception:
                pass
        super().__setattr__(name, value)

    def __getattr__(self, name):
        val = super().__getattr__(name)
        if name in _ENCRYPT_FIELDS and val and isinstance(val, str) and val.startswith("enc:"):
            try:
                return decrypt_value(val[4:])
            except Exception:
                return ""
        return val


class JournalRequest(_AttrDoc):
    def __init__(self, **kwargs):
        defaults = {
            "id":               str(uuid.uuid4()),
            "transaction_type": "GL",          # "GL" | "AP"
            "status":           "RECEIVED",
            "current_stage":    "QUEUED",
            "created_at":       datetime.now(timezone.utc),
            "updated_at":       datetime.now(timezone.utc),
            "total_rows":       0,
            "good_rows":        0,
            "bad_rows":         0,
            "approval_status":  "NOT_REQUIRED",
        }
        defaults.update(kwargs)
        super().__init__(defaults)


class MappingHistory(_AttrDoc):
    def __init__(self, **kwargs):
        defaults = {
            "id":         str(uuid.uuid4()),
            "created_at": datetime.now(timezone.utc),
            "times_used": 1,
            "confidence": 1.0,
        }
        defaults.update(kwargs)
        super().__init__(defaults)


# ── Collection map ────────────────────────────────────────────────────────────

_COL_MAP = {
    AppSettings:    "app_settings",
    JournalRequest: "journal_requests",
    MappingHistory: "mapping_history",
}


# ── Query builder ─────────────────────────────────────────────────────────────

class _Query:
    def __init__(self, model_cls, session):
        self._model   = model_cls
        self._session = session
        self._filter: dict = {}
        self._sort    = None
        self._limit_n = 0

    def filter_by(self, **kwargs):
        self._filter.update(kwargs)
        return self

    def filter(self, expr):
        if isinstance(expr, dict):
            self._filter.update(expr)
        return self

    def order_by(self, sort_spec):
        if isinstance(sort_spec, (tuple, list)) and len(sort_spec) == 2:
            self._sort = [tuple(sort_spec)]
        return self

    def limit(self, n: int):
        self._limit_n = n
        return self

    def _wrap(self, doc):
        if doc is None:
            return None
        d = {("id" if k == "_id" else k): v for k, v in doc.items()}
        obj = object.__new__(self._model)
        _AttrDoc.__init__(obj, d)
        return obj

    def all(self) -> list:
        col = _mdb()[_COL_MAP[self._model]]
        cursor = col.find(self._filter)
        if self._sort:
            cursor = cursor.sort(self._sort)
        if self._limit_n:
            cursor = cursor.limit(self._limit_n)
        return [self._wrap(d) for d in cursor]

    def count(self) -> int:
        return _mdb()[_COL_MAP[self._model]].count_documents(self._filter)

    def first(self):
        doc = _mdb()[_COL_MAP[self._model]].find_one(self._filter)
        obj = self._wrap(doc)
        if obj and self._session:
            self._session._track(obj)
        return obj


# ── Session ───────────────────────────────────────────────────────────────────

class _MongoSession:
    def __init__(self):
        self._to_insert: list = []
        self._tracked:   list = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *_):
        if exc_type is None:
            self.commit()
        return False

    def _track(self, obj):
        if obj not in self._tracked:
            self._tracked.append(obj)

    def _mongo_id(self, obj):
        if isinstance(obj, AppSettings):
            return "settings"
        return obj._raw().get("id")

    def get(self, model_cls, id_val):
        mongo_id = "settings" if model_cls is AppSettings else str(id_val)
        doc = _mdb()[_COL_MAP[model_cls]].find_one({"_id": mongo_id})
        if doc is None:
            return None
        d = {("id" if k == "_id" else k): v for k, v in doc.items()}
        obj = object.__new__(model_cls)
        _AttrDoc.__init__(obj, d)
        self._track(obj)
        return obj

    def add(self, obj):
        self._to_insert.append(obj)
        self._track(obj)

    def commit(self):
        mdb = _mdb()
        for obj in self._to_insert:
            data = {k: v for k, v in obj._raw().items() if k != "id"}
            data["_id"] = self._mongo_id(obj)
            col = mdb[_COL_MAP[type(obj)]]
            col.replace_one({"_id": data["_id"]}, data, upsert=True)
            obj._pop_dirty()

        for obj in self._tracked:
            if obj in self._to_insert:
                continue
            dirty = obj._pop_dirty()
            dirty.pop("id", None)
            if not dirty:
                continue
            col = mdb[_COL_MAP[type(obj)]]
            col.update_one({"_id": self._mongo_id(obj)}, {"$set": dirty})

        self._to_insert.clear()

    def rollback(self):
        self._to_insert.clear()
        self._tracked.clear()

    def refresh(self, obj):
        doc = _mdb()[_COL_MAP[type(obj)]].find_one({"_id": self._mongo_id(obj)})
        if doc:
            d = {("id" if k == "_id" else k): v for k, v in doc.items()}
            obj._raw().update(d)
            obj._pop_dirty()

    def query(self, model_cls):
        return _Query(model_cls, self)

    def close(self):
        pass


SessionLocal = _MongoSession


def get_db():
    db = _MongoSession()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise


# ── Default settings ──────────────────────────────────────────────────────────

_AP_JOB = "oracle/apps/ess/financials/payables/invoices/transactions,APXIIMPT"

_DEFAULT_SETTINGS = {
    # Placeholders — fill in real values via the /settings page after first run.
    "fusion_url":              "",
    "fusion_username":         "",
    "fusion_password":         "",
    # ── GL (Journal Import) ──────────────────────────────────────────────────
    "fusion_document_account": "fin$/generalLedger$/import$",
    "fusion_job_name":         _CORRECT_JOB,
    "fusion_ledger_name":      "US Primary Ledger",
    # ── AP (Payables Invoice Import) ─────────────────────────────────────────
    "ap_document_account":     "fin$/payables$/import$",
    "ap_job_name":             _AP_JOB,
    "ap_business_unit_id":     "",            # numeric ID (e.g. 300000046987012)
    "ap_business_unit_name":   "",            # human label (e.g. US1 Business Unit)
    "ap_ledger_id":            "",            # numeric ID (e.g. 300000046975971)
    "ap_source":               "External",
    "ap_pay_group":            "1000",
    "ap_invoice_group":        "",            # default Import Set token
    # ── Gmail polling ────────────────────────────────────────────────────────
    "gmail_credentials_file":  "config/gmail_credentials.json",
    "gmail_token_file":        "config/gmail_token.json",
    "gmail_poll_seconds":      60,
    "gmail_subject_filter":    "journal upload",
    "gmail_subject_filter_ap": "invoice upload",   # separate filter for AP polling
    # App Password mode (no OAuth) — set these via /settings
    "gmail_user":              "",
    "gmail_app_password":      "",
    "notification_email":      "",
    "mapping_threshold":       0.70,
    "ess_poll_seconds":        5,
    "ess_max_minutes":         30,
    # Public host used in approval emails. Leave blank to auto-detect from
    # the SPACE_HOST env var (HF Spaces) or APP_BASE_URL env var.
    "app_base_url":            "",
}


def get_settings() -> AppSettings:
    mdb = _mdb()
    doc = mdb["app_settings"].find_one({"_id": "settings"})
    if doc is None:
        seed = {**_DEFAULT_SETTINGS, "_id": "settings",
                "updated_at": datetime.now(timezone.utc)}
        # Encrypt sensitive fields before initial insert
        for f in _ENCRYPT_FIELDS:
            if seed.get(f):
                seed[f] = "enc:" + encrypt_value(seed[f])
        mdb["app_settings"].insert_one(seed)
        doc = mdb["app_settings"].find_one({"_id": "settings"})
    d = {("id" if k == "_id" else k): v for k, v in doc.items()}
    obj = object.__new__(AppSettings)
    _AttrDoc.__init__(obj, d)
    return obj


def init_db():
    """Create MongoDB indexes and seed default settings."""
    try:
        mdb = _mdb()
        mdb["journal_requests"].create_index("created_at")
        mdb["journal_requests"].create_index("status")
        mdb["journal_requests"].create_index("approval_token", sparse=True)
        mdb["mapping_history"].create_index(
            [("source_column", 1), ("target_field", 1)])
        mdb["process_logs"].create_index("request_id", sparse=True)
        mdb["secure_files"].create_index("name", sparse=True)
        mdb["uploaded_files"].create_index("stored_at")
        mdb["generated_files"].create_index("_id")

        if mdb["app_settings"].count_documents({"_id": "settings"}) == 0:
            seed = {**_DEFAULT_SETTINGS, "_id": "settings",
                    "updated_at": datetime.now(timezone.utc)}
            for f in _ENCRYPT_FIELDS:
                if seed.get(f):
                    seed[f] = "enc:" + encrypt_value(seed[f])
            mdb["app_settings"].insert_one(seed)
            logger.info("Seeded default settings (password encrypted)")
        else:
            # Migrate: shorten ess_poll_seconds if still on the legacy 60s default
            mdb["app_settings"].update_one(
                {"_id": "settings"},
                {"$set": {"fusion_job_name": _CORRECT_JOB}},
            )
            mdb["app_settings"].update_one(
                {"_id": "settings", "ess_poll_seconds": {"$gte": 30}},
                {"$set": {"ess_poll_seconds": 5}},
            )
            # Heal Gmail file-path fields if they're missing or empty —
            # otherwise Path("") becomes Path('.') and write_text() crashes.
            mdb["app_settings"].update_one(
                {"_id": "settings", "$or": [
                    {"gmail_credentials_file": {"$exists": False}},
                    {"gmail_credentials_file": ""},
                    {"gmail_credentials_file": None},
                ]},
                {"$set": {"gmail_credentials_file": "config/gmail_credentials.json"}},
            )
            mdb["app_settings"].update_one(
                {"_id": "settings", "$or": [
                    {"gmail_token_file": {"$exists": False}},
                    {"gmail_token_file": ""},
                    {"gmail_token_file": None},
                ]},
                {"$set": {"gmail_token_file": "config/gmail_token.json"}},
            )
            # Backfill AP-related setting fields if missing
            ap_setonly = {k: v for k, v in _DEFAULT_SETTINGS.items() if k.startswith("ap_") or k == "gmail_subject_filter_ap"}
            existing = mdb["app_settings"].find_one({"_id": "settings"}) or {}
            missing  = {k: v for k, v in ap_setonly.items() if k not in existing}
            if missing:
                mdb["app_settings"].update_one({"_id": "settings"}, {"$set": missing})
                logger.info("Backfilled %d AP setting fields", len(missing))

        # Backfill transaction_type='GL' on all existing journal_requests
        # so the dashboard tab filter works correctly.
        try:
            res = mdb["journal_requests"].update_many(
                {"transaction_type": {"$exists": False}},
                {"$set": {"transaction_type": "GL"}},
            )
            if res.modified_count:
                logger.info("Backfilled transaction_type=GL on %d legacy requests",
                            res.modified_count)
        except Exception as e:
            logger.warning("transaction_type backfill skipped: %s", e)

        # Indexes for the new field
        mdb["journal_requests"].create_index("transaction_type", sparse=True)
        logger.info("MongoDB ready — %s", DB_NAME)
    except Exception as e:
        logger.error("MongoDB init failed: %s", e)
        raise


# ── Secure file storage (credentials, tokens) ────────────────────────────────

def store_secure_file(name: str, content: bytes):
    """Store a file in MongoDB, encrypted with Fernet."""
    try:
        f = _get_fernet()
        stored = f.encrypt(content).decode() if f else base64.b64encode(content).decode()
        _mdb()["secure_files"].replace_one(
            {"_id": name},
            {
                "_id":       name,
                "content":   stored,
                "size_bytes": len(content),
                "stored_at": datetime.now(timezone.utc),
                "encrypted": f is not None,
            },
            upsert=True,
        )
        logger.info("Stored secure file: %s (%d bytes)", name, len(content))
    except Exception as e:
        logger.warning("store_secure_file failed for %s: %s", name, e)


def get_secure_file(name: str) -> bytes | None:
    """Retrieve and decrypt a secure file from MongoDB."""
    try:
        doc = _mdb()["secure_files"].find_one({"_id": name})
        if not doc:
            return None
        raw = doc["content"].encode()
        if doc.get("encrypted", True):
            f = _get_fernet()
            return f.decrypt(raw) if f else base64.b64decode(raw)
        return base64.b64decode(raw)
    except Exception as e:
        logger.warning("get_secure_file failed for %s: %s", name, e)
        return None


# ── MongoDB log storage ───────────────────────────────────────────────────────

def store_log_file(request_id: str, filename: str, file_bytes: bytes):
    """Store a log/output file in MongoDB as base64."""
    try:
        safe_key = (filename
                    .replace(".", "_").replace("/", "_")
                    .replace("\\", "_").replace(" ", "_"))
        b64 = base64.b64encode(file_bytes).decode()
        sha = hashlib.sha256(file_bytes).hexdigest()
        _mdb()["process_logs"].update_one(
            {"_id": request_id},
            {"$set": {
                f"files.{safe_key}": {
                    "filename":   filename,
                    "content_b64": b64,
                    "sha256":     sha,
                    "size_bytes": len(file_bytes),
                    "stored_at":  datetime.now(timezone.utc),
                }
            }},
            upsert=True,
        )
    except Exception as e:
        logger.warning("store_log_file failed for %s/%s: %s", request_id, filename, e)


def append_log(request_id: str, level: str, message: str):
    """Append a structured log entry to the request's MongoDB log document."""
    try:
        _mdb()["process_logs"].update_one(
            {"_id": request_id},
            {"$push": {"logs": {
                "ts":    datetime.now(timezone.utc).isoformat(),
                "level": level,
                "msg":   message,
            }}},
            upsert=True,
        )
    except Exception:
        pass


def get_process_logs(request_id: str) -> list[dict]:
    """Return all log entries for a request from MongoDB."""
    try:
        doc = _mdb()["process_logs"].find_one({"_id": request_id})
        return doc.get("logs", []) if doc else []
    except Exception:
        return []


def get_log_files_meta(request_id: str) -> dict:
    """Return metadata (name, size, sha256) for stored log files."""
    try:
        doc = _mdb()["process_logs"].find_one({"_id": request_id},
                                               {"files": 1})
        if not doc or not doc.get("files"):
            return {}
        return {k: {kk: vv for kk, vv in v.items() if kk != "content_b64"}
                for k, v in doc["files"].items()}
    except Exception:
        return {}


# ── Uploaded file storage (journal source files) ─────────────────────────────

def store_uploaded_file(req_id: str, filename: str, content: bytes):
    """Store an uploaded journal file in MongoDB (base64 + SHA-256 + optional Fernet)."""
    try:
        f = _get_fernet()
        stored = f.encrypt(content).decode() if f else base64.b64encode(content).decode()
        sha = hashlib.sha256(content).hexdigest()
        _mdb()["uploaded_files"].replace_one(
            {"_id": req_id},
            {
                "_id":        req_id,
                "filename":   filename,
                "content":    stored,
                "sha256":     sha,
                "size_bytes": len(content),
                "stored_at":  datetime.now(timezone.utc),
                "encrypted":  f is not None,
            },
            upsert=True,
        )
    except Exception as e:
        logger.warning("store_uploaded_file failed for %s: %s", req_id, e)


def get_uploaded_file(req_id: str) -> bytes | None:
    """Retrieve and decrypt an uploaded file from MongoDB."""
    try:
        doc = _mdb()["uploaded_files"].find_one({"_id": req_id})
        if not doc:
            return None
        raw = doc["content"].encode()
        if doc.get("encrypted", True):
            f = _get_fernet()
            return f.decrypt(raw) if f else base64.b64decode(raw)
        return base64.b64decode(raw)
    except Exception as e:
        logger.warning("get_uploaded_file failed for %s: %s", req_id, e)
        return None


# ── Generated file storage (FBDI CSV, ZIP, bad_data, ESS log) ────────────────
# Stored in `generated_files` collection, keyed by request_id + file kind.

def store_generated_file(req_id: str, kind: str, filename: str, content: bytes):
    """
    Store a generated file (kind = 'fbdi_csv' | 'fbdi_zip' | 'bad_csv' | 'ess_log') in MongoDB.
    Content is base64-encoded; SHA-256 is computed for integrity (not displayed).
    """
    try:
        b64 = base64.b64encode(content).decode()
        sha = hashlib.sha256(content).hexdigest()
        _mdb()["generated_files"].update_one(
            {"_id": req_id},
            {"$set": {
                f"files.{kind}": {
                    "filename":    filename,
                    "content_b64": b64,
                    "sha256":      sha,
                    "size_bytes":  len(content),
                    "stored_at":   datetime.now(timezone.utc),
                }
            }},
            upsert=True,
        )
    except Exception as e:
        logger.warning("store_generated_file failed for %s/%s: %s", req_id, kind, e)


def get_generated_file(req_id: str, kind: str) -> tuple[bytes, str] | None:
    """Retrieve a generated file from MongoDB. Returns (bytes, filename) or None."""
    try:
        doc = _mdb()["generated_files"].find_one({"_id": req_id})
        if not doc or kind not in doc.get("files", {}):
            return None
        f = doc["files"][kind]
        return base64.b64decode(f["content_b64"]), f["filename"]
    except Exception as e:
        logger.warning("get_generated_file failed for %s/%s: %s", req_id, kind, e)
        return None


def has_generated_file(req_id: str, kind: str) -> bool:
    """Check existence without fetching content."""
    try:
        doc = _mdb()["generated_files"].find_one(
            {"_id": req_id, f"files.{kind}": {"$exists": True}},
            {"_id": 1},
        )
        return doc is not None
    except Exception:
        return False


# ── Import Journals claim tracking (cross-submission exclusivity) ────────────

def claim_ji_request(oracle_rid: str, our_request_id: str, job_name: str = "") -> bool:
    """
    Atomically claim an Oracle Import Journals request ID for the current submission.
    Returns True if WE claimed it (first), False if another submission already owns it.
    Used to prevent concurrent submissions from collecting each other's JI logs.
    """
    try:
        from pymongo.errors import DuplicateKeyError
        try:
            _mdb()["ji_claims"].insert_one({
                "_id":         str(oracle_rid),
                "claimed_by":  our_request_id,
                "job_name":    job_name,
                "claimed_at":  datetime.now(timezone.utc),
            })
            return True
        except DuplicateKeyError:
            return False
    except Exception as e:
        logger.debug("claim_ji_request failed for %s: %s", oracle_rid, e)
        return False


def get_ji_claim_owner(oracle_rid: str) -> str | None:
    """Return the request_id that owns this Oracle JI request, or None."""
    try:
        doc = _mdb()["ji_claims"].find_one({"_id": str(oracle_rid)})
        return doc["claimed_by"] if doc else None
    except Exception:
        return None
