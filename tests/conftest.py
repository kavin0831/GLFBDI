"""
Shared pytest fixtures.

The Oracle FBDI app depends on a live MongoDB connection at import time
(database.py builds a client lazily, but several modules call _mdb() in
helpers). We stub the database with mongomock when available, so the tests
run hermetically — no Mongo server required.
"""
from __future__ import annotations

import os
import sys
import types
from pathlib import Path

import pytest

# Make the app package importable
APP_ROOT = Path(__file__).resolve().parent.parent
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

# Force the encryption layer to skip Fernet so file storage round-trips work
os.environ.setdefault("FERNET_KEY", "")


@pytest.fixture(autouse=True, scope="session")
def _stub_mongo():
    """Replace database._get_client with a mongomock client for the test session."""
    try:
        import mongomock
    except ImportError:
        pytest.skip("mongomock not installed — run `pip install -r requirements-test.txt`")

    import database as _db

    client = mongomock.MongoClient()
    _db._client = client  # bypass _get_client lazy init
    # Disable Fernet so stored files are plain base64 (deterministic round-trip)
    _db._fernet_inst = None

    # Patch _get_fernet to always return None for tests
    _db._get_fernet = lambda: None

    yield
    # No teardown needed — process exits


@pytest.fixture
def fresh_db():
    """Clear all collections between tests for isolation."""
    import database as _db
    db = _db._mdb()
    for name in db.list_collection_names():
        db[name].delete_many({})
    yield db


class _Cfg:
    """Tiny stand-in for AppSettings."""
    fusion_url = "https://fusion.example.com"
    fusion_username = "u"
    fusion_password = "p"
    fusion_document_account = "fin$/generalLedger$/import$"
    fusion_job_name = "test"


@pytest.fixture
def cfg():
    return _Cfg()


@pytest.fixture
def sample_csv_bytes() -> bytes:
    """Minimal CSV with friendly FBDI headers."""
    return (
        "*Status Code,*Ledger ID,*Effective Date of Transaction,"
        "*Currency Code,*Actual Flag,Segment1,Entered Debit Amount,"
        "Entered Credit Amount,Ledger Name\n"
        "NEW,300000046975971,2025-12-16,USD,A,1000,100.00,,US Primary Ledger\n"
        "NEW,300000046975971,2025-12-16,USD,A,1000,,100.00,US Primary Ledger\n"
    ).encode("utf-8")
