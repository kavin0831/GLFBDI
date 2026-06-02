"""Edit & save round-trip: parse -> edit table -> save -> reload identical."""
from __future__ import annotations

import io
import csv as _csv

from fastapi.testclient import TestClient


def _make_app_client():
    import app as _appmod
    return TestClient(_appmod.app)


def _seed_request(req_id: str, csv_bytes: bytes, filename: str = "test_in.csv"):
    from database import store_uploaded_file, SessionLocal, JournalRequest
    store_uploaded_file(req_id, filename, csv_bytes)
    with SessionLocal() as db:
        req = JournalRequest(
            id=req_id,
            file_name=filename,
            file_path=filename,
            file_type="csv",
            file_size_bytes=len(csv_bytes),
            status="FAILED",
            current_stage="VALIDATION_FAILED",
            sender_email="test",
        )
        db.add(req)
        db.commit()


def test_save_edit_roundtrip(fresh_db, sample_csv_bytes):
    client = _make_app_client()
    req_id = "edit-roundtrip-1"
    _seed_request(req_id, sample_csv_bytes)

    # Pretend the user submitted some edited rows
    payload = {
        "headers": ["*Status Code", "*Ledger ID", "*Currency Code", "Segment1",
                    "Entered Debit Amount", "Entered Credit Amount"],
        "rows": [
            ["NEW", "300000046975971", "USD", "1000", "200.00", ""],
            ["NEW", "300000046975971", "USD", "1000", "", "200.00"],
        ],
    }
    r = client.post(f"/request/{req_id}/save_edit", json=payload)
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["ok"] is True
    assert data["version"] == 1
    assert data["filename"] == "edited_v1.csv"

    # Reload — download the just-saved file and reparse
    from database import get_generated_file
    result = get_generated_file(req_id, "edited_csv_v1")
    assert result is not None
    body, fname = result
    assert fname == "edited_v1.csv"

    reader = _csv.reader(io.StringIO(body.decode("utf-8")))
    rows = list(reader)
    assert rows[0] == payload["headers"]
    assert rows[1] == payload["rows"][0]
    assert rows[2] == payload["rows"][1]

    # Saving again should bump to v2
    r2 = client.post(f"/request/{req_id}/save_edit", json=payload)
    assert r2.status_code == 200
    assert r2.json()["version"] == 2


def test_list_files_includes_original_and_edits(fresh_db, sample_csv_bytes):
    client = _make_app_client()
    req_id = "edit-roundtrip-2"
    _seed_request(req_id, sample_csv_bytes)

    payload = {"headers": ["*Status Code"], "rows": [["NEW"]]}
    client.post(f"/request/{req_id}/save_edit", json=payload)

    r = client.get(f"/request/{req_id}/files")
    assert r.status_code == 200
    kinds = {f["kind"] for f in r.json()}
    assert "original" in kinds
    assert "edited_csv_v1" in kinds


def test_download_original_and_edit(fresh_db, sample_csv_bytes):
    client = _make_app_client()
    req_id = "edit-roundtrip-3"
    _seed_request(req_id, sample_csv_bytes, filename="src.csv")

    # Original
    r = client.get(f"/request/{req_id}/download/src.csv")
    assert r.status_code == 200
    assert r.content == sample_csv_bytes

    # Save an edit then download it
    payload = {"headers": ["A", "B"], "rows": [["1", "2"]]}
    client.post(f"/request/{req_id}/save_edit", json=payload)
    r2 = client.get(f"/request/{req_id}/download/edited_v1.csv")
    assert r2.status_code == 200
    assert b"A,B" in r2.content
    assert b"1,2" in r2.content
