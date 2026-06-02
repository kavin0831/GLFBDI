"""
End-to-end: failed validation → user edits → reprocess → succeeded (mocked Oracle).

Rather than spawn the full workflow thread (which depends on the ML mapper and
makes the test slow and flaky), we simulate the lifecycle using the same
public endpoints / DB writes the workflow uses, then assert the version & file
state. This catches the integration between save_edit, reprocess, and the
download routes.
"""
from __future__ import annotations

from fastapi.testclient import TestClient


def _client():
    import app as _appmod
    return TestClient(_appmod.app)


def _seed_failed(req_id: str, csv_bytes: bytes):
    from database import store_uploaded_file, SessionLocal, JournalRequest
    store_uploaded_file(req_id, "bad.csv", csv_bytes)
    with SessionLocal() as db:
        req = JournalRequest(
            id=req_id, file_name="bad.csv", file_path="bad.csv",
            file_type="csv", file_size_bytes=len(csv_bytes),
            status="FAILED", current_stage="VALIDATION_FAILED",
            error_message="Mock failure: unbalanced",
            sender_email="test",
        )
        db.add(req)
        db.commit()


def test_failed_edit_reprocess_lifecycle(fresh_db, sample_csv_bytes, monkeypatch):
    client = _client()
    req_id = "e2e-1"
    _seed_failed(req_id, sample_csv_bytes)

    # Patch process_request so reprocess doesn't actually run the workflow.
    # We just want to know it was queued with the right state transitions.
    import app as _appmod
    spawned = {"called": False, "req_id": None}
    def _fake_process(rid):
        spawned["called"] = True
        spawned["req_id"] = rid
        # Mark "succeeded" as if the mocked Oracle import worked
        from database import SessionLocal, JournalRequest
        with SessionLocal() as db:
            r = db.get(JournalRequest, rid)
            if r:
                r.status = "SUCCEEDED"
                r.current_stage = "COMPLETED"
                db.commit()
    monkeypatch.setattr(_appmod, "process_request", _fake_process)

    # 1. Save edited rows
    payload = {
        "headers": ["*Status Code", "Entered Debit Amount", "Entered Credit Amount"],
        "rows": [
            ["NEW", "100.00", ""],
            ["NEW", "", "100.00"],
        ],
    }
    save_resp = client.post(f"/request/{req_id}/save_edit", json=payload)
    assert save_resp.status_code == 200
    assert save_resp.json()["version"] == 1

    # 2. Trigger reprocess (mocked process_request runs synchronously in thread)
    rp = client.post(f"/request/{req_id}/reprocess", follow_redirects=False)
    assert rp.status_code in (303, 307)

    # Give the daemon thread a moment to run our fake
    import time as _t
    for _ in range(20):
        if spawned["called"]:
            break
        _t.sleep(0.05)
    assert spawned["called"], "process_request was not spawned"
    assert spawned["req_id"] == req_id

    # 3. State should be SUCCEEDED now
    from database import SessionLocal, JournalRequest
    with SessionLocal() as db:
        r = db.get(JournalRequest, req_id)
        assert r.status == "SUCCEEDED"
        assert r.latest_edit_filename == "edited_v1.csv"
        assert r.version == 1

    # 4. Both versions visible in /files
    files = client.get(f"/request/{req_id}/files").json()
    kinds = {f["kind"] for f in files}
    assert "original" in kinds
    assert "edited_csv_v1" in kinds
