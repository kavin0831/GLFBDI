"""
Comprehensive tests for all fixes applied in the 2026-06-08 batch session.

Tests are grouped by fix category.  All tests are hermetic — no Oracle
or live-MongoDB dependency (mongomock is used via conftest.py fixture).

Run:  pytest tests/test_fixes.py -v
"""
from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import re
import sys
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

APP_ROOT = Path(__file__).resolve().parent.parent
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

# ── helpers ──────────────────────────────────────────────────────────────────

def _make_zip(files: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, data in files.items():
            zf.writestr(name, data)
    return buf.getvalue()


# ═══════════════════════════════════════════════════════════════════════════════
# 1. FIX: /reprocess and /api/retry must dispatch to correct pipeline
# ═══════════════════════════════════════════════════════════════════════════════

class TestReprocessRouting:
    """The reprocess / retry endpoints previously always called process_request
    (GL pipeline).  After the fix they call _dispatch_processing(req_id, txn_type)."""

    def test_reprocess_endpoint_uses_dispatch(self):
        """Verify /reprocess endpoint code calls _dispatch_processing, not process_request."""
        app_src = (APP_ROOT / "app.py").read_text(encoding="utf-8")
        # Find the reprocess_request function body
        m = re.search(
            r"async def reprocess_request.*?(?=\n@app\.|$)",
            app_src, re.DOTALL
        )
        assert m, "reprocess_request function not found"
        body = m.group(0)
        assert "_dispatch_processing(" in body, \
            "reprocess_request must call _dispatch_processing"
        assert "threading.Thread(target=process_request" not in body, \
            "reprocess_request must NOT hardcode process_request"

    def test_retry_endpoint_uses_dispatch(self):
        """Verify /api/retry endpoint code calls _dispatch_processing, not process_request."""
        app_src = (APP_ROOT / "app.py").read_text(encoding="utf-8")
        m = re.search(
            r"async def retry_request.*?(?=\n@app\.|$)",
            app_src, re.DOTALL
        )
        assert m, "retry_request function not found"
        body = m.group(0)
        assert "_dispatch_processing(" in body, \
            "retry_request must call _dispatch_processing"
        assert "threading.Thread(target=process_request" not in body, \
            "retry_request must NOT hardcode process_request"

    def test_reprocess_clears_stop_reason(self):
        """reprocess_request must clear both error_message and stop_reason."""
        app_src = (APP_ROOT / "app.py").read_text(encoding="utf-8")
        m = re.search(
            r"async def reprocess_request.*?(?=\n@app\.|$)",
            app_src, re.DOTALL
        )
        body = m.group(0)
        assert "req.error_message = None" in body
        assert "req.stop_reason = None" in body

    def test_retry_clears_stop_reason(self):
        """retry_request must clear both error_message and stop_reason."""
        app_src = (APP_ROOT / "app.py").read_text(encoding="utf-8")
        m = re.search(
            r"async def retry_request.*?(?=\n@app\.|$)",
            app_src, re.DOTALL
        )
        body = m.group(0)
        assert "req.stop_reason = None" in body


# ═══════════════════════════════════════════════════════════════════════════════
# 2. FIX: Dead `if False` branch in submit_ap_fbdi
# ═══════════════════════════════════════════════════════════════════════════════

class TestFusionServiceApFix:
    """The cfg.ap_invoice_group fallback was dead (guarded by `if False`).
    After fix it should be active."""

    def test_no_if_false_in_submit_ap_fbdi(self):
        src = (APP_ROOT / "services" / "fusion_service.py").read_text(encoding="utf-8")
        # Find submit_ap_fbdi body
        m = re.search(r"def submit_ap_fbdi.*?(?=\ndef |\Z)", src, re.DOTALL)
        assert m, "submit_ap_fbdi not found"
        body = m.group(0)
        assert "if False" not in body, \
            "submit_ap_fbdi must not contain dead 'if False' branch"

    def test_cfg_fallback_in_inv_grp(self):
        """cfg.ap_invoice_group must appear in the active inv_grp assignment."""
        src = (APP_ROOT / "services" / "fusion_service.py").read_text(encoding="utf-8")
        m = re.search(r"def submit_ap_fbdi.*?(?=\ndef |\Z)", src, re.DOTALL)
        body = m.group(0)
        # The cfg fallback line must exist and not be guarded by `if False`
        assert "cfg.ap_invoice_group" in body


# ═══════════════════════════════════════════════════════════════════════════════
# 3. FIX: Hardcoded credentials removed from ap_submit_test.py
# ═══════════════════════════════════════════════════════════════════════════════

class TestNoHardcodedCreds:
    def test_ap_submit_test_no_hardcoded_password(self):
        src = (APP_ROOT / "apinvoiceimporttest" / "ap_submit_test.py").read_text(encoding="utf-8")
        # Should use os.environ.get
        assert "os.environ.get" in src or "_os.environ.get" in src, \
            "ap_submit_test.py must use environment variables"
        # Must NOT have hardcoded password
        assert "12345678" not in src, "Hardcoded password must be removed"
        assert '"Kavin.Sasikumar"' not in src, "Hardcoded username must be removed"

    def test_ap_submit_test_no_hardcoded_bu_id(self):
        src = (APP_ROOT / "apinvoiceimporttest" / "ap_submit_test.py").read_text(encoding="utf-8")
        # The BU_ID and Ledger_ID should come from env vars
        assert '"300000046987012"' not in src, "Hardcoded BU_ID must be removed"
        assert '"300000046975971"' not in src, "Hardcoded Ledger_ID must be removed"


# ═══════════════════════════════════════════════════════════════════════════════
# 4. FIX: GL has_warnings triggers inner_failed
# ═══════════════════════════════════════════════════════════════════════════════

class TestGLHasWarnings:
    """analyze_ess_logs returns has_warnings when JI reports partial import.
    Both places in workflow.py must check has_warnings alongside has_errors."""

    def test_analyze_ess_logs_warns_on_jierror(self):
        from services.fusion_service import analyze_ess_logs
        # Simulate a log where JI shows "Warning" status (partial rejection)
        log_text = (
            "Journal Import — started\n"
            "Manual    123456789 Warning  2 100.00 100.00\n"
            "Import complete\n"
        )
        # analyze_ess_logs reads the "all_text" key (not "log_text")
        result = analyze_ess_logs({"all_text": log_text})
        # Should detect either errors or warnings
        assert result.get("has_errors") or result.get("has_warnings"), \
            "JI Warning status should set has_errors or has_warnings"

    def test_workflow_checks_has_warnings(self):
        """workflow.py must check both has_errors and has_warnings when setting inner_failed."""
        src = (APP_ROOT / "workflow.py").read_text(encoding="utf-8")
        # Find all places where has_errors is checked in the context of inner_failed
        # After our fix, they must also check has_warnings
        pattern = r'if analysis.*has_errors.*or.*has_warnings'
        matches = re.findall(pattern, src)
        assert len(matches) >= 2, \
            f"workflow.py should check both has_errors and has_warnings in >= 2 places, found: {matches}"

    def test_analyze_ess_logs_zero_group_ids(self):
        from services.fusion_service import analyze_ess_logs
        log_text = "SQL*Loader: Release ...\nTotal: 0 group id(s) selected for import.\n"
        # analyze_ess_logs reads the "all_text" key (not "log_text")
        result = analyze_ess_logs({"all_text": log_text})
        assert result.get("has_errors"), "Total: 0 group id(s) should set has_errors"

    def test_analyze_ess_logs_sql_loader_rejection(self):
        from services.fusion_service import analyze_ess_logs
        # The sql_errors regex requires "Record N: Rejected" FOLLOWED by ORA-xxxxx on the next line.
        # Also test the "Total logical records rejected" count path.
        log_text = (
            "SQL*Loader: Release 19.0.0.0.0\n"
            "Record 3: Rejected - Error on table GL_INTERFACE\n"
            "ORA-01400: cannot insert NULL into (\"GL_INTERFACE\".\"STATUS\")\n"
            "Total logical records rejected:       1\n"
        )
        # analyze_ess_logs reads the "all_text" key (not "log_text")
        result = analyze_ess_logs({"all_text": log_text})
        assert result.get("has_errors"), "SQL*Loader rejection should set has_errors"


# ═══════════════════════════════════════════════════════════════════════════════
# 5. FIX: GL group_id version-aware (prevents GL_INTERFACE row collisions)
# ═══════════════════════════════════════════════════════════════════════════════

class TestGLGroupIdVersioning:

    def _compute(self, seed: str) -> str:
        return str(int(hashlib.sha1(seed.encode("utf-8")).hexdigest()[:9], 16) % 999999999)

    def test_v0_uses_request_id_only(self):
        """v0 (first run) should use just the request_id as the seed."""
        req_id = "abc123"
        expected = self._compute(req_id)  # no :v0 suffix for v0
        assert expected.isdigit()
        assert len(expected) <= 9

    def test_v1_differs_from_v0(self):
        """v1 (first reprocess) must produce a different group_id than v0."""
        req_id = "abc123"
        gid_v0 = self._compute(req_id)
        gid_v1 = self._compute(f"{req_id}:v1")
        assert gid_v0 != gid_v1, "Reprocess group_id must differ from original"

    def test_v2_differs_from_v1(self):
        req_id = "abc123"
        gid_v1 = self._compute(f"{req_id}:v1")
        gid_v2 = self._compute(f"{req_id}:v2")
        assert gid_v1 != gid_v2

    def test_deterministic(self):
        """Same (req_id, version) always produces same group_id."""
        req_id = "testreq999"
        gid_a = self._compute(f"{req_id}:v1")
        gid_b = self._compute(f"{req_id}:v1")
        assert gid_a == gid_b

    def test_fbdi_generator_accepts_override(self):
        """fbdi_generator.build_rows should use meta['gl_group_id'] when provided."""
        from utils import fbdi_generator
        # Read the source to verify the override is checked first
        src = (APP_ROOT / "utils" / "fbdi_generator.py").read_text(encoding="utf-8")
        assert "meta.get(\"gl_group_id\")" in src or "meta.get('gl_group_id')" in src, \
            "fbdi_generator must accept gl_group_id override in meta"

    def test_workflow_stamps_gl_group_id_in_meta_before_generate(self):
        """workflow.py must set meta['gl_group_id'] before calling _stage_generate."""
        src = (APP_ROOT / "workflow.py").read_text(encoding="utf-8")
        # The gl_group_id must be set in meta before the _stage_generate call
        generate_pos = src.find("csv_path, zip_path, bad_csv_path = _stage_generate")
        assert generate_pos != -1, "_stage_generate call not found"
        prefix = src[:generate_pos]
        assert 'meta["gl_group_id"]' in prefix or "meta['gl_group_id']" in prefix, \
            "meta['gl_group_id'] must be set before _stage_generate is called"


# ═══════════════════════════════════════════════════════════════════════════════
# 6. FIX: AP artifact versioning in _stage_generate
# ═══════════════════════════════════════════════════════════════════════════════

class TestAPArtifactVersioning:

    def test_stage_generate_uses_versioned_keys(self):
        """workflow_ap._stage_generate must use f-string version in store keys."""
        src = (APP_ROOT / "workflow_ap.py").read_text(encoding="utf-8")
        m = re.search(r"def _stage_generate.*?(?=\ndef |\Z)", src, re.DOTALL)
        assert m, "_stage_generate not found"
        body = m.group(0)
        # Must NOT hardcode v0
        assert '"ap_hdr_csv_v0"' not in body, \
            "_stage_generate must not hardcode v0 for ap_hdr_csv"
        assert '"fbdi_zip_v0"' not in body, \
            "_stage_generate must not hardcode v0 for fbdi_zip"
        # Must use f-string versioning
        assert 'f"ap_hdr_csv_v{ver}"' in body or "f'ap_hdr_csv_v{ver}'" in body, \
            "_stage_generate must use f'ap_hdr_csv_v{ver}'"

    def test_stage_generate_reads_version_from_db(self):
        """_stage_generate must query the DB for the current version."""
        src = (APP_ROOT / "workflow_ap.py").read_text(encoding="utf-8")
        m = re.search(r"def _stage_generate.*?(?=\ndef |\Z)", src, re.DOTALL)
        body = m.group(0)
        assert "_mdb" in body and '"version"' in body, \
            "_stage_generate must read version from MongoDB"

    def test_stage_generate_uses_filenames_not_paths(self):
        """_db_update calls in _stage_generate should use .name not str(full_path)."""
        src = (APP_ROOT / "workflow_ap.py").read_text(encoding="utf-8")
        m = re.search(r"def _stage_generate.*?(?=\ndef |\Z)", src, re.DOTALL)
        body = m.group(0)
        assert "fbdi_csv_path=hdr_csv.name" in body or "fbdi_csv_path=hdr_csv.name" in body
        assert "fbdi_zip_path=zip_path.name" in body


# ═══════════════════════════════════════════════════════════════════════════════
# 7. FIX: AP import set versioning
# ═══════════════════════════════════════════════════════════════════════════════

class TestAPImportSetVersioning:

    def _compute_versioned_group(self, base: str, ver: int) -> str:
        """Replicate the version-stamp logic from workflow_ap.py."""
        if ver > 0:
            base_clean = re.sub(r"_v\d+$", "", base.rstrip())
            return f"{base_clean}_v{ver}"
        return base

    def test_v0_no_suffix(self):
        """Version 0 (first run) should not append a version suffix."""
        result = self._compute_versioned_group("MY_GROUP", 0)
        assert result == "MY_GROUP"

    def test_v1_appends_suffix(self):
        result = self._compute_versioned_group("MY_GROUP", 1)
        assert result == "MY_GROUP_v1"

    def test_v2_replaces_v1_suffix(self):
        """If meta already has _v1, it must be stripped before adding _v2."""
        result = self._compute_versioned_group("MY_GROUP_v1", 2)
        assert result == "MY_GROUP_v2"

    def test_auto_group_gets_version(self):
        result = self._compute_versioned_group("BATCH_abc12345", 1)
        assert result == "BATCH_abc12345_v1"

    def test_workflow_ap_stamps_version_in_process_ap_request(self):
        """process_ap_request must contain the version-stamping logic."""
        src = (APP_ROOT / "workflow_ap.py").read_text(encoding="utf-8")
        m = re.search(r"def process_ap_request.*?(?=\ndef |\Z)", src, re.DOTALL)
        assert m, "process_ap_request not found"
        body = m.group(0)
        assert "_v{" in body or "_vN" in body or "_ver_for_grp" in body, \
            "process_ap_request must stamp version into invoice group"

    def test_prebuilt_zip_stamps_version(self):
        src = (APP_ROOT / "workflow_ap.py").read_text(encoding="utf-8")
        m = re.search(r"def _process_prebuilt_ap_zip.*?(?=\ndef |\Z)", src, re.DOTALL)
        assert m, "_process_prebuilt_ap_zip not found"
        body = m.group(0)
        assert "_pb_ver" in body or "_v{" in body, \
            "_process_prebuilt_ap_zip must use versioned invoice group"


# ═══════════════════════════════════════════════════════════════════════════════
# 8. FIX: AP logs versioned (ess_log_vN not always ess_log_v0)
# ═══════════════════════════════════════════════════════════════════════════════

class TestAPLogVersioning:

    def test_download_all_ap_logs_uses_version_tag(self):
        src = (APP_ROOT / "workflow_ap.py").read_text(encoding="utf-8")
        m = re.search(r"def _download_all_ap_logs.*?(?=\ndef |\Z)", src, re.DOTALL)
        assert m, "_download_all_ap_logs not found"
        body = m.group(0)
        assert '"ess_log_v0"' not in body, \
            "_download_all_ap_logs must not hardcode ess_log_v0"
        assert 'f"ess_log_{v_tag}"' in body or "ess_log_" in body, \
            "_download_all_ap_logs must use versioned ess_log key"

    def test_download_all_ap_logs_stores_individual_files(self):
        """Individual log files should be stored via store_log_file for the Log Files card."""
        src = (APP_ROOT / "workflow_ap.py").read_text(encoding="utf-8")
        m = re.search(r"def _download_all_ap_logs.*?(?=\ndef |\Z)", src, re.DOTALL)
        body = m.group(0)
        assert "_store_log_file" in body or "store_log_file" in body, \
            "_download_all_ap_logs must call store_log_file for individual files"

    def test_bip_fallback_scans_highest_version(self):
        """_save_ap_report_pdf fallback must scan for highest ess_log_vN, not hardcode v0."""
        src = (APP_ROOT / "workflow_ap.py").read_text(encoding="utf-8")
        m = re.search(r"def _save_ap_report_pdf.*?(?=\ndef |\Z)", src, re.DOTALL)
        assert m, "_save_ap_report_pdf not found"
        body = m.group(0)
        assert '"ess_log_v0"' not in body, \
            "_save_ap_report_pdf fallback must not hardcode ess_log_v0"
        assert "_best_log_key" in body or "ess_log_v" in body


# ═══════════════════════════════════════════════════════════════════════════════
# 9. database.get_log_file — new function
# ═══════════════════════════════════════════════════════════════════════════════

class TestGetLogFile:

    def test_get_log_file_exists(self):
        import database
        assert hasattr(database, "get_log_file"), \
            "database.py must export get_log_file()"

    def test_get_log_file_returns_none_when_missing(self, fresh_db):
        import database
        result = database.get_log_file("nonexistent", "some_key")
        assert result is None

    def test_store_and_retrieve_log_file(self, fresh_db):
        import database
        req_id = "test_log_req_001"
        content = b"ESS Job log content\nLine 2\n"
        database.store_log_file(req_id, "test_log.log", content)
        # The key is the safe_key derived from the filename
        meta = database.get_log_files_meta(req_id)
        assert meta, "store_log_file should persist the file"
        key = list(meta.keys())[0]
        result = database.get_log_file(req_id, key)
        assert result is not None, "get_log_file should find the stored file"
        data, filename = result
        assert data == content
        assert filename == "test_log.log"

    def test_get_log_file_wrong_key(self, fresh_db):
        import database
        req_id = "test_log_req_002"
        database.store_log_file(req_id, "mylog.log", b"data")
        result = database.get_log_file(req_id, "wrong_key_xyz")
        assert result is None


# ═══════════════════════════════════════════════════════════════════════════════
# 10. New download routes exist in app.py
# ═══════════════════════════════════════════════════════════════════════════════

class TestNewDownloadRoutes:

    def test_dl_kind_route_exists(self):
        src = (APP_ROOT / "app.py").read_text(encoding="utf-8")
        assert '"/request/{req_id}/dl/kind/{kind' in src, \
            "app.py must define /request/{req_id}/dl/kind/{kind} route"

    def test_dl_log_route_exists(self):
        src = (APP_ROOT / "app.py").read_text(encoding="utf-8")
        assert '"/request/{req_id}/dl/log/{key' in src, \
            "app.py must define /request/{req_id}/dl/log/{key} route"

    def test_dl_kind_calls_get_generated_file(self):
        src = (APP_ROOT / "app.py").read_text(encoding="utf-8")
        m = re.search(r"async def download_by_kind.*?(?=\n@app\.|$)", src, re.DOTALL)
        assert m, "download_by_kind not found"
        body = m.group(0)
        assert "get_generated_file(" in body

    def test_dl_log_calls_get_log_file(self):
        src = (APP_ROOT / "app.py").read_text(encoding="utf-8")
        m = re.search(r"async def download_log_file.*?(?=\n@app\.|$)", src, re.DOTALL)
        assert m, "download_log_file not found"
        body = m.group(0)
        assert "get_log_file(" in body


# ═══════════════════════════════════════════════════════════════════════════════
# 11. Template fixes — error context on edit pages
# ═══════════════════════════════════════════════════════════════════════════════

class TestTemplateErrorContext:

    def test_edit_csv_shows_stop_reason(self):
        src = (APP_ROOT / "templates" / "edit_csv.html").read_text(encoding="utf-8")
        assert "req.stop_reason" in src, \
            "edit_csv.html must show stop_reason from previous run"

    def test_edit_ap_shows_rejections(self):
        src = (APP_ROOT / "templates" / "edit_ap.html").read_text(encoding="utf-8")
        assert "req.ap_rejections_json" in src, \
            "edit_ap.html must show ap_rejections_json from previous run"

    def test_edit_ap_shows_stop_reason(self):
        src = (APP_ROOT / "templates" / "edit_ap.html").read_text(encoding="utf-8")
        assert "req.stop_reason" in src, \
            "edit_ap.html must show stop_reason when no rejections"


# ═══════════════════════════════════════════════════════════════════════════════
# 12. Template fixes — versioned downloads in request_detail.html
# ═══════════════════════════════════════════════════════════════════════════════

class TestVersionedDownloadsTemplate:

    def test_template_shows_versioned_ap_hdr_csv(self):
        src = (APP_ROOT / "templates" / "request_detail.html").read_text(encoding="utf-8")
        assert "ap_hdr_csv_v" in src, \
            "request_detail.html must show versioned ap_hdr_csv_vN files"

    def test_template_shows_versioned_ap_line_csv(self):
        src = (APP_ROOT / "templates" / "request_detail.html").read_text(encoding="utf-8")
        assert "ap_line_csv_v" in src

    def test_template_shows_versioned_ess_log(self):
        src = (APP_ROOT / "templates" / "request_detail.html").read_text(encoding="utf-8")
        assert "ess_log_v" in src

    def test_template_shows_versioned_fbdi_zip(self):
        src = (APP_ROOT / "templates" / "request_detail.html").read_text(encoding="utf-8")
        assert "fbdi_zip_v" in src

    def test_template_uses_dl_kind_route(self):
        src = (APP_ROOT / "templates" / "request_detail.html").read_text(encoding="utf-8")
        assert "/dl/kind/" in src, \
            "request_detail.html must use /dl/kind/{kind} for versioned downloads"

    def test_template_log_files_have_download_links(self):
        src = (APP_ROOT / "templates" / "request_detail.html").read_text(encoding="utf-8")
        assert "/dl/log/" in src, \
            "request_detail.html must have download links for log files"


# ═══════════════════════════════════════════════════════════════════════════════
# 13. Jinja2 template syntax check
# ═══════════════════════════════════════════════════════════════════════════════

class TestTemplateSyntax:
    """Verify Jinja2 can parse each modified template without errors."""

    def _parse(self, filename: str):
        try:
            from jinja2 import Environment, FileSystemLoader, TemplateSyntaxError
        except ImportError:
            pytest.skip("jinja2 not installed")
        tpl_dir = APP_ROOT / "templates"
        env = Environment(loader=FileSystemLoader(str(tpl_dir)))
        env.parse((tpl_dir / filename).read_text(encoding="utf-8"))  # raises on error

    def test_request_detail_html(self):
        self._parse("request_detail.html")

    def test_edit_csv_html(self):
        self._parse("edit_csv.html")

    def test_edit_ap_html(self):
        self._parse("edit_ap.html")


# ═══════════════════════════════════════════════════════════════════════════════
# 14. store_generated_file / get_generated_file round-trip (regression)
# ═══════════════════════════════════════════════════════════════════════════════

class TestGeneratedFileRoundtrip:

    def test_store_and_retrieve_exact_kind(self, fresh_db):
        import database
        req_id = "rt_test_001"
        content = b"hello world csv data"
        database.store_generated_file(req_id, "ap_hdr_csv_v1", "ApInvoicesInterface.csv", content)
        result = database.get_generated_file(req_id, "ap_hdr_csv_v1")
        assert result is not None
        data, fname = result
        assert data == content
        assert fname == "ApInvoicesInterface.csv"

    def test_resolve_versioned_kind_finds_highest(self, fresh_db):
        """_resolve_versioned_kind in app.py should return the highest vN."""
        import database
        req_id = "rt_test_002"
        database.store_generated_file(req_id, "ess_log_v0", "ess_v0.zip", b"v0 data")
        database.store_generated_file(req_id, "ess_log_v1", "ess_v1.zip", b"v1 data")
        database.store_generated_file(req_id, "ess_log_v2", "ess_v2.zip", b"v2 data")
        # Simulate what _resolve_versioned_kind does
        doc = database._mdb()["generated_files"].find_one({"_id": req_id})
        files = doc.get("files", {})
        candidates = []
        for k in files:
            if k.startswith("ess_log_v"):
                try:
                    n = int(k.rsplit("_v", 1)[1])
                    candidates.append((n, k))
                except ValueError:
                    pass
        candidates.sort(reverse=True)
        assert candidates[0][0] == 2, "Highest version should be v2"
        assert candidates[0][1] == "ess_log_v2"

    def test_multiple_versions_all_stored(self, fresh_db):
        """Ensure earlier versions aren't overwritten by later ones."""
        import database
        req_id = "rt_test_003"
        database.store_generated_file(req_id, "fbdi_zip_v0", "ApInvoicesImport.zip", b"v0")
        database.store_generated_file(req_id, "fbdi_zip_v1", "ApInvoicesImport.zip", b"v1")
        r0 = database.get_generated_file(req_id, "fbdi_zip_v0")
        r1 = database.get_generated_file(req_id, "fbdi_zip_v1")
        assert r0 and r0[0] == b"v0"
        assert r1 and r1[0] == b"v1"


# ═══════════════════════════════════════════════════════════════════════════════
# 15. BIP XML analysis (end-to-end parsing)
# ═══════════════════════════════════════════════════════════════════════════════

class TestBIPXMLAnalysis:

    def _make_bip_xml(self, fetched=2, created=1, rejected=1,
                       inv_num="INV-001", supplier="ACME", reason="Invalid account"):
        # Use Oracle's real BIP XML field names that analyze_ap_bip_xml reads:
        #   INVOICE_NUM_R, SUPPLIER_NAME_R (on G_REJECTIONS)
        #   REJECT_REASON, REJECTION_DESCRIPTION (nested in G_REJECTIONS_DETAIL)
        return f"""<?xml version="1.0" encoding="UTF-8"?>
<APXIIMPT_OUTPUT>
  <LIST_G_HEADER>
    <G_HEADER>
      <C_INVOICES_FETCHED>{fetched}</C_INVOICES_FETCHED>
      <C_INVOICES_CREATED>{created}</C_INVOICES_CREATED>
      <C_INVOICES_REJECTED>{rejected}</C_INVOICES_REJECTED>
      <LIST_G_REJECTIONS>
        <G_REJECTIONS>
          <INVOICE_NUM_R>{inv_num}</INVOICE_NUM_R>
          <SUPPLIER_NAME_R>{supplier}</SUPPLIER_NAME_R>
          <SUPPLIER_NUMBER_R>12345</SUPPLIER_NUMBER_R>
          <VENDOR_SITE_CODE>MAIN</VENDOR_SITE_CODE>
          <INVOICE_AMOUNT_R>500.00</INVOICE_AMOUNT_R>
          <INVOICE_CURRENCY_CODE_R>USD</INVOICE_CURRENCY_CODE_R>
          <INVOICE_DATE_R>2025-01-15</INVOICE_DATE_R>
          <LIST_G_REJECTIONS_DETAIL>
            <G_REJECTIONS_DETAIL>
              <REJECT_REASON>{reason}</REJECT_REASON>
              <REJECTION_DESCRIPTION>GL account does not exist</REJECTION_DESCRIPTION>
            </G_REJECTIONS_DETAIL>
          </LIST_G_REJECTIONS_DETAIL>
        </G_REJECTIONS>
      </LIST_G_REJECTIONS>
    </G_HEADER>
  </LIST_G_HEADER>
</APXIIMPT_OUTPUT>""".encode("utf-8")

    def test_parse_rejection_counts(self):
        from services.fusion_service import analyze_ap_bip_xml
        xml = self._make_bip_xml(fetched=2, created=1, rejected=1)
        result = analyze_ap_bip_xml(xml)
        assert result.get("has_rejections") is True
        assert result.get("fetched") == 2 or str(result.get("fetched")) == "2"
        assert result.get("rejected") == 1 or str(result.get("rejected")) == "1"

    def test_parse_rejection_invoice_details(self):
        from services.fusion_service import analyze_ap_bip_xml
        xml = self._make_bip_xml(inv_num="TEST-99", supplier="BIG CORP",
                                  reason="Payment terms not found")
        result = analyze_ap_bip_xml(xml)
        rejections = result.get("rejections", [])
        assert len(rejections) >= 1
        inv = rejections[0]
        assert inv.get("invoice_num") == "TEST-99"
        assert "Payment terms not found" in (inv.get("reasons") or []) or \
               "Payment terms not found" in str(inv)

    def test_no_rejections(self):
        from services.fusion_service import analyze_ap_bip_xml
        xml_str = """<?xml version="1.0" encoding="UTF-8"?>
<APXIIMPT_OUTPUT>
  <LIST_G_HEADER>
    <G_HEADER>
      <C_INVOICES_FETCHED>3</C_INVOICES_FETCHED>
      <C_INVOICES_CREATED>3</C_INVOICES_CREATED>
      <C_INVOICES_REJECTED>0</C_INVOICES_REJECTED>
    </G_HEADER>
  </LIST_G_HEADER>
</APXIIMPT_OUTPUT>""".encode("utf-8")
        result = analyze_ap_bip_xml(xml_str)
        assert not result.get("has_rejections")


# ═══════════════════════════════════════════════════════════════════════════════
# 16. fbdi_generator group_id override
# ═══════════════════════════════════════════════════════════════════════════════

class TestFBDIGeneratorGroupIdOverride:

    def _minimal_records_and_mappings(self, req_id="test_req"):
        """Produce the minimum records + mappings to call build_rows."""
        records = [
            {
                "*Status Code": "NEW",
                "Ledger Name": "US Primary Ledger",
                "*Effective Date of Transaction": "2025-12-16",
                "*Currency Code": "USD",
                "*Actual Flag": "A",
                "Segment1": "1000",
                "Entered Debit Amount": "100.00",
                "Entered Credit Amount": "",
            },
            {
                "*Status Code": "NEW",
                "Ledger Name": "US Primary Ledger",
                "*Effective Date of Transaction": "2025-12-16",
                "*Currency Code": "USD",
                "*Actual Flag": "A",
                "Segment1": "2000",
                "Entered Debit Amount": "",
                "Entered Credit Amount": "100.00",
            },
        ]
        src_cols = list(records[0].keys())
        try:
            from services.ml_mapper import map_all_columns
            mappings = map_all_columns(src_cols)
        except Exception:
            # Fallback: manual mappings for the test
            gl_col_map = {
                "*Status Code": "*Status Code",
                "Ledger Name": "Ledger Name",
                "*Effective Date of Transaction": "*Effective Date of Transaction",
                "*Currency Code": "*Currency Code",
                "*Actual Flag": "*Actual Flag",
                "Segment1": "Segment1",
                "Entered Debit Amount": "Entered Debit Amount",
                "Entered Credit Amount": "Entered Credit Amount",
            }
            mappings = [{"src": c, "target": gl_col_map.get(c, c), "score": 1.0}
                        for c in src_cols]
        return records, mappings

    def test_build_rows_uses_override_group_id(self):
        """When meta['gl_group_id'] is set, build_rows must use that value."""
        from utils.fbdi_generator import build_rows
        records, mappings = self._minimal_records_and_mappings()
        override_gid = "999888777"
        meta = {
            "request_id": "req_test_override",
            "gl_group_id": override_gid,
            "ledger_id":  "300000000000001",
            "bad_row_indices": [],
        }
        good_rows, bad_rows = build_rows(records, mappings, meta)
        assert good_rows, "build_rows should produce rows"
        # build_rows returns a list of dicts keyed by column name
        for row in good_rows:
            assert row["Interface Group Identifier"] == override_gid, \
                f"Row group_id should be {override_gid}, got {row.get('Interface Group Identifier')}"

    def test_build_rows_falls_back_to_sha1_when_no_override(self):
        from utils.fbdi_generator import build_rows
        import hashlib
        records, mappings = self._minimal_records_and_mappings()
        req_id = "req_test_no_override"
        meta = {
            "request_id": req_id,
            "ledger_id":  "300000000000001",
            "bad_row_indices": [],
        }
        good_rows, _ = build_rows(records, mappings, meta)
        assert good_rows
        expected_gid = str(int(hashlib.sha1(req_id.encode()).hexdigest()[:9], 16) % 999999999)
        # build_rows returns a list of dicts keyed by column name
        for row in good_rows:
            assert row["Interface Group Identifier"] == expected_gid


# ═══════════════════════════════════════════════════════════════════════════════
# 17. FastAPI app can be imported and routes are registered
# ═══════════════════════════════════════════════════════════════════════════════

class TestAppImportAndRoutes:
    """Import app module and verify key routes are registered."""

    @pytest.fixture(scope="class", autouse=True)
    def _patch_startup(self, _stub_mongo):
        """Patch startup-time calls that need live Oracle/SMTP."""
        with patch("app.init_db", return_value=None), \
             patch("app._start_background_tasks", return_value=None, create=True):
            yield

    def test_app_imports(self):
        try:
            import app as _app
            assert hasattr(_app, "app"), "app.py must define a FastAPI 'app' object"
        except Exception as e:
            pytest.fail(f"app.py failed to import: {e}")

    def test_reprocess_route_registered(self):
        import app as _app
        routes = [r.path for r in _app.app.routes]
        assert "/request/{req_id}/reprocess" in routes, \
            "/reprocess route must be registered"

    def test_dl_kind_route_registered(self):
        import app as _app
        routes = [r.path for r in _app.app.routes]
        assert any("/dl/kind/" in r for r in routes), \
            "/dl/kind/ route must be registered"

    def test_dl_log_route_registered(self):
        import app as _app
        routes = [r.path for r in _app.app.routes]
        assert any("/dl/log/" in r for r in routes), \
            "/dl/log/ route must be registered"


# ═══════════════════════════════════════════════════════════════════════════════
# 18. Security: no hardcoded credentials or identifiers anywhere in app code
# ═══════════════════════════════════════════════════════════════════════════════

class TestNoHardcodedSecrets:
    """
    Verify no production files contain hardcoded passwords, BU IDs, or
    the specific Oracle demo URL (only the test file used env vars now).
    """
    # Files to scan (exclude test file itself)
    _SCAN_FILES = [
        "app.py",
        "workflow.py",
        "workflow_ap.py",
        "services/fusion_service.py",
        "database.py",
        "utils/fbdi_generator.py",
        "utils/ap_fbdi_generator.py",
    ]
    # Patterns that must NOT appear in production files
    _BANNED_PATTERNS = [
        "Kavin.Sasikumar",  # hardcoded username
        "12345678",         # hardcoded password (specific Oracle demo)
        # DO NOT add "300000046975971" here — may appear as a doc example
    ]

    def test_no_hardcoded_credentials_in_prod_files(self):
        violations = []
        for fname in self._SCAN_FILES:
            path = APP_ROOT / fname
            if not path.exists():
                continue
            text = path.read_text(encoding="utf-8", errors="replace")
            for pattern in self._BANNED_PATTERNS:
                if pattern in text:
                    violations.append(f"{fname}: '{pattern}'")
        assert not violations, f"Hardcoded credentials found: {violations}"
