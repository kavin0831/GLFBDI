"""Unit tests for workflow._stage_normalize and _parse_date_any."""
from __future__ import annotations

import pytest


def test_parse_date_any_common_formats():
    from workflow import _parse_date_any
    cases = {
        "16/12/2025":  "2025/12/16",
        "12-16-2025":  "2025/12/16",
        "2025-12-16":  "2025/12/16",
        "16-Dec-2025": "2025/12/16",
        "2025/12/16":  "2025/12/16",
    }
    for inp, expected in cases.items():
        assert _parse_date_any(inp) == expected, f"{inp!r} → {_parse_date_any(inp)!r}, expected {expected!r}"


def test_parse_date_any_handles_garbage():
    from workflow import _parse_date_any
    assert _parse_date_any("not a date") is None
    assert _parse_date_any("") is None


def test_normalize_dates(fresh_db):
    from workflow import _stage_normalize
    records = [
        {"Accounting Date": "16/12/2025", "Amount": "100"},
        {"Accounting Date": "16-Dec-2025", "Amount": "200"},
    ]
    cols = ["Accounting Date", "Amount"]
    out = _stage_normalize("test-req-1", records, cols)
    assert out[0]["Accounting Date"] == "2025/12/16"
    assert out[1]["Accounting Date"] == "2025/12/16"


def test_normalize_yn_flags(fresh_db):
    from workflow import _stage_normalize
    records = [
        {"Actual Flag": "true"},
        {"Actual Flag": "0"},
        {"Actual Flag": "yes"},
        {"Actual Flag": "no"},
        {"Actual Flag": "1"},
    ]
    cols = ["Actual Flag"]
    out = _stage_normalize("test-req-yn", records, cols)
    assert out[0]["Actual Flag"] == "Y"
    assert out[1]["Actual Flag"] == "N"
    assert out[2]["Actual Flag"] == "Y"
    assert out[3]["Actual Flag"] == "N"
    assert out[4]["Actual Flag"] == "Y"


def test_normalize_thousand_separators(fresh_db):
    from workflow import _stage_normalize
    records = [
        {"Entered Debit Amount": "1,000.50"},
        {"Entered Debit Amount": "1,000,000.00"},
        {"Entered Debit Amount": "100.50"},  # untouched
    ]
    cols = ["Entered Debit Amount"]
    out = _stage_normalize("test-req-amt", records, cols)
    assert out[0]["Entered Debit Amount"] == "1000.50"
    assert out[1]["Entered Debit Amount"] == "1000000.00"
    assert out[2]["Entered Debit Amount"] == "100.50"


def test_normalize_currency_uppercase(fresh_db):
    from workflow import _stage_normalize
    records = [{"Currency Code": "usd"}, {"Currency Code": "inr"}]
    cols = ["Currency Code"]
    out = _stage_normalize("test-req-ccy", records, cols)
    assert out[0]["Currency Code"] == "USD"
    assert out[1]["Currency Code"] == "INR"


def test_normalize_whitespace(fresh_db):
    from workflow import _stage_normalize
    records = [{"Segment1": "  1000  "}, {"Segment1": "  ABC"}]
    cols = ["Segment1"]
    out = _stage_normalize("test-req-ws", records, cols)
    assert out[0]["Segment1"] == "1000"
    assert out[1]["Segment1"] == "ABC"
