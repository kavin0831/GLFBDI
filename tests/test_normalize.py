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
    records = [{"Segment3": "  1000  "}, {"Segment3": "  ABC"}]
    cols = ["Segment3"]
    out = _stage_normalize("test-req-ws", records, cols)
    # "1000" already 4 digits, ABC ignored (non-numeric)
    assert out[0]["Segment3"] == "1000"
    assert out[1]["Segment3"] == "ABC"


def test_normalize_segment_pads_short_to_column_max(fresh_db):
    """User typed '0' but other rows have '121' / '500' → pad short values
    to the column's max numeric width so Oracle COA accepts them."""
    from workflow import _stage_normalize
    records = [
        {"Segment5": "0",   "Segment6": "000"},
        {"Segment5": "121", "Segment6": "000"},
        {"Segment5": "500", "Segment6": "0"},
    ]
    cols = ["Segment5", "Segment6"]
    out = _stage_normalize("seg-pad-1", records, cols)
    # Segment5 max width = 3 ('121','500') → pad '0' to '000'
    assert out[0]["Segment5"] == "000"
    assert out[1]["Segment5"] == "121"
    assert out[2]["Segment5"] == "500"
    # Segment6 all digits, max width = 3 → '0' stays '000'
    assert out[0]["Segment6"] == "000"
    assert out[2]["Segment6"] == "000"


def test_normalize_segment_min_width_three(fresh_db):
    """Single-digit-only column still pads to 3 (common COA width)."""
    from workflow import _stage_normalize
    records = [{"Segment4": "0"}, {"Segment4": "5"}]
    cols = ["Segment4"]
    out = _stage_normalize("seg-pad-2", records, cols)
    assert out[0]["Segment4"] == "000"
    assert out[1]["Segment4"] == "005"


def test_normalize_segment_preserves_non_numeric(fresh_db):
    """A column containing letters/dashes is left alone — not all COAs are numeric."""
    from workflow import _stage_normalize
    records = [{"Segment3": "AB"}, {"Segment3": "CD-1"}]
    cols = ["Segment3"]
    out = _stage_normalize("seg-pad-3", records, cols)
    assert out[0]["Segment3"] == "AB"
    assert out[1]["Segment3"] == "CD-1"


def test_normalize_segment_skips_segment1_and_segment2(fresh_db):
    """Segment1 (Company) and Segment2 (Balancing) are typically 2-digit codes
    in Oracle COAs — padding '10' → '010' there would corrupt valid data.
    The auto-pad only applies from Segment3 onwards."""
    from workflow import _stage_normalize
    records = [
        {"Segment1": "10", "Segment2": "1",  "Segment3": "0"},
        {"Segment1": "10", "Segment2": "1",  "Segment3": "121"},
        {"Segment1": "5",  "Segment2": "10", "Segment3": "500"},
    ]
    cols = ["Segment1", "Segment2", "Segment3"]
    out = _stage_normalize("seg-pad-skip12", records, cols)
    # Segment1 / Segment2 untouched — verbatim
    assert out[0]["Segment1"] == "10"
    assert out[1]["Segment1"] == "10"
    assert out[2]["Segment1"] == "5"
    assert out[0]["Segment2"] == "1"
    assert out[1]["Segment2"] == "1"
    assert out[2]["Segment2"] == "10"
    # Segment3 still padded normally
    assert out[0]["Segment3"] == "000"
    assert out[1]["Segment3"] == "121"
    assert out[2]["Segment3"] == "500"
