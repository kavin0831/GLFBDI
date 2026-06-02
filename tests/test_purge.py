"""Tests for purge_interface_rows — silent best-effort purge."""
from __future__ import annotations

import respx
import httpx


class Cfg:
    fusion_url = "https://fusion.example.com"
    fusion_username = "u"
    fusion_password = "p"


@respx.mock
def test_purge_success_201():
    from services.fusion_service import purge_interface_rows
    route = respx.post(
        "https://fusion.example.com/ess/rest/scheduler/v1/requests"
    ).mock(return_value=httpx.Response(201, json={"requestId": "12345"}))

    ok = purge_interface_rows(Cfg(), group_id="999", ledger_id="111")
    assert ok is True
    assert route.called


@respx.mock
def test_purge_failure_404_returns_false_no_raise():
    from services.fusion_service import purge_interface_rows
    respx.post(
        "https://fusion.example.com/ess/rest/scheduler/v1/requests"
    ).mock(return_value=httpx.Response(404, text="not found"))

    # All variants will hit 404 → should silently return False
    ok = purge_interface_rows(Cfg(), group_id="999")
    assert ok is False


@respx.mock
def test_purge_empty_group_id_returns_false():
    from services.fusion_service import purge_interface_rows
    # No URL mock — confirms it doesn't even try if group_id is empty
    ok = purge_interface_rows(Cfg(), group_id="")
    assert ok is False


@respx.mock
def test_purge_network_error_returns_false():
    from services.fusion_service import purge_interface_rows
    respx.post(
        "https://fusion.example.com/ess/rest/scheduler/v1/requests"
    ).mock(side_effect=httpx.ConnectError("simulated"))

    ok = purge_interface_rows(Cfg(), group_id="999")
    assert ok is False
