"""Tests for get_conversion_rate — Oracle REST first, hardcoded fallback."""
from __future__ import annotations

import pytest
import respx
import httpx


def _reset_cache():
    from services import fusion_service as fs
    fs._RATE_CACHE.clear()


def test_usd_usd_returns_1_no_network():
    from services.fusion_service import get_conversion_rate
    _reset_cache()

    class Cfg: fusion_url = "https://fusion.example.com"; fusion_username = "u"; fusion_password = "p"
    # No respx mock → if it tried to call out it would fail; verifies no call
    assert get_conversion_rate(Cfg(), "USD", "USD", "2025-12-16") == 1.0


@respx.mock
def test_rest_success_returns_rate():
    from services.fusion_service import get_conversion_rate

    _reset_cache()

    class Cfg:
        fusion_url = "https://fusion.example.com"
        fusion_username = "u"
        fusion_password = "p"

    respx.get("https://fusion.example.com/fscmRestApi/resources/11.13.18.05/currencyRates").mock(
        return_value=httpx.Response(200, json={"items": [{"ConversionRate": 83.5}]})
    )
    rate = get_conversion_rate(Cfg(), "USD", "INR", "2025-12-16")
    assert rate == 83.5


@respx.mock
def test_rest_404_falls_back_to_table():
    from services.fusion_service import get_conversion_rate, _FX_FALLBACK
    _reset_cache()

    class Cfg:
        fusion_url = "https://fusion.example.com"
        fusion_username = "u"
        fusion_password = "p"

    respx.get("https://fusion.example.com/fscmRestApi/resources/11.13.18.05/currencyRates").mock(
        return_value=httpx.Response(404)
    )
    rate = get_conversion_rate(Cfg(), "USD", "INR", "2025-12-16")
    assert rate == _FX_FALLBACK[("USD", "INR")]


@respx.mock
def test_rest_empty_items_falls_back():
    from services.fusion_service import get_conversion_rate, _FX_FALLBACK
    _reset_cache()

    class Cfg:
        fusion_url = "https://fusion.example.com"
        fusion_username = "u"
        fusion_password = "p"

    respx.get("https://fusion.example.com/fscmRestApi/resources/11.13.18.05/currencyRates").mock(
        return_value=httpx.Response(200, json={"items": []})
    )
    rate = get_conversion_rate(Cfg(), "USD", "EUR", "2025-12-16")
    assert rate == _FX_FALLBACK[("USD", "EUR")]


@respx.mock
def test_cache_hit_no_second_call():
    from services.fusion_service import get_conversion_rate
    _reset_cache()

    class Cfg:
        fusion_url = "https://fusion.example.com"
        fusion_username = "u"
        fusion_password = "p"

    route = respx.get(
        "https://fusion.example.com/fscmRestApi/resources/11.13.18.05/currencyRates"
    ).mock(return_value=httpx.Response(200, json={"items": [{"ConversionRate": 83.0}]}))

    r1 = get_conversion_rate(Cfg(), "USD", "INR", "2025-12-16")
    r2 = get_conversion_rate(Cfg(), "USD", "INR", "2025-12-16")
    assert r1 == r2 == 83.0
    assert route.call_count == 1, f"Expected 1 REST call, got {route.call_count}"


def test_inverse_fallback():
    from services.fusion_service import get_conversion_rate, _FX_FALLBACK
    _reset_cache()

    class Cfg:
        fusion_url = "http://no-net.invalid"  # guaranteed to fail
        fusion_username = "u"
        fusion_password = "p"

    # JPY→USD isn't in the table, but USD→JPY is; inverse should be used.
    rate = get_conversion_rate(Cfg(), "JPY", "USD", "2025-12-16")
    assert abs(rate - 1.0 / _FX_FALLBACK[("USD", "JPY")]) < 1e-9
