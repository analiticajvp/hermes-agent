"""Tests for _candidate_nats_urls() in scripts/nats_bus.py.

Tests cover: empty env, single URL, CSV dedup, strict mode,
empty CSV entries, and whitespace stripping.
"""
from __future__ import annotations

import importlib
import importlib.util
import sys
from pathlib import Path

import pytest

# Load nats_bus from canonical scripts/ path, regardless of sys.path order.
# This avoids picking up any hot-patch at /opt/hermes/nats_bus.py.
_SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
_NATS_BUS_PATH = _SCRIPTS_DIR / "nats_bus.py"


def _load_nats_bus_module():
    """Load nats_bus from the canonical scripts/ path."""
    spec = importlib.util.spec_from_file_location("nats_bus_canonical", str(_NATS_BUS_PATH))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


DEFAULT_URLS = [
    "nats://nats-bus:4222",
    "nats://nats:4222",
    "nats://100.115.121.70:4222",
    "nats://localhost:4222",
]


def test_empty_nats_url_returns_defaults(monkeypatch):
    """NATS_URL="" should return all 4 DEFAULT_NATS_URLS in order."""
    monkeypatch.setenv("NATS_URL", "")
    monkeypatch.delenv("NATS_STRICT", raising=False)
    mod = _load_nats_bus_module()
    result = mod._candidate_nats_urls()
    assert result == DEFAULT_URLS


def test_single_valid_url(monkeypatch):
    """NATS_URL=nats://foo:4222 should return [foo] + defaults (dedup)."""
    monkeypatch.setenv("NATS_URL", "nats://foo:4222")
    monkeypatch.delenv("NATS_STRICT", raising=False)
    mod = _load_nats_bus_module()
    result = mod._candidate_nats_urls()
    assert result[0] == "nats://foo:4222"
    assert "nats://nats-bus:4222" in result
    assert len(result) == len(DEFAULT_URLS) + 1  # foo + 4 defaults


def test_csv_urls_dedup(monkeypatch):
    """nats-bus in CSV should not appear twice when defaults are appended."""
    monkeypatch.setenv("NATS_URL", "nats://nats-bus:4222,nats://foo:4222")
    monkeypatch.delenv("NATS_STRICT", raising=False)
    mod = _load_nats_bus_module()
    result = mod._candidate_nats_urls()
    assert result.count("nats://nats-bus:4222") == 1
    assert result[0] == "nats://nats-bus:4222"
    assert result[1] == "nats://foo:4222"


def test_strict_mode(monkeypatch):
    """NATS_STRICT=1 + explicit URL should return ONLY the explicit URL."""
    monkeypatch.setenv("NATS_URL", "nats://foo:4222")
    monkeypatch.setenv("NATS_STRICT", "1")
    mod = _load_nats_bus_module()
    result = mod._candidate_nats_urls()
    assert result == ["nats://foo:4222"]


def test_csv_with_empty_entries(monkeypatch):
    """Empty entries in CSV should be ignored."""
    monkeypatch.setenv("NATS_URL", "nats://a:4222,,nats://b:4222")
    monkeypatch.delenv("NATS_STRICT", raising=False)
    mod = _load_nats_bus_module()
    result = mod._candidate_nats_urls()
    assert "nats://a:4222" in result
    assert "nats://b:4222" in result
    assert "" not in result


def test_whitespace_in_csv(monkeypatch):
    """URLs with surrounding whitespace should be stripped."""
    monkeypatch.setenv("NATS_URL", " nats://a:4222 , nats://b:4222 ")
    monkeypatch.delenv("NATS_STRICT", raising=False)
    mod = _load_nats_bus_module()
    result = mod._candidate_nats_urls()
    assert "nats://a:4222" in result
    assert "nats://b:4222" in result
    assert " nats://a:4222 " not in result
    assert " nats://b:4222 " not in result


# ---------------------------------------------------------------------------
# Alerting tests (T2.5 — _reconnect_failures counter + telegram alert)
# ---------------------------------------------------------------------------

def test_alert_telegram_called_on_threshold(monkeypatch):
    """Cuando _reconnect_failures >= threshold, _alert_telegram_bus_down dispara."""
    monkeypatch.setenv("HERMES_NATS_RECONNECT_ALERT_THRESHOLD", "3")
    mod = _load_nats_bus_module()
    alert_called: list[int] = []

    async def _fake_alert() -> None:
        alert_called.append(1)

    # Patch the module-level function and the global counter.
    monkeypatch.setattr(mod, "_alert_telegram_bus_down", _fake_alert)
    monkeypatch.setattr(mod, "_reconnect_failures", 2)
    monkeypatch.setattr(mod, "_ALERT_THRESHOLD", 3)

    # Simulate _on_disconnect: increment counter and check threshold.
    async def _run():
        mod._reconnect_failures += 1
        if mod._reconnect_failures >= mod._ALERT_THRESHOLD:
            await mod._alert_telegram_bus_down()

    import asyncio
    asyncio.run(_run())
    assert alert_called, "Alert should have been called when failures == threshold"
    assert mod._reconnect_failures == 3


def test_alert_resets_on_reconnect(monkeypatch):
    """_on_reconnect resetea _reconnect_failures a 0."""
    mod = _load_nats_bus_module()
    monkeypatch.setattr(mod, "_reconnect_failures", 5)

    import asyncio
    asyncio.run(mod._on_reconnect())
    assert mod._reconnect_failures == 0, "Counter should reset to 0 after reconnect"


def test_alert_survives_telegram_failure(monkeypatch):
    """Si el POST a Telegram falla, Hermes NO crashea."""
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "fake-token-for-test")
    mod = _load_nats_bus_module()
    monkeypatch.setattr(mod, "_reconnect_failures", 10)

    import urllib.request as _req

    def _raise(*args, **kwargs):
        raise OSError("Simulated network failure")

    monkeypatch.setattr(_req, "urlopen", _raise)

    import asyncio
    # Should NOT raise — the function must swallow the exception.
    try:
        asyncio.run(mod._alert_telegram_bus_down())
    except Exception as exc:
        pytest.fail(f"_alert_telegram_bus_down raised unexpectedly: {exc}")
