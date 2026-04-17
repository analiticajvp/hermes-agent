"""Tests for reconnect counter alerting in scripts/nats_bus.py.

Tests cover: counter increment, threshold triggering alert,
counter reset on reconnect, and alert surviving Telegram failures.
"""
from __future__ import annotations

import asyncio
import importlib.util
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
_NATS_BUS_PATH = _SCRIPTS_DIR / "nats_bus.py"


def _load_fresh_module(monkeypatch, *, threshold: int = 3) -> object:
    """Load nats_bus from scripts/ with a fresh module state.

    Sets HERMES_NATS_RECONNECT_ALERT_THRESHOLD to `threshold`
    for predictable test behaviour.
    """
    monkeypatch.setenv("HERMES_NATS_RECONNECT_ALERT_THRESHOLD", str(threshold))
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "fake-token-123")
    monkeypatch.setenv("HERMES_NATS_ALERT_CHAT_ID", "99999")
    spec = importlib.util.spec_from_file_location("nats_bus_test_alert", str(_NATS_BUS_PATH))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.asyncio
async def test_alert_telegram_called_on_threshold(monkeypatch):
    """After N failures, _alert_telegram_bus_down must be called."""
    mod = _load_fresh_module(monkeypatch, threshold=3)

    alert_called: list[bool] = []

    async def fake_alert() -> None:
        alert_called.append(True)

    # Reset module state
    mod._reconnect_failures = 0
    mod._alert_telegram_bus_down = fake_alert

    # Simulate 3 disconnects
    for _ in range(3):
        await mod._on_disconnect()

    assert len(alert_called) >= 1, "Alert should have been called at threshold"


@pytest.mark.asyncio
async def test_alert_resets_on_reconnect_success(monkeypatch):
    """Reconnect success must reset the failure counter to 0."""
    mod = _load_fresh_module(monkeypatch, threshold=3)

    async def fake_alert() -> None:
        pass

    mod._reconnect_failures = 2
    mod._alert_telegram_bus_down = fake_alert

    await mod._on_disconnect()  # counter -> 3, alert called (via fake)
    assert mod._reconnect_failures == 3

    await mod._on_reconnect()
    assert mod._reconnect_failures == 0, "Counter must reset after reconnect"


@pytest.mark.asyncio
async def test_alert_survives_telegram_failure(monkeypatch):
    """If Telegram HTTP call fails, Hermes must NOT crash."""
    mod = _load_fresh_module(monkeypatch, threshold=1)

    # Simulate urllib failing
    original_alert = mod._alert_telegram_bus_down

    async def failing_alert() -> None:
        # Simulate the HTTP call failing by calling the real function
        # but with urllib patched to raise
        import urllib.request as _req
        original_urlopen = _req.urlopen

        def boom(*args, **kwargs):
            raise OSError("Simulated network failure")

        _req.urlopen = boom
        try:
            await original_alert()
        finally:
            _req.urlopen = original_urlopen

    mod._reconnect_failures = 0
    mod._alert_telegram_bus_down = failing_alert

    # This must not raise
    await mod._on_disconnect()  # counter -> 1, triggers alert (which fails internally)
    # If we get here, the test passes — no crash
    assert mod._reconnect_failures == 1
