#!/usr/bin/env python3
"""Tests for _check_bus_publish_pattern — legacy + drift guards.

The legacy guard blocks hand-built NATS envelopes. The drift guard blocks
LLM attempts to call `send_bus_message` from inside the execute_code
sandbox (import or bare call). `send_bus_message` is deliberately absent
from SANDBOX_ALLOWED_TOOLS — it is a direct Hermes tool, invoked at the
turn level, never from sandboxed Python.
"""

from __future__ import annotations

import os

os.environ.setdefault("TERMINAL_ENV", "local")

import pytest

from tools.code_execution_tool import _check_bus_publish_pattern


LEGACY_REJECTIONS = [
    pytest.param(
        'msg = {"type": "message", "from": "HERMES", "to": "HALL9000"}',
        id="legacy-envelope-type-field",
    ),
    pytest.param(
        'await nc.publish("agent.bus", b"hi")',
        id="legacy-nc-publish-agent-bus",
    ),
    pytest.param(
        'await js.publish("agent.bus.hall9000", payload)',
        id="legacy-js-publish-agent-bus",
    ),
    pytest.param(
        'to = "CODEX"',
        id="legacy-peer-name-codex",
    ),
    pytest.param(
        "to = 'HERMES-VPS'",
        id="legacy-peer-name-hermes-vps",
    ),
]

DRIFT_REJECTIONS = [
    pytest.param(
        "from hermes_tools import send_bus_message",
        id="drift-import-send-bus-message",
    ),
    pytest.param(
        "from hermes_tools import read_file, send_bus_message",
        id="drift-import-multi-with-send-bus-message",
    ),
    pytest.param(
        "import hermes_tools\nhermes_tools.send_bus_message(to='HALL9000')",
        id="drift-module-import-and-attr-call",
    ),
    pytest.param(
        "send_bus_message(to='HALL9000', body='[tarea] test')",
        id="drift-bare-call",
    ),
    pytest.param(
        "result = send_bus_message(\n    to='PI',\n    body='[opinión] test',\n)",
        id="drift-bare-call-multiline",
    ),
]

PASSING_CODE = [
    pytest.param(
        "from hermes_tools import read_file\ncontent = read_file('/tmp/x.txt')",
        id="ok-legit-sandbox-import",
    ),
    pytest.param(
        'result = terminal(command="ls /tmp")',
        id="ok-terminal-tool-call",
    ),
    pytest.param(
        "# comment mentioning send_bus_message is fine outside a call\nx = 1",
        id="ok-comment-with-symbol",
    ),
    pytest.param(
        'note = "the docs describe send_bus_message but we dont call it"',
        id="ok-string-literal-with-symbol",
    ),
    pytest.param(
        'import hermes_tools_other_module\nx = 1',
        id="ok-different-module-named-hermes-tools-prefix",
    ),
    pytest.param(
        'obj.send_bus_message_like_method()',
        id="ok-unrelated-method-with-similar-name",
    ),
]


@pytest.mark.parametrize("code", LEGACY_REJECTIONS)
def test_legacy_patterns_rejected(code: str) -> None:
    err = _check_bus_publish_pattern(code)
    assert err is not None, f"expected rejection, got pass for: {code!r}"
    assert "bus-publish guard" in err
    assert "send_bus_message" in err


@pytest.mark.parametrize("code", DRIFT_REJECTIONS)
def test_drift_patterns_rejected(code: str) -> None:
    err = _check_bus_publish_pattern(code)
    assert err is not None, f"expected rejection, got pass for: {code!r}"
    assert "bus-drift guard" in err
    assert "DIRECT Hermes tool call" in err
    assert "execute_code sandbox" in err


@pytest.mark.parametrize("code", PASSING_CODE)
def test_legitimate_code_passes(code: str) -> None:
    err = _check_bus_publish_pattern(code)
    assert err is None, f"expected pass, got rejection: {err!r} for code: {code!r}"


def test_drift_hint_mentions_valid_peers() -> None:
    err = _check_bus_publish_pattern("send_bus_message(to='HALL9000')")
    assert err is not None
    for peer in ("HALL9000", "PI", "HERMES", "ALL"):
        assert peer in err, f"expected valid peer {peer!r} in hint, got: {err!r}"


def test_legacy_guard_runs_before_drift_guard() -> None:
    code = 'send_bus_message(to="CODEX")'
    err = _check_bus_publish_pattern(code)
    assert err is not None
    assert "bus-publish guard" in err
    assert "CODEX" in err
