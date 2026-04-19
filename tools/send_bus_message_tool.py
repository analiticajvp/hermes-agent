#!/usr/bin/env python3
"""send_bus_message tool — canonical NATS agent.bus publisher for Hermes.

Replaces ad-hoc `code_execution` scripts that the LLM used to write by
hand to publish to NATS. Those scripts produced legacy envelopes
(`{"type": "message", "from": ..., "to": ..., "body": ...}`) that HAL's
schema validator drops. This tool routes every outbound message through
`scripts/nats_bus.py::publish` which builds the canonical envelope
(`from_agent`, `to`, `body`, `task_id`, `task_type`, `task_payload`,
`msg_id`, `ts`) and passes peer-enum and length validation.

Peers are a closed set: HALL9000, PI, HERMES, ALL. The LLM MUST NOT
invent new peer names (CODEX and HERMES-VPS are legacy, not canonical).
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

CANONICAL_PEERS: frozenset[str] = frozenset({"HALL9000", "PI", "HERMES", "ALL"})
_SCRIPTS_DIR = "/opt/hermes/scripts"


def _ensure_scripts_on_path() -> None:
    if _SCRIPTS_DIR not in sys.path:
        sys.path.insert(0, _SCRIPTS_DIR)


def _load_publish() -> Any:
    _ensure_scripts_on_path()
    from nats_bus import publish as _publish  # type: ignore[import]
    return _publish


def _run_coro(coro: Any) -> None:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        asyncio.run(coro)
        return
    loop.create_task(coro)


def _validate_inputs(to: str, body: str) -> str | None:
    if not isinstance(to, str) or to not in CANONICAL_PEERS:
        return (
            f"send_bus_message failed: 'to' must be one of "
            f"{sorted(CANONICAL_PEERS)}, got {to!r}. "
            "Use PI (not CODEX) and HERMES (not HERMES-VPS)."
        )
    if not isinstance(body, str) or not body.strip():
        return "send_bus_message failed: 'body' must be a non-empty string."
    return None


def _invoke_publish(to: str, body: str, task_id: str | None,
                    task_type: str | None, task_payload: dict | None) -> None:
    publish = _load_publish()
    _run_coro(publish(body=body, to=to, task_id=task_id,
                      task_type=task_type, task_payload=task_payload))


def send_bus_message(
    to: str,
    body: str,
    task_id: str | None = None,
    task_type: str | None = None,
    task_payload: dict | None = None,
) -> str:
    """Publish a canonical envelope to agent.bus. Returns a status string."""
    err = _validate_inputs(to, body)
    if err:
        logger.warning(err)
        return err
    try:
        _invoke_publish(to, body, task_id, task_type, task_payload)
    except Exception as exc:  # noqa: BLE001 — report, don't crash the agent
        msg = f"send_bus_message failed to publish: {exc}"
        logger.error(msg)
        return msg
    summary = f"ok: published to {to}"
    if task_id:
        summary += f" (task_id={task_id})"
    logger.info("send_bus_message %s body=%s", summary, body[:80])
    return summary


def check_send_bus_message_requirements() -> bool:
    try:
        _load_publish()
        return True
    except Exception as exc:  # noqa: BLE001
        logger.warning("send_bus_message unavailable: %s", exc)
        return False


SEND_BUS_MESSAGE_SCHEMA: dict = {
    "name": "send_bus_message",
    "description": (
        "NATIVE TOP-LEVEL TOOL. Invoke this directly as a tool call, "
        "NOT from inside execute_code or terminal. Do NOT do "
        "`from hermes_tools import send_bus_message` — that will fail "
        "because this tool is not exposed inside the execute_code "
        "sandbox. Call it at the tool-call level, just like you call "
        "\"terminal\" or \"execute_code\" themselves. "
        "Publish a message to the inter-agent NATS bus `agent.bus` "
        "using the canonical envelope. This is the ONLY supported way "
        "to communicate with HALL9000 or PI from Hermes. Never hand-"
        "build a JSON envelope via code_execution — it will be "
        "rejected by HAL's schema validator. Peer names are fixed: "
        "HALL9000, PI, HERMES, ALL (PI is NOT called CODEX, HERMES is "
        "NOT called HERMES-VPS)."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "to": {
                "type": "string",
                "enum": ["HALL9000", "PI", "HERMES", "ALL"],
                "description": (
                    "Target agent. Use HALL9000 for the Mac-side "
                    "agent, PI for the Pi-bridge / Codex CLI, HERMES "
                    "for another Hermes instance, ALL to broadcast."
                ),
            },
            "body": {
                "type": "string",
                "description": (
                    "Message body. If routing a user request, prefix "
                    "the intent tag: `[tarea]` for actions, "
                    "`[chequeo]` for status probes, `[opinion]` for "
                    "feedback, `[auditoria]` for deep reviews."
                ),
            },
            "task_id": {
                "type": "string",
                "description": (
                    "Optional correlation ID. Include when you expect "
                    "a reply you'll match later; omit for fire-and-"
                    "forget."
                ),
            },
            "task_type": {
                "type": "string",
                "description": (
                    "Optional structured task type (e.g. check_ovh, "
                    "check_status, request_feedback, execute_task). "
                    "Leave empty if intent tag in body is enough."
                ),
            },
            "task_payload": {
                "type": "object",
                "description": (
                    "Optional JSON payload carrying structured task "
                    "arguments (e.g. question, topic, context)."
                ),
            },
        },
        "required": ["to", "body"],
    },
}


from tools.registry import registry  # noqa: E402

registry.register(
    name="send_bus_message",
    toolset="agent_bus",
    schema=SEND_BUS_MESSAGE_SCHEMA,
    handler=lambda args, **kw: send_bus_message(
        to=args.get("to", ""),
        body=args.get("body", ""),
        task_id=args.get("task_id"),
        task_type=args.get("task_type"),
        task_payload=args.get("task_payload"),
    ),
    check_fn=check_send_bus_message_requirements,
    emoji="\U0001f4e1",
)
