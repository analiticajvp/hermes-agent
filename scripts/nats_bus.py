"""NATS adapter for Hermes — async pub/sub on agent.bus.

Functional style, no classes. Uses nats-py for
async NATS communication over Tailscale network.

Usage:
    from hermes.nats_bus import publish, start_listener, should_auto_ack

    await publish("Audit complete", to="HALL9000", task_id="task-001")
    await start_listener(on_message=my_handler)

    # In an auto-reply listener:
    # if should_auto_ack(msg):
    #     await publish("Recibido", to=msg["from_agent"], task_id=msg.get("task_id"))
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from collections.abc import Callable
from typing import Any

import nats
from nats.aio.client import Client as NatsClient

log = logging.getLogger("hermes.nats_bus")

DEFAULT_NATS_URLS: tuple[str, ...] = (
    "nats://nats-bus:4222",
    "nats://nats:4222",
    "nats://100.115.121.70:4222",
    "nats://localhost:4222",
)
NATS_URL: str = os.environ.get("NATS_URL", DEFAULT_NATS_URLS[0])
SELF_NAME: str = os.environ.get("NATS_SELF", "HERMES_LOCAL_DISABLED")
SUBJECT: str = os.environ.get("NATS_SUBJECT", "agent.bus")
PEER_ALLOWLIST: set[str] = {
    peer.strip() for peer in os.environ.get("HERMES_NATS_PEER_ALLOWLIST", "PI,HALL9000,HAL9000").split(",") if peer.strip()
}
MAX_BODY_CHARS: int = int(os.environ.get("HERMES_NATS_MAX_BODY_CHARS", "1200"))
DEFAULT_WAIT_TIMEOUT_S: float = float(os.environ.get("HERMES_NATS_WAIT_TIMEOUT_S", "15"))

_client: NatsClient | None = None
_reconnect_failures: int = 0
_ALERT_THRESHOLD: int = int(os.environ.get("HERMES_NATS_RECONNECT_ALERT_THRESHOLD", "5"))


def _candidate_nats_urls() -> list[str]:
    raw = os.environ.get("NATS_URL", "").strip()
    strict = os.environ.get("NATS_STRICT", "").strip() == "1"
    explicit = [item.strip() for item in raw.split(",") if item.strip()] if raw else []
    if strict and explicit:
        return explicit
    seen: set[str] = set()
    ordered: list[str] = []
    for url in [*explicit, *DEFAULT_NATS_URLS]:
        if url and url not in seen:
            seen.add(url)
            ordered.append(url)
    return ordered


async def _connect_first_available() -> tuple[NatsClient, str]:
    last_error: Exception | None = None
    for candidate in _candidate_nats_urls():
        try:
            client = await nats.connect(
                candidate,
                max_reconnect_attempts=-1,
                reconnect_time_wait=2,
                error_cb=_on_error,
                disconnected_cb=_on_disconnect,
                reconnected_cb=_on_reconnect,
            )
            return client, candidate
        except Exception as exc:
            last_error = exc
            log.warning("NATS unreachable via %s: %s", candidate, exc)
    if last_error is not None:
        raise last_error
    raise RuntimeError("No hay candidates NATS configurados")


async def _get_client() -> NatsClient:
    """Get or create a persistent NATS connection."""
    global _client
    if _client and _client.is_connected:
        return _client
    _client, connected_url = await _connect_first_available()
    log.info("Connected to NATS at %s as %s", connected_url, SELF_NAME)
    return _client


async def _alert_telegram_bus_down() -> None:
    """Send Telegram alert via Bot API — bypasses NATS intentionally."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("HERMES_NATS_ALERT_CHAT_ID", "8386273656")
    if not token:
        log.warning("HERMES_NATS_ALERT: TELEGRAM_BOT_TOKEN not set, skipping alert")
        return
    api_url = f"https://api.telegram.org/bot{token}/sendMessage"
    text = (
        f"❗ Hermes NATS bus down — {_reconnect_failures} fallos consecutivos. "
        f"URLs: {_candidate_nats_urls()}"
    )
    try:
        import urllib.request as _req
        import json as _json
        data = _json.dumps({"chat_id": chat_id, "text": text}).encode()
        with _req.urlopen(_req.Request(api_url, data=data, headers={"Content-Type": "application/json"}), timeout=5):
            pass
        log.info("Telegram NATS alert sent to chat_id=%s", chat_id)
    except Exception as exc:
        log.error("Telegram NATS alert FAILED (non-fatal): %s", exc)


async def _on_error(exc: Exception) -> None:
    log.error("NATS error: %s", exc)


async def _on_disconnect() -> None:
    global _reconnect_failures
    _reconnect_failures += 1
    log.warning("NATS disconnected (consecutive_failures=%d)", _reconnect_failures)
    if _reconnect_failures >= _ALERT_THRESHOLD:
        await _alert_telegram_bus_down()


async def _on_reconnect() -> None:
    global _reconnect_failures
    if _reconnect_failures > 0:
        log.info("NATS reconnected after %d consecutive failures", _reconnect_failures)
    _reconnect_failures = 0


def _validate_publish_request(
    body: str,
    to: str,
    task_id: str | None,
    task_type: str | None,
) -> None:
    if to not in PEER_ALLOWLIST and to != "ALL":
        raise ValueError(f"Peer no permitido: {to}")
    if not body or len(body) > MAX_BODY_CHARS:
        raise ValueError(f"Body inválido o demasiado largo ({len(body)}/{MAX_BODY_CHARS})")
    if task_type and not task_id:
        raise ValueError("task_id es obligatorio cuando hay task_type")



def _build_envelope(
    body: str,
    to: str = "ALL",
    task_id: str | None = None,
    task_type: str | None = None,
    task_payload: dict[str, Any] | None = None,
) -> bytes:
    """Build a JSON message envelope."""

    envelope = {
        "from_agent": SELF_NAME,
        "body": body,
        "to": to,
        "task_id": task_id,
        "task_type": task_type,
        "task_payload": task_payload,
        "msg_id": str(uuid.uuid4())[:8],
        "ts": time.time(),
    }
    return json.dumps(envelope, ensure_ascii=False).encode()


def _parse_envelope(raw: bytes) -> dict[str, Any] | None:
    """Parse raw NATS payload. Returns None on failure."""
    try:
        return json.loads(raw.decode())
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


def _is_for_me(msg: dict[str, Any]) -> bool:
    """Check if message targets HERMES or ALL."""
    return msg.get("to") in (SELF_NAME, "ALL")


def _is_echo(msg: dict[str, Any]) -> bool:
    """Check if message was sent by me."""
    return msg.get("from_agent") == SELF_NAME


def normalize_ack_text(text: str | None) -> str:
    """Normalize ACK-like text for loop detection."""
    if text is None:
        return ""
    normalized = text.strip().lower()
    while normalized.startswith("[") and "]" in normalized:
        normalized = normalized.split("]", 1)[1].strip()
    normalized = normalized.rstrip(".!¡!").strip()
    return " ".join(normalized.split())


def has_control_prefix(text: str | None) -> bool:
    """Return True for result/final control messages that should not be ACKed."""
    raw = (text or "").strip().lower()
    return raw.startswith("[result]") or raw.startswith("[done]")


def is_ack_like(text: str | None) -> bool:
    """Return True for trivial ACK bodies that must not auto-reply."""
    normalized = normalize_ack_text(text)
    return normalized in {"recibido", "ack"}


def should_auto_ack(msg: dict[str, Any]) -> bool:
    """Guard for Hermes auto-reply listeners.

    Auto-ACK is allowed only for non-echo, addressed messages
    whose body is not already a trivial ACK.
    """
    if _is_echo(msg):
        return False
    if not _is_for_me(msg):
        return False
    if has_control_prefix(msg.get("body")):
        return False
    return not is_ack_like(msg.get("body"))


def _publish_log_payload(
    body: str,
    to: str,
    task_id: str | None,
    task_type: str | None,
) -> tuple[object, ...]:
    return SUBJECT, to, task_id, task_type, body[:80]


async def publish(
    body: str,
    to: str = "ALL",
    task_id: str | None = None,
    task_type: str | None = None,
    task_payload: dict[str, Any] | None = None,
) -> None:
    """Publish a message to agent.bus."""
    _validate_publish_request(body, to, task_id, task_type)
    nc = await _get_client()
    payload = _build_envelope(
        body,
        to=to,
        task_id=task_id,
        task_type=task_type,
        task_payload=task_payload,
    )
    await nc.publish(SUBJECT, payload)
    log.info("Published to %s: to=%s task=%s type=%s body=%s", *_publish_log_payload(body, to, task_id, task_type))


def _matches_waiting_message(msg: dict[str, Any], to: str, task_id: str) -> bool:
    if msg.get("from_agent") != to:
        return False
    if msg.get("to") not in (SELF_NAME, "ALL"):
        return False
    return msg.get("task_id") == task_id


async def _queue_matching_reply(
    raw_msg: Any,
    queue: asyncio.Queue[dict[str, Any]],
    to: str,
    task_id: str,
) -> None:
    msg = _parse_envelope(raw_msg.data)
    if msg is None or not _matches_waiting_message(msg, to, task_id):
        return
    await queue.put(msg)


async def _subscribe_for_replies(
    nc: NatsClient,
    queue: asyncio.Queue[dict[str, Any]],
    to: str,
    task_id: str,
):
    async def _handler(raw_msg: Any) -> None:
        await _queue_matching_reply(raw_msg, queue, to, task_id)

    return await nc.subscribe(SUBJECT, cb=_handler)


def _collect_reply_state(
    msg: dict[str, Any],
    ack_body: str | None,
    result_body: str | None,
) -> tuple[str | None, str | None, bool]:
    incoming_body = str(msg.get("body", ""))
    if is_ack_like(incoming_body) and ack_body is None:
        return incoming_body, result_body, False
    if has_control_prefix(incoming_body) or result_body is None:
        return ack_body, incoming_body, has_control_prefix(incoming_body)
    return ack_body, result_body, False


async def _wait_for_reply_bodies(
    queue: asyncio.Queue[dict[str, Any]],
    timeout_s: float,
) -> tuple[str | None, str | None]:
    ack_body: str | None = None
    result_body: str | None = None
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        try:
            msg = await asyncio.wait_for(queue.get(), timeout=max(0.1, remaining))
        except asyncio.TimeoutError:
            break
        ack_body, result_body, done = _collect_reply_state(msg, ack_body, result_body)
        if done:
            break
    return ack_body, result_body


def _reply_summary(task_id: str, ack_body: str | None, result_body: str | None) -> dict[str, Any]:
    return {
        "task_id": task_id,
        "ack_received": ack_body is not None,
        "result_received": result_body is not None,
        "reply_body": result_body or ack_body,
        "timeout": ack_body is None and result_body is None,
    }


async def send_and_wait_bus_message(
    to: str,
    body: str,
    task_id: str,
    timeout_s: float = DEFAULT_WAIT_TIMEOUT_S,
    task_type: str | None = None,
    task_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Publish to agent.bus and wait for ACK/RESULT correlated by task_id."""
    _validate_publish_request(body, to, task_id, task_type)
    nc = await _get_client()
    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    sub = await _subscribe_for_replies(nc, queue, to, task_id)
    try:
        await publish(body, to=to, task_id=task_id, task_type=task_type, task_payload=task_payload)
        ack_body, result_body = await _wait_for_reply_bodies(queue, timeout_s)
        return _reply_summary(task_id, ack_body, result_body)
    finally:
        await sub.unsubscribe()


def _should_dispatch_message(msg: dict[str, Any] | None) -> bool:
    if msg is None:
        return False
    if _is_echo(msg):
        return False
    return _is_for_me(msg)


async def _dispatch_listener_message(
    raw_msg: Any,
    on_message: Callable[[dict[str, Any]], None],
) -> None:
    msg = _parse_envelope(raw_msg.data)
    if msg is None:
        log.warning("Unparseable message: %s", raw_msg.data[:100])
        return
    if not _should_dispatch_message(msg):
        return
    log.info(
        "Received: from=%s to=%s task=%s body=%s",
        msg.get("from_agent"),
        msg.get("to"),
        msg.get("task_id"),
        msg.get("body", "")[:80],
    )
    on_message(msg)


async def _subscribe_listener(nc: NatsClient, on_message: Callable[[dict[str, Any]], None]) -> None:
    async def _handler(raw_msg: Any) -> None:
        await _dispatch_listener_message(raw_msg, on_message)

    await nc.subscribe(SUBJECT, cb=_handler)
    log.info("Subscribed to %s, listening...", SUBJECT)


async def start_listener(
    on_message: Callable[[dict[str, Any]], None],
) -> None:
    """Subscribe to agent.bus and dispatch messages.

    Blocks until the connection is closed. Call from
    Hermes main loop via asyncio.create_task().

    on_message receives a parsed dict with keys:
    from_agent, body, to, task_id, msg_id, ts.
    """
    nc = await _get_client()
    await _subscribe_listener(nc, on_message)
    while nc.is_connected:
        await asyncio.sleep(1)


async def close() -> None:
    """Drain and close the NATS connection."""
    global _client
    if _client and _client.is_connected:
        await _client.drain()
        _client = None
        log.info("NATS connection closed")
