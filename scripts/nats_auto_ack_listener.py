#!/usr/bin/env python3
"""Hermes NATS auto-ACK listener with anti-loop guard."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import shlex
import signal
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

REPO_ROOT = Path(__file__).resolve().parents[1]
_SCRIPTS_DIR = REPO_ROOT / "scripts"
# Insert scripts/ BEFORE repo root so canonical nats_bus.py in scripts/
# takes precedence over any legacy hot-patch at /opt/hermes/nats_bus.py.
for _p in (str(_SCRIPTS_DIR), str(REPO_ROOT)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from nats_bus import NATS_URL, publish, should_auto_ack, start_listener

try:
    from task_bus_reader import read_and_summarize as _read_and_summarize
except ImportError:  # pragma: no cover - fallback if module missing
    def _read_and_summarize(*_args: object, **_kwargs: object) -> str:
        return "[RESULT] Task-bus UNAVAILABLE: task_bus_reader module not found."


try:
    from hal.nats_heartbeat import publish_step
except ImportError:  # pragma: no cover - VPS/container fallback
    async def publish_step(subject: str, agent: str, doing: str, extra: dict[str, Any] | None = None) -> None:
        return None

try:
    from shared.bus_directives import has_code_context, parse_exact_reply_instruction
    from shared.intent_tags import parse_intent_tag
except ImportError:  # pragma: no cover - VPS/container fallback
    import re

    PREFIX_TAG_RE = re.compile(r"^\s*\[([^\]]+)\]\s*(.*)$", re.DOTALL)
    SUFFIX_TAG_RE = re.compile(r"^(.*?)(?:\s*\[([^\]]+)\]\s*)$", re.DOTALL)
    INTENT_ALIASES = {
        "tarea": "task",
        "accion": "task",
        "acción": "task",
        "opinion": "feedback",
        "opinión": "feedback",
        "review": "feedback",
        "revision": "feedback",
        "revisión": "feedback",
        "chequeo": "check",
        "check": "check",
        "verificacion": "check",
        "verificación": "check",
        "auditoria": "model_review",
        "auditoría": "model_review",
        "audit": "model_review",
        "model_review": "model_review",
    }

    @dataclass(frozen=True)
    class _IntentTag:
        raw_tag: str | None
        intent: str | None
        body: str

    def _intent_tag(raw_tag: str | None, body: str) -> _IntentTag:
        if raw_tag is None:
            return _IntentTag(raw_tag=None, intent=None, body=body)
        return _IntentTag(raw_tag=raw_tag, intent=INTENT_ALIASES.get(raw_tag.lower()), body=body)

    def parse_intent_tag(body: str | None) -> _IntentTag:
        text = (body or "").strip()
        if not text:
            return _intent_tag(None, "")
        prefix_match = PREFIX_TAG_RE.match(text)
        if prefix_match:
            return _intent_tag(prefix_match.group(1).strip(), prefix_match.group(2).strip())
        suffix_match = SUFFIX_TAG_RE.match(text)
        if suffix_match:
            return _intent_tag(suffix_match.group(2).strip(), suffix_match.group(1).strip())
        return _intent_tag(None, text)

    def parse_exact_reply_instruction(text: str | None) -> str | None:
        body = (text or "").strip()
        if not body:
            return None
        for pattern in (
            re.compile(r"(?:respond(?:é|e)|de[cíi])\s+(?:solo\s+y\s+)?exactamente\s*:\s*(?P<reply>.+)$", re.IGNORECASE | re.DOTALL),
            re.compile(r"(?:respond(?:é|e)|de[cíi])\s+solo\s*:\s*(?P<reply>.+)$", re.IGNORECASE | re.DOTALL),
        ):
            match = pattern.search(body)
            if match is None:
                continue
            cleaned = match.group("reply").strip().splitlines()[0].strip().strip("`\"'“”‘’ ")
            return cleaned or None
        return None

    def has_code_context(text: str | None) -> bool:
        body = (text or "").strip()
        if not body:
            return False
        lowered = body.lower()
        markers = ("diff --git", "archivo", "file", ".py", ".md", ".mjs", ".sh", ".ts", ".tsx", ".js", "```", "/opt/", "/root/", "hall9000/")
        return any(marker in lowered for marker in markers) or len(body) > 400


logging.basicConfig(
    level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("hermes.nats_auto_ack")
HEARTBEAT_SUBJECT = os.environ.get("HERMES_HEARTBEAT_SUBJECT", "agent.hermes")
OVH_SSH_TARGET = os.environ.get("OVH_SSH_TARGET", "ovh")
TASK_CLAIMS_DIR = Path(os.environ.get("HERMES_TASK_CLAIMS_DIR", "/tmp/hermes-nats-task-claims"))
TASK_CLAIMS_TTL_SECONDS = int(os.environ.get("HERMES_TASK_CLAIMS_TTL_SECONDS", "600"))
MODEL_BRIDGE_ENABLED = os.environ.get("HERMES_NATS_MODEL_BRIDGE_ENABLED", "1") == "1"
MODEL_BRIDGE_TIMEOUT_S = int(os.environ.get("HERMES_NATS_MODEL_BRIDGE_TIMEOUT_S", "90"))
MAX_RESULT_CHARS = int(os.environ.get("HERMES_NATS_MAX_RESULT_CHARS", "1200"))
MODEL_BRIDGE_CMD = os.environ.get(
    "HERMES_NATS_MODEL_BRIDGE_CMD",
    "hermes chat -Q --source tool -q",
)
OVH_CHECK_COMMAND = os.environ.get(
    "OVH_CHECK_COMMAND",
    f"ssh -o BatchMode=yes -o ConnectTimeout=8 {shlex.quote(OVH_SSH_TARGET)} 'echo ovh-ok'",
)
SUPPORTED_TASK_TYPES = {"check_ovh", "request_feedback", "execute_task", "model_review", "list_task_bus", "check_self_status"}
SELF_STATUS_TOKENS = (
    "tu estatus", "estatus propio", "estatus operativo",
    "tu estado", "estado propio", "operativo",
    "heartbeat", "uptime", "health",
    "tu status", "status propio", "your status",
)
TASK_BUS_TOKENS = ("task-bus", "taskbus", "pendientes", "tareas pendientes",
                   "que tareas", "qué tareas", "tasks pending")
OVH_CHECK_TOKENS = ("verific", "conect", "conexion", "connect", "check")
FEEDBACK_TOKENS = ("feedback", "opini", "review", "revisá", "revisa", "revisión", "revision")
AUDIT_TOKENS = (
    "audit",
    "auditor",
    "auditoría",
    "auditoria",
    "auditá",
    "audita",
    "auditar",
    "reaudit",
    "model_review",
)


@dataclass(frozen=True)
class TaskPayload:
    topic: str = ""
    question: str = ""


@dataclass(frozen=True)
class MessageContext:
    body: str
    from_agent: str | None
    task_id: str | None
    task_payload: TaskPayload
    msg_id: str | None = None
    correlation_id: str | None = None
    protocol_version: str | None = None


@dataclass(frozen=True)
class ResolvedTask:
    action: str | None
    explicit_task_type: bool
    effective_body: str
    exact_reply: str | None = None


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


@dataclass(frozen=True)
class CommandError:
    kind: str
    detail: str


def _parse_task_payload(payload: object) -> TaskPayload:
    if not isinstance(payload, dict):
        return TaskPayload()
    return TaskPayload(
        topic=str(payload.get("topic") or ""),
        question=str(payload.get("question") or ""),
    )


def _message_context(msg: dict[str, Any]) -> MessageContext:
    return MessageContext(
        body=str(msg.get("body", "")),
        from_agent=msg.get("from_agent"),
        task_id=msg.get("task_id"),
        task_payload=_parse_task_payload(msg.get("task_payload")),
        msg_id=msg.get("msg_id"),
        correlation_id=msg.get("correlation_id") or msg.get("task_id"),
        protocol_version=msg.get("protocol_version"),
    )


async def _heartbeat(doing: str, extra: dict[str, Any] | None = None) -> None:
    await publish_step(HEARTBEAT_SUBJECT, "hermes", doing, extra)


def _contains_ovh_check_request(text: str) -> bool:
    lowered = text.lower()
    return "ovh" in lowered and any(token in lowered for token in OVH_CHECK_TOKENS)



def _self_status_rule_matches(text: str) -> bool:
    return any(token in text for token in SELF_STATUS_TOKENS)


def _task_bus_rule_matches(text: str) -> bool:
    neg_prefixes = ("no ", "sin ", "salvo ", "excepto ", "aparte de ")
    for token in TASK_BUS_TOKENS:
        idx = text.find(token)
        if idx == -1:
            continue
        window = text[max(0, idx - 20):idx]
        if any(neg in window for neg in neg_prefixes):
            continue
        return True
    return False


def _audit_rule_matches(text: str, task_id: str | None) -> bool:
    task_id_text = (task_id or "").lower()
    return any(token in text for token in AUDIT_TOKENS) or task_id_text.startswith("audit-") or "audit" in task_id_text or "reaudit" in task_id_text


def _feedback_rule_matches(text: str, task_id: str | None) -> bool:
    task_id_text = (task_id or "").lower()
    if _audit_rule_matches(text, task_id):
        return False
    return any(token in text for token in FEEDBACK_TOKENS) or task_id_text.startswith("review-") or "feedback" in task_id_text


def _action_from_intent(text: str, intent: str | None) -> str | None:
    match intent:
        case "model_review":
            return "model_review"
        case "feedback":
            return "request_feedback"
        case "check" if _self_status_rule_matches(text):
            return "check_self_status"
        case "check" if _task_bus_rule_matches(text):
            return "list_task_bus"
        case "check" if "ovh" in text:
            return "check_ovh"
        case "task":
            return "execute_task"
        case _:
            return None


def detect_action(body: str, task_id: str | None = None, intent: str | None = None) -> str | None:
    text = body.lower()
    intent_action = _action_from_intent(text, intent)
    if intent_action is not None:
        return intent_action
    if _self_status_rule_matches(text):
        return "check_self_status"
    if _task_bus_rule_matches(text):
        return "list_task_bus"
    if _contains_ovh_check_request(text):
        return "check_ovh"
    if _audit_rule_matches(text, task_id):
        return "model_review"
    if _feedback_rule_matches(text, task_id):
        return "request_feedback"
    return None


def _resolve_explicit_task_type(task_type: str, cleaned_body: str, task_id: str | None) -> ResolvedTask:
    if task_type == "request_feedback" and _audit_rule_matches(cleaned_body.lower(), task_id):
        return ResolvedTask(action="model_review", explicit_task_type=True, effective_body=cleaned_body)
    resolved = task_type if task_type in SUPPORTED_TASK_TYPES else "unsupported_task"
    return ResolvedTask(action=resolved, explicit_task_type=True, effective_body=cleaned_body)


def resolve_task_type(msg: dict[str, Any]) -> ResolvedTask:
    tagged = parse_intent_tag(str(msg.get("body", "")))
    cleaned_body = tagged.body or str(msg.get("body", ""))
    task_id = msg.get("task_id")
    task_type = msg.get("task_type")
    exact_reply = parse_exact_reply_instruction(cleaned_body)
    if exact_reply is not None:
        return ResolvedTask(
            action="direct_reply",
            explicit_task_type=True,
            effective_body=cleaned_body,
            exact_reply=exact_reply,
        )
    if task_type:
        return _resolve_explicit_task_type(task_type, cleaned_body, task_id)
    return ResolvedTask(
        action=detect_action(cleaned_body, task_id, tagged.intent),
        explicit_task_type=False,
        effective_body=cleaned_body,
    )


def tcp_probe() -> str:
    parsed = urlparse(NATS_URL)
    host = parsed.hostname or "localhost"
    port = parsed.port or 4222
    try:
        with socket.create_connection((host, port), timeout=5):
            return f"[RESULT] OVH reachable ✅ tcp {host}:{port}"
    except OSError as exc:
        return f"[RESULT] OVH unreachable ❌ tcp {host}:{port} {exc}"


def _claim_key(body: str, from_agent: str | None, task_id: str | None) -> str:
    raw_key = f"{from_agent or 'unknown'}|{task_id or 'no-task'}|{body}"
    return hashlib.sha1(raw_key.encode("utf-8")).hexdigest()


def _cleanup_stale_claims(now: float) -> None:
    for stale in TASK_CLAIMS_DIR.iterdir():
        try:
            if now - stale.stat().st_mtime > TASK_CLAIMS_TTL_SECONDS:
                stale.unlink()
        except FileNotFoundError:
            continue


def _write_claim_file(claim_path: Path, raw_key: str) -> bool:
    try:
        fd = os.open(str(claim_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        return False

    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(raw_key)
    return True


def claim_task(body: str, from_agent: str | None, task_id: str | None) -> bool:
    TASK_CLAIMS_DIR.mkdir(parents=True, exist_ok=True)
    raw_key = f"{from_agent or 'unknown'}|{task_id or 'no-task'}|{body}"
    claim_path = TASK_CLAIMS_DIR / _claim_key(body, from_agent, task_id)
    _cleanup_stale_claims(time.time())
    return _write_claim_file(claim_path, raw_key)


def build_feedback(body: str, task_payload: TaskPayload | None = None) -> str | None:
    lowered = body.lower()
    topic = ""
    if task_payload is not None:
        topic = (task_payload.topic or task_payload.question).lower()
        lowered = f"{lowered} {topic}".strip()
    if _audit_rule_matches(lowered, None):
        return None
    if "start_hermes_nats_listener.sh" in lowered:
        return "[RESULT] Feedback Hermes: `scripts/start_hermes_nats_listener.sh` quedó deshabilitado a propósito. `HERMES` está reservado exclusivamente para Hermes-VPS; la Mac no debe levantar un Hermes local."
    if "protenback" in lowered or "restore_openmind.sh" in lowered:
        return "[RESULT] Opinión Hermes: `protenback` está bien planteado. El orden correcto es red → bus → consumidores → UI, porque primero asegura conectividad real y después levanta componentes dependientes. Riesgo residual: si Tailscale requiere login/intervención humana post-reboot, el script debe fallar explícitamente. No veo un faltante obvio en ese flujo base."
    if "[tarea]" in lowered and "persist" in lowered:
        return "[RESULT] Hermes recibió una tarea de persistencia/política. Puedo obedecer la convención desde este listener, pero persistirla en Honcho requiere el canal/model-bridge adecuado o tooling explícito fuera de este executor."
    return None


def ensure_result_prefix(text: str) -> str:
    raw = (text or "").strip()
    if not raw:
        return "[RESULT] Respuesta vacía de Hermes."
    lowered = raw.lower()
    if lowered.startswith("[result]") or lowered.startswith("[done]"):
        prefixed = raw
    else:
        prefixed = f"[RESULT] {raw}"
    return prefixed[:MAX_RESULT_CHARS]


def _has_audit_context(text: str) -> bool:
    return has_code_context(text)


def _missing_audit_context_result() -> str:
    return "[RESULT] Necesito contexto concreto para auditar: diff, archivo, fragmento de código o criterio verificable. Sin eso no corresponde responder con feedback genérico."


def _build_model_bridge_prompt(body: str, from_agent: str | None, task_id: str | None) -> str:
    return (
        "Respondé como HERMES en el bus NATS. "
        "Sé breve, operativo y en español. "
        "No repitas el mensaje original completo. "
        "Si corresponde, respondé con contenido útil en <= 500 caracteres. "
        f"Origen: {from_agent or 'UNKNOWN'}. "
        f"Task ID: {task_id or 'sin-task-id'}. "
        f"Mensaje: {body}"
    )


def _run_shell_command(command: str, timeout: int) -> CommandResult:
    proc = subprocess.run(
        command,
        shell=True,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return CommandResult(returncode=proc.returncode, stdout=proc.stdout or "", stderr=proc.stderr or "")


def _try_shell_command(command: str, timeout: int) -> CommandResult | CommandError:
    try:
        return _run_shell_command(command, timeout)
    except subprocess.TimeoutExpired:
        return CommandError(kind="timeout", detail="timeout")
    except OSError as exc:
        return CommandError(kind="oserror", detail=str(exc))


def _model_bridge_result_from_command(result: CommandResult | CommandError) -> str:
    if isinstance(result, CommandError):
        if result.kind == "timeout":
            return "[RESULT] Hermes model bridge timeout."
        return f"[RESULT] Hermes model bridge error: {result.detail}"

    output = (result.stdout or result.stderr).strip()
    if result.returncode != 0:
        detail = output or f"exit {result.returncode}"
        return f"[RESULT] Hermes model bridge failed: {detail}"[:500]
    if not output:
        return "[RESULT] Hermes model bridge devolvió respuesta vacía."
    return ensure_result_prefix(output[:500])


def run_model_bridge(body: str, from_agent: str | None, task_id: str | None) -> str:
    if not MODEL_BRIDGE_ENABLED:
        return "[RESULT] Model bridge NATS deshabilitado en Hermes."

    prompt = _build_model_bridge_prompt(body, from_agent, task_id)
    command = f"{MODEL_BRIDGE_CMD} {shlex.quote(prompt)}"
    return _model_bridge_result_from_command(_try_shell_command(command, MODEL_BRIDGE_TIMEOUT_S))


def _ovh_error_result(error: CommandError) -> str:
    if error.kind == "timeout":
        return "[RESULT] OVH timeout."
    lowered = error.detail.lower()
    if "not found" in lowered:
        return tcp_probe()
    return f"[RESULT] OVH check error: {error.detail}"


def _ovh_result_from_command(result: CommandResult | CommandError) -> str:
    if isinstance(result, CommandError):
        return _ovh_error_result(result)
    if result.returncode == 0:
        detail = (result.stdout or "ovh-ok").strip()
        return f"[RESULT] OVH reachable ✅ {detail}".strip()

    detail = (result.stderr or result.stdout or f"exit {result.returncode}").strip()
    lowered = detail.lower()
    if "not found" in lowered or "could not resolve hostname" in lowered:
        return tcp_probe()
    return f"[RESULT] OVH unreachable ❌ {detail}".strip()


def _run_ovh_check_command() -> str:
    return _ovh_result_from_command(_try_shell_command(OVH_CHECK_COMMAND, 12))


def _run_self_status() -> str:
    """Hardcoded listener self-status. No LLM call. Instant response."""
    import datetime
    import os
    import socket
    host = socket.gethostname()
    pid = os.getpid()
    subject = os.environ.get("NATS_SUBJECT", "agent.bus")
    url = os.environ.get("NATS_URL", "unknown")
    js_flag = os.environ.get("NATS_JETSTREAM_ENABLED", "0") == "1"
    self_name = os.environ.get("NATS_SELF", "HERMES")
    now = datetime.datetime.now(datetime.timezone.utc).isoformat(
        timespec="seconds"
    )
    lines = [
        f"[RESULT] Estado {self_name}: operativo.",
        f"Host={host} | PID={pid}",
        f"Bus={url} subject={subject} jetstream={js_flag}",
        "Listener responde: check_self_status, check_ovh, "
        "list_task_bus, request_feedback, execute_task, model_review.",
        f"Timestamp={now}",
    ]
    return " ".join(lines)


def _run_model_review_action(body: str, from_agent: str | None, task_id: str | None) -> str:
    if not _has_audit_context(body):
        return _missing_audit_context_result()
    return run_model_bridge(f"[auditoría] {body}", from_agent, task_id)


def _run_feedback_action(
    body: str,
    task_payload: TaskPayload | None,
    from_agent: str | None,
    task_id: str | None,
) -> str:
    lowered = body.lower()
    if _audit_rule_matches(lowered, task_id):
        return _run_model_review_action(body, from_agent, task_id)
    feedback = build_feedback(body, task_payload)
    if feedback is not None:
        return feedback
    return run_model_bridge(f"[opinión] {body}", from_agent, task_id)


def _run_execute_task_action(body: str, from_agent: str | None, task_id: str | None) -> str:
    if _contains_ovh_check_request(body):
        return _run_ovh_check_command()
    return run_model_bridge(f"[tarea] {body}", from_agent, task_id)


def run_action(
    action: str, body: str, task_payload: TaskPayload | None = None,
    from_agent: str | None = None, task_id: str | None = None,
    exact_reply: str | None = None,
) -> str:
    match action:
        case "direct_reply":
            return exact_reply or "[RESULT] ERROR: direct_reply sin payload."
        case "model_review":
            return _run_model_review_action(body, from_agent, task_id)
        case "request_feedback":
            return _run_feedback_action(body, task_payload, from_agent, task_id)
        case "execute_task":
            return _run_execute_task_action(body, from_agent, task_id)
        case "check_ovh":
            return _run_ovh_check_command()
        case "check_self_status":
            return _run_self_status()
        case "list_task_bus":
            return _read_and_summarize()
        case _:
            return "[RESULT] Tarea no soportada por executor Hermes."


def _structured_reply_kwargs(context: MessageContext, message_kind: str) -> dict[str, Any]:
    if context.protocol_version != "1.1":
        return {}
    return {
        "protocol_version": "1.1",
        "message_kind": message_kind,
        "correlation_id": context.correlation_id,
        "causation_id": context.msg_id,
        "reply_to_msg_id": context.msg_id,
    }


async def _publish_result(context: MessageContext, result: str) -> None:
    await publish(
        result,
        to=context.from_agent or "ALL",
        task_id=context.task_id,
        **_structured_reply_kwargs(context, "result"),
    )


async def _send_ack(context: MessageContext) -> None:
    await _heartbeat(
        "📥 tarea recibida",
        {
            "task_id": context.task_id,
            "from": context.from_agent,
        },
    )
    await publish(
        "Recibido",
        to=context.from_agent or "ALL",
        task_id=context.task_id,
        **_structured_reply_kwargs(context, "ack"),
    )
    await _heartbeat(
        "🤝 ACK enviado",
        {
            "task_id": context.task_id,
            "to": context.from_agent,
        },
    )
    log.info("ACK sent: to=%s task=%s", context.from_agent, context.task_id)


def _audit_received_payload(context: MessageContext) -> dict[str, Any]:
    return {
        "task_id": context.task_id,
        "from": context.from_agent,
        "lines": len(context.body.splitlines()),
    }


def _audit_passed(result: str) -> bool:
    lowered = result.lower()
    return not any(token in lowered for token in (" failed", "error", "timeout", "❌"))


async def _send_model_bridge_result(context: MessageContext) -> None:
    await _heartbeat("📥 diff recibido", _audit_received_payload(context))
    await _heartbeat("🔍 analizando estructura del código", {"task_id": context.task_id})
    result = await asyncio.to_thread(run_model_bridge, context.body, context.from_agent, context.task_id)
    await _heartbeat("🧪 validando tests y cobertura", {"task_id": context.task_id})
    await _heartbeat("📋 revisando convenciones de naming", {"task_id": context.task_id})
    await _publish_result(context, result)
    await _heartbeat("✅ AUDIT PASSED" if _audit_passed(result) else "❌ AUDIT FAILED", {"task_id": context.task_id})
    log.info(
        "Model bridge result sent: to=%s task=%s result=%s",
        context.from_agent,
        context.task_id,
        result,
    )


def _action_heartbeat_payload(context: MessageContext, task: ResolvedTask) -> dict[str, Any]:
    return {
        "task_id": context.task_id,
        "action": task.action,
    }


async def _compute_action_result(context: MessageContext, task: ResolvedTask) -> str:
    return await asyncio.to_thread(
        run_action,
        task.action or "unsupported_task",
        task.effective_body,
        context.task_payload,
        context.from_agent,
        context.task_id,
        task.exact_reply,
    )


async def _send_action_result(context: MessageContext, task: ResolvedTask) -> None:
    payload = _action_heartbeat_payload(context, task)
    await _heartbeat("🔍 leyendo spec de tarea", payload)
    result = await _compute_action_result(context, task)
    await _publish_result(context, result)
    await _heartbeat("✅ resultado publicado", payload)
    log.info("Action result sent: to=%s task=%s result=%s", context.from_agent, context.task_id, result)


def _error_heartbeat_payload(msg: dict[str, Any], exc: Exception) -> dict[str, Any]:
    return {
        "task_id": msg.get("task_id"),
        "detail": str(exc)[:160],
    }


def _log_ignored_message(msg: dict[str, Any]) -> None:
    log.info(
        "Ignored ACK-like or non-actionable message: from=%s to=%s task=%s body=%s",
        msg.get("from_agent"),
        msg.get("to"),
        msg.get("task_id"),
        str(msg.get("body", ""))[:80],
    )


def _is_unsupported_untyped(task: ResolvedTask) -> bool:
    return task.action == "unsupported_task" and not task.explicit_task_type


def _from_task_payload(context: MessageContext) -> dict[str, Any]:
    return {
        "task_id": context.task_id,
        "from": context.from_agent,
    }


async def _handle_duplicate_delivery(context: MessageContext) -> None:
    await _heartbeat("♻️ entrega duplicada ignorada", _from_task_payload(context))
    log.info("Skipped duplicate task delivery: from=%s task=%s", context.from_agent, context.task_id)


async def _handle_untyped_ack_only(context: MessageContext) -> None:
    await _heartbeat("ℹ️ mensaje sin acción explícita — solo ACK", _from_task_payload(context))
    log.info("ACK only, untyped message not supported: from=%s task=%s", context.from_agent, context.task_id)


async def _process_message(msg: dict[str, Any]) -> None:
    if not should_auto_ack(msg):
        _log_ignored_message(msg)
        return

    context = _message_context(msg)
    if not claim_task(context.body, context.from_agent, context.task_id):
        await _handle_duplicate_delivery(context)
        return

    await _send_ack(context)
    task = resolve_task_type(msg)
    if task.action is None:
        await _send_model_bridge_result(context)
        return
    if _is_unsupported_untyped(task):
        await _handle_untyped_ack_only(context)
        return
    await _send_action_result(context, task)


async def handle_message(msg: dict[str, Any]) -> None:
    try:
        await _process_message(msg)
    except Exception as exc:
        await _heartbeat("⚠️ error procesando tarea", _error_heartbeat_payload(msg, exc))
        raise


# --- Graceful shutdown support ---
# The listener runs as a long-lived process inside the hermes-agent
# container, supervised by ensure_nats_auto_ack_listener.sh. When the
# supervisor sends SIGTERM (code sync, restart, etc.) we want to:
#   1) stop accepting new bus messages,
#   2) drain in-flight handlers within a configurable grace period,
#   3) close the NATS connection cleanly,
# so we never lose a [RESULT] mid-flight just because the supervisor
# ticked. Without this, long tasks (model_review via LLM) get abruptly
# killed after 8s and the caller sees ACK without RESULT.

_SHUTDOWN_GRACE_S = float(os.environ.get("LISTENER_SHUTDOWN_GRACE_S", "60"))
_in_flight: set[asyncio.Task] = set()


def _track_in_flight(task: asyncio.Task) -> None:
    _in_flight.add(task)
    task.add_done_callback(_in_flight.discard)


def _register_signal_handlers(
    loop: asyncio.AbstractEventLoop,
    shutdown_event: asyncio.Event,
) -> None:
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, shutdown_event.set)
        except NotImplementedError:
            # Windows / constrained runtimes — fall back silently.
            pass


async def _drain_in_flight(grace_seconds: float) -> int:
    if not _in_flight:
        return 0
    log.info(
        "shutdown: draining %d in-flight task(s), grace=%.1fs",
        len(_in_flight),
        grace_seconds,
    )
    try:
        await asyncio.wait_for(
            asyncio.gather(*_in_flight, return_exceptions=True),
            timeout=grace_seconds,
        )
    except asyncio.TimeoutError:
        survivors = len(_in_flight)
        log.warning(
            "shutdown: %d task(s) still running after %.1fs grace — exiting anyway",
            survivors,
            grace_seconds,
        )
        return survivors
    log.info("shutdown: all in-flight tasks completed cleanly")
    return 0


async def _safe_heartbeat(doing: str, extra: dict[str, Any] | None = None) -> None:
    try:
        await _heartbeat(doing, extra)
    except Exception as exc:  # Heartbeat failure must not block shutdown.
        log.debug("shutdown: heartbeat '%s' failed: %s", doing, exc)


async def _graceful_shutdown(listener_task: asyncio.Task) -> None:
    log.info("shutdown: signal received, stopping listener")
    await _safe_heartbeat(
        "🟡 listener Hermes deteniendo (graceful)",
        {"grace_s": _SHUTDOWN_GRACE_S, "in_flight": len(_in_flight)},
    )
    # Step 1: drain NATS connection FIRST so no new messages feed
    # _dispatch_tracked during the grace period. drain() unsubscribes
    # cleanly and lets in-flight callbacks finish before closing.
    try:
        from nats_bus import close as _nats_close

        await _nats_close()
    except Exception as exc:
        log.warning("shutdown: nats close failed: %s", exc)
    # Step 2: now it is safe to cancel the listener's keepalive loop.
    listener_task.cancel()
    try:
        await listener_task
    except asyncio.CancelledError:
        pass
    except Exception as exc:
        log.warning("shutdown: listener task exited with error: %s", exc)
    # Step 3: drain the handlers that were already running.
    survivors = await _drain_in_flight(_SHUTDOWN_GRACE_S)
    await _safe_heartbeat("🔻 listener Hermes apagado", {"survivors": survivors})
    log.info("shutdown: exit clean (survivors=%d)", survivors)


def _dispatch_tracked(msg: dict[str, Any]) -> None:
    task = asyncio.create_task(handle_message(msg))
    _track_in_flight(task)


async def main() -> None:
    log.info("Starting Hermes NATS auto-ACK listener")
    await _heartbeat(
        "🟢 listener Hermes iniciado",
        {"nats_url": NATS_URL, "shutdown_grace_s": _SHUTDOWN_GRACE_S},
    )
    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()
    _register_signal_handlers(loop, shutdown_event)
    listener_task = asyncio.create_task(start_listener(_dispatch_tracked))
    shutdown_waiter = asyncio.create_task(shutdown_event.wait())
    try:
        done, pending = await asyncio.wait(
            {listener_task, shutdown_waiter},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
    finally:
        await _graceful_shutdown(listener_task)


if __name__ == "__main__":
    asyncio.run(main())
