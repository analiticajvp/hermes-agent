#!/usr/bin/env python3
"""Minimal Engram MCP-over-stdio proxy backed by the OVH Engram HTTP API.

Darwin runs inside Docker on Hetzner while Juan's canonical Engram SQLite
store lives on Hermes OVH.  The upstream `engram mcp` binary is local-first,
so this proxy exposes the Engram MCP tool names used by Hermes/Codex and
persists/searches against the OVH `engram serve` HTTP endpoint over Tailscale.

The proxy intentionally keeps a tiny surface area: JSON-RPC initialize,
tools/list, and tools/call.  It does not cache memory locally, so reads and
writes verify the real shared OVH Engram service.
"""

from __future__ import annotations

import datetime as _dt
import json
import os
import sys
import traceback
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


HTTP_URL = os.environ.get("ENGRAM_HTTP_URL", "http://100.92.211.3:17437").rstrip("/")
DEFAULT_PROJECT = os.environ.get("ENGRAM_PROJECT", "openmind")
DEFAULT_SCOPE = os.environ.get("ENGRAM_SCOPE", "project")
SESSION_PREFIX = os.environ.get("ENGRAM_SESSION_PREFIX", "darwin-mcp")


def _rpc_result(msg_id: Any, result: Any) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": msg_id, "result": result}


def _rpc_error(msg_id: Any, code: int, message: str, data: Any | None = None) -> dict[str, Any]:
    err: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        err["data"] = data
    return {"jsonrpc": "2.0", "id": msg_id, "error": err}


def _emit(obj: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def _request(method: str, path: str, body: dict[str, Any] | None = None) -> Any:
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        HTTP_URL + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")
        raise RuntimeError(f"Engram HTTP {exc.code}: {detail}") from exc
    if not raw:
        return None
    text = raw.decode("utf-8", "replace")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def _session_id() -> str:
    day = _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%d")
    return f"{SESSION_PREFIX}-{DEFAULT_PROJECT}-{day}"


def _ensure_session(project: str) -> str:
    sid = _session_id()
    try:
        _request("POST", "/sessions", {"id": sid, "project": project, "name": sid})
    except RuntimeError as exc:
        # Engram's HTTP API treats duplicate session creation as idempotent enough
        # for our use; only surface errors unrelated to an existing session.
        if "already exists" not in str(exc).lower():
            raise
    return sid


def _text(content: Any) -> dict[str, Any]:
    if not isinstance(content, str):
        content = json.dumps(content, ensure_ascii=False, indent=2)
    return {"content": [{"type": "text", "text": content}]}


def _tool_schema() -> list[dict[str, Any]]:
    string = {"type": "string"}
    return [
        {
            "name": "mem_context",
            "description": "Return recent Engram context from the shared OVH memory store.",
            "inputSchema": {
                "type": "object",
                "properties": {"project": string},
                "additionalProperties": False,
            },
        },
        {
            "name": "mem_search",
            "description": "Search the shared OVH Engram memory store.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": string,
                    "project": string,
                    "type": string,
                    "scope": string,
                    "limit": {"type": "integer", "minimum": 1, "maximum": 100},
                },
                "required": ["query"],
                "additionalProperties": False,
            },
        },
        {
            "name": "mem_get_observation",
            "description": "Fetch one Engram observation by id from OVH.",
            "inputSchema": {
                "type": "object",
                "properties": {"observation_id": {"type": ["integer", "string"]}},
                "required": ["observation_id"],
                "additionalProperties": False,
            },
        },
        {
            "name": "mem_save",
            "description": "Save a structured observation into the shared OVH Engram store.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "title": string,
                    "type": string,
                    "content": string,
                    "project": string,
                    "scope": string,
                },
                "required": ["title", "content"],
                "additionalProperties": False,
            },
        },
        {
            "name": "mem_session_summary",
            "description": "Save a session summary into the shared OVH Engram store.",
            "inputSchema": {
                "type": "object",
                "properties": {"content": string, "project": string, "title": string},
                "required": ["content"],
                "additionalProperties": False,
            },
        },
        {
            "name": "mem_capture_passive",
            "description": "Persist passive key learnings into shared OVH Engram.",
            "inputSchema": {
                "type": "object",
                "properties": {"content": string, "project": string},
                "required": ["content"],
                "additionalProperties": False,
            },
        },
    ]


def _call_tool(name: str, args: dict[str, Any]) -> dict[str, Any]:
    project = str(args.get("project") or DEFAULT_PROJECT)

    if name == "mem_context":
        qs = urllib.parse.urlencode({"project": project})
        return _text(_request("GET", f"/context?{qs}"))

    if name == "mem_search":
        params: dict[str, Any] = {"q": args["query"], "project": project}
        for key in ("type", "scope", "limit"):
            if args.get(key) not in (None, ""):
                params[key] = args[key]
        return _text(_request("GET", f"/search?{urllib.parse.urlencode(params)}"))

    if name == "mem_get_observation":
        obs_id = urllib.parse.quote(str(args["observation_id"]), safe="")
        return _text(_request("GET", f"/observations/{obs_id}"))

    if name == "mem_save":
        sid = _ensure_session(project)
        payload = {
            "session_id": sid,
            "title": str(args["title"]),
            "content": str(args["content"]),
            "type": str(args.get("type") or "discovery"),
            "project": project,
            "scope": str(args.get("scope") or DEFAULT_SCOPE),
        }
        return _text(_request("POST", "/observations", payload))

    if name == "mem_session_summary":
        title = str(args.get("title") or "Session summary")
        sid = _ensure_session(project)
        payload = {
            "session_id": sid,
            "title": title,
            "content": str(args["content"]),
            "type": "session_summary",
            "project": project,
            "scope": DEFAULT_SCOPE,
        }
        return _text(_request("POST", "/observations", payload))

    if name == "mem_capture_passive":
        sid = _ensure_session(project)
        payload = {
            "session_id": sid,
            "title": "Passive key learnings",
            "content": str(args["content"]),
            "type": "discovery",
            "project": project,
            "scope": DEFAULT_SCOPE,
        }
        return _text(_request("POST", "/observations", payload))

    raise ValueError(f"unknown tool: {name}")


def main() -> int:
    for raw in sys.stdin:
        raw = raw.strip()
        if not raw:
            continue
        try:
            msg = json.loads(raw)
            method = msg.get("method")
            msg_id = msg.get("id")

            if method == "initialize":
                _emit(
                    _rpc_result(
                        msg_id,
                        {
                            "protocolVersion": "2024-11-05",
                            "capabilities": {"tools": {}},
                            "serverInfo": {"name": "engram-ovh-http-proxy", "version": "1.0.0"},
                        },
                    )
                )
            elif method == "notifications/initialized":
                continue
            elif method == "tools/list":
                _emit(_rpc_result(msg_id, {"tools": _tool_schema()}))
            elif method == "tools/call":
                params = msg.get("params") or {}
                name = str(params.get("name") or "")
                args = params.get("arguments") or {}
                _emit(_rpc_result(msg_id, _call_tool(name, args)))
            else:
                _emit(_rpc_error(msg_id, -32601, f"method not found: {method}"))
        except Exception as exc:  # keep the MCP server alive after one bad call
            _emit(_rpc_error(msg.get("id") if "msg" in locals() else None, -32000, str(exc), traceback.format_exc()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
