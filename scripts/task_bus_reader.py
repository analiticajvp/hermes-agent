#!/usr/bin/env python3
"""Native task-bus introspection — no shell-out required."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Sequence

_FRONTMATTER_RE = re.compile(r"^---\s*\n(.*?)\n---\s*\n", re.DOTALL)
_FIELD_RE = re.compile(r"^(\w+):\s*(.+)$", re.MULTILINE)

_DEFAULT_TASKS_ROOT = "/opt/data/vault/Hall9000/shared/task-bus/tasks"
_ACTIVE_STATUSES = frozenset({"detected", "proposed", "approved", "blocked", "in_progress"})
_MAX_ACTIVE = 15
_TITLE_MAX = 80


@dataclass(frozen=True)
class TaskEntry:
    file_name: str
    status: str = ""
    task_id: str = ""
    title: str = ""
    provenance: str = ""
    updated: str = ""


def _parse_frontmatter(text: str) -> dict[str, str]:
    match = _FRONTMATTER_RE.match(text)
    if not match:
        return {}
    return {k: v.strip() for k, v in _FIELD_RE.findall(match.group(1))}


def read_task_files(root: Path) -> list[TaskEntry]:
    """Glob *.md under root, skip _template.md, parse YAML frontmatter."""
    entries: list[TaskEntry] = []
    for md_file in sorted(root.glob("*.md")):
        if md_file.name == "_template.md":
            continue
        try:
            fields = _parse_frontmatter(md_file.read_text(encoding="utf-8"))
        except OSError:
            continue
        entries.append(
            TaskEntry(
                file_name=md_file.name,
                status=fields.get("status", ""),
                task_id=fields.get("id", md_file.stem),
                title=fields.get("title", md_file.stem),
                provenance=fields.get("provenance", ""),
                updated=fields.get("updated", ""),
            )
        )
    return entries


def summarize_tasks(entries: Sequence[TaskEntry]) -> str:
    """Build a human-readable task-bus summary block."""
    counts: dict[str, int] = {}
    for entry in entries:
        counts[entry.status] = counts.get(entry.status, 0) + 1

    n = len(entries)
    lines: list[str] = [f"Task-bus summary ({n} real tasks; template excluded):"]
    for status, cnt in sorted(counts.items()):
        lines.append(f"  - {status}: {cnt}")

    active = [e for e in entries if e.status in _ACTIVE_STATUSES]
    if active:
        lines.append("")
        lines.append("Active (non-done):")
        visible = active[:_MAX_ACTIVE]
        for entry in visible:
            tid = entry.task_id or entry.file_name
            t = entry.title[:_TITLE_MAX]
            lines.append(f"  * {entry.status} → {tid} — {t}")
        overflow = len(active) - len(visible)
        if overflow > 0:
            lines.append(f"  (... {overflow} more)")

    return "\n".join(lines)


def read_and_summarize(
    root: Path | str = _DEFAULT_TASKS_ROOT,
) -> str:
    """Read task files from root and return a summary string.

    Returns a UNAVAILABLE message on filesystem errors.
    """
    task_root = Path(root)
    try:
        if not task_root.exists():
            return f"[RESULT] Task-bus UNAVAILABLE: directory not found: {task_root}"
        entries = read_task_files(task_root)
    except PermissionError as exc:
        return f"[RESULT] Task-bus UNAVAILABLE: permission denied: {exc}"
    except OSError as exc:
        return f"[RESULT] Task-bus UNAVAILABLE: {exc}"
    return summarize_tasks(entries)
