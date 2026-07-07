#!/usr/bin/env bash
set -euo pipefail

export HERMES_HOME="${HERMES_HOME:-/data/hermes}"
export HOME="${HOME:-$HERMES_HOME}"
export PYTHONUNBUFFERED=1
export PYTHONDONTWRITEBYTECODE=1
export PATH="$HERMES_HOME/bin:$PATH"

mkdir -p "$HERMES_HOME" "$HOME"

if [ -f "$HERMES_HOME/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  . "$HERMES_HOME/.env"
  set +a
fi

if [ "${1:-}" = "whereami" ]; then
  python - <<'PY'
import json
import os
import platform
import socket
import urllib.request
from pathlib import Path


def probe(name: str, url: str) -> str:
    try:
        with urllib.request.urlopen(url, timeout=3) as response:
            return f"ok status={response.status}"
    except Exception as exc:
        return f"ERROR {exc}"


home = Path(os.environ.get("HERMES_HOME", "/data/hermes"))
honcho_path = home / "honcho.json"
config_path = home / "config.yaml"
honcho_base = ""
if honcho_path.exists():
    try:
        honcho_base = str(json.loads(honcho_path.read_text()).get("baseUrl", ""))
    except Exception as exc:
        honcho_base = f"unreadable: {exc}"

print("Darwin runtime")
print("──────────────")
print(f"Identity:        Darwin")
print(f"Server:          {os.environ.get('DARWIN_SERVER_NAME', 'Server Alemán / Hetzner')}")
print(f"Host/container:  {socket.gethostname()}")
print(f"Platform:        {platform.platform()}")
print(f"User:            uid={os.getuid()} gid={os.getgid()}")
print(f"Workdir:         {os.getcwd()}")
print(f"HERMES_HOME:     {home}")
print(f"Config:          {config_path}")
print(f"Honcho config:   {honcho_path}")
print(f"Honcho baseUrl:  {honcho_base}")
print(f"Honcho probe:    {probe('honcho', honcho_base.rstrip('/') + '/openapi.json') if honcho_base else 'not configured'}")
print(f"Engram URL:      {os.environ.get('ENGRAM_HTTP_URL', 'http://100.92.211.3:17437')}")
print(f"Engram probe:    {probe('engram', os.environ.get('ENGRAM_HTTP_URL', 'http://100.92.211.3:17437').rstrip('/') + '/health')}")
PY
  exit 0
fi

_check_honcho_shared_memory() {
  local cfg="$HERMES_HOME/honcho.json"
  [ -f "$cfg" ] || return 0

  local parsed base_url workspace
  parsed="$(python - "$cfg" <<'PY'
import json, sys
from pathlib import Path
try:
    cfg = json.loads(Path(sys.argv[1]).read_text())
except Exception:
    sys.exit(0)
print(str(cfg.get('baseUrl', '')).rstrip('/'))
print(str(cfg.get('workspace', '')))
PY
)" || return 0
  base_url="$(printf '%s\n' "$parsed" | sed -n '1p')"
  workspace="$(printf '%s\n' "$parsed" | sed -n '2p')"
  [ -n "$base_url" ] && [ -n "$workspace" ] || return 0

  curl -fsS --max-time 2 \
    -H 'Content-Type: application/json' \
    -d '{}' \
    "$base_url/v3/workspaces/$workspace/peers/list" >/dev/null
}

if ! _check_honcho_shared_memory; then
  echo "[Darwin/docker] WARNING: Honcho compartido no disponible; continúo con memoria local (state.db/MEMORY.md/USER.md)." >&2
fi

if [ "$#" -gt 0 ] && [ -x "$HERMES_HOME/bin/$1" ]; then
  tool="$HERMES_HOME/bin/$1"
  shift
  exec "$tool" "$@"
fi

if [ "$#" -eq 0 ]; then
  exec hermes -p "${DARWIN_PROFILE:-default}" --tui
fi

exec hermes "$@"
