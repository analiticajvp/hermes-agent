#!/usr/bin/env bash
set -euo pipefail

# Darwin remote launcher: from Juan's Mac console, run the Darwin Docker
# deployment on Server Alemán / Hetzner. Previous local launchers are backed
# up under /Users/juanc.verni/LocalHermes/backups/launcher-*/.

REMOTE_USER_HOST="${DARWIN_SERVER_SSH:-juan@hetzner-apps}"
REMOTE_HOSTNAME="${DARWIN_SERVER_HOSTNAME:-100.83.86.67}"
REMOTE_KEY="${DARWIN_SERVER_KEY:-$HOME/.ssh/id_ed25519_hetzner}"
REMOTE_DARWIN="${DARWIN_REMOTE_BIN:-/home/juan/.local/bin/Darwin}"
REMOTE_WORKDIR="${DARWIN_REMOTE_WORKDIR:-/home/juan/LocalHermes/server-workspace}"

ssh_args=(
  -o HostName="$REMOTE_HOSTNAME"
  -o ServerAliveInterval=30
  -o ServerAliveCountMax=3
  -i "$REMOTE_KEY"
)

# Allocate a TTY only for interactive invocations; keep scripting usable.
if [[ -t 0 && -t 1 ]]; then
  ssh_args=(-tt "${ssh_args[@]}")
fi

printf -v remote_cmd 'cd %q && exec %q' "$REMOTE_WORKDIR" "$REMOTE_DARWIN"
for arg in "$@"; do
  printf -v quoted ' %q' "$arg"
  remote_cmd+="$quoted"
done

exec ssh "${ssh_args[@]}" "$REMOTE_USER_HOST" "$remote_cmd"
