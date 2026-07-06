#!/usr/bin/env bash
set -euo pipefail

ROOT="${DARWIN_ROOT:-/home/juan/LocalHermes}"
BIN_DIR="${DARWIN_BIN_DIR:-$HOME/.local/bin}"
mkdir -p "$BIN_DIR"
ln -sfn "$ROOT/deploy/darwin-docker/Darwin" "$BIN_DIR/Darwin"
ln -sfn "$ROOT/deploy/darwin-docker/Darwin" "$BIN_DIR/darwin"

case ":$PATH:" in
  *":$BIN_DIR:"*) ;;
  *)
    PROFILE="$HOME/.profile"
    touch "$PROFILE"
    if ! grep -q 'export PATH="$HOME/.local/bin:$PATH"' "$PROFILE"; then
      printf '\n# Darwin LocalHermes wrapper\nexport PATH="$HOME/.local/bin:$PATH"\n' >> "$PROFILE"
    fi
    ;;
esac

echo "Darwin instalado en $BIN_DIR/Darwin"
