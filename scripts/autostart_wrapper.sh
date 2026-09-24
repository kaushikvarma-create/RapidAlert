#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════
#  RapidAlert — Desktop Autostart Wrapper
#  Ensures desktop environment is ready, launches run.sh, and keeps
#  terminal open on exit for debugging.
# ═══════════════════════════════════════════════════════════════════
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$SCRIPT_DIR"

# Allow desktop session 2 seconds to initialize
sleep 2

export DISPLAY="${DISPLAY:-:1}"
export XAUTHORITY="${XAUTHORITY:-$HOME/.Xauthority}"
export PATH="/usr/local/cuda/bin:$HOME/.local/bin:$PATH"

echo "[RapidAlert Autostart] Starting surveillance pipeline..."

# Run run.sh and prevent instant terminal closure if it stops
if ! ./run.sh "$@"; then
    echo ""
    echo "⚠️ RapidAlert exited with an error. Press Enter to close this window..."
    read -r
fi
