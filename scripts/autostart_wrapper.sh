#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════
#  RapidAlert — Desktop Autostart Wrapper
#  Ensures desktop environment is ready, launches run.sh, and keeps
#  terminal open on exit for debugging.
# ═══════════════════════════════════════════════════════════════════
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$SCRIPT_DIR"

# 1. Allow desktop session 1.5 seconds to settle
sleep 1.5

TARGET_URL="http://localhost:7000"

# 2. Open browser to the dashboard
if command -v xdg-open &>/dev/null; then
  xdg-open "$TARGET_URL" >/dev/null 2>&1 || true
elif command -v brave &>/dev/null; then
  brave "$TARGET_URL" >/dev/null 2>&1 &
elif command -v google-chrome &>/dev/null; then
  google-chrome "$TARGET_URL" >/dev/null 2>&1 &
elif command -v firefox &>/dev/null; then
  firefox "$TARGET_URL" >/dev/null 2>&1 &
fi

# 3. If systemd service is active, attach live journal logs
if systemctl is-active --quiet rapidalert 2>/dev/null; then
  echo -e "\033[1;32m✅ RapidAlert is running in background (started on boot).\033[0m"
  echo -e "\033[1;36m• Showing real-time live surveillance telemetry (Ctrl+C to exit viewer)...\033[0m\n"
  journalctl -u rapidalert -f -n 60 --no-hostname
else
  # If systemd service is not active, run run.sh directly
  echo "[RapidAlert Autostart] Starting surveillance pipeline directly..."
  if ! ./run.sh "$@"; then
    echo ""
    echo "⚠️ RapidAlert exited with an error. Press Enter to close this window..."
    read -r
  fi
fi
