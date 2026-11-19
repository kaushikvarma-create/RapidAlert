#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════
#  RapidAlert — Graceful Shutdown Utility
#  Stops the background service, uvicorn server, and frees all ports.
# ═══════════════════════════════════════════════════════════════════
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

echo ""
echo -e "${BOLD}${YELLOW}Stopping RapidAlert AI Surveillance...${NC}"

# 1. Stop systemd service if running
if systemctl is-active --quiet rapidalert 2>/dev/null; then
  echo -e "${CYAN}  • Stopping systemd background service...${NC}"
  # The unit has Restart=always: killing the backend without stopping the unit just makes
  # systemd start it again 5 seconds later, so a failed stop here must not be ignored.
  if ! sudo -n systemctl stop rapidalert 2>/dev/null && ! systemctl stop rapidalert 2>/dev/null; then
    echo -e "${RED}  ✗ Could not stop the rapidalert service (needs privileges).${NC}"
    echo -e "    Run:  sudo systemctl stop rapidalert"
    exit 1
  fi
fi

# 2. Terminate uvicorn backend processes (exact command line only — see run.sh BACKEND_PGREP)
BACKEND_PGREP='^python3 -m uvicorn backend\.main:app '
echo -e "${CYAN}  • Stopping backend server processes...${NC}"
pkill -15 -f "${BACKEND_PGREP}" 2>/dev/null || true
sleep 0.5
pkill -9 -f "${BACKEND_PGREP}" 2>/dev/null || true

# 3. Free port 7000
fuser -k 7000/tcp 2>/dev/null || true

# 4. Optional: Stop vLLM Docker containers if user passes --all or --vllm
if [[ "${1:-}" == "--all" || "${1:-}" == "--vllm" ]]; then
  echo -e "${CYAN}  • Stopping vLLM Docker containers...${NC}"
  docker stop rapidalert_vllm_0 rapidalert_vllm_1 2>/dev/null || true
fi

echo -e "${BOLD}${GREEN}✅ RapidAlert stopped successfully.${NC}"
echo ""
