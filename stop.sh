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
  sudo systemctl stop rapidalert || true
fi

# 2. Terminate uvicorn backend processes
echo -e "${CYAN}  • Stopping backend server processes...${NC}"
pkill -15 -f "backend.main" 2>/dev/null || true
pkill -15 -f "uvicorn.*backend" 2>/dev/null || true
sleep 0.5
pkill -9 -f "backend.main" 2>/dev/null || true
pkill -9 -f "uvicorn.*backend" 2>/dev/null || true

# 3. Free port 7000
fuser -k 7000/tcp 2>/dev/null || true

# 4. Optional: Stop vLLM Docker containers if user passes --all or --vllm
if [[ "${1:-}" == "--all" || "${1:-}" == "--vllm" ]]; then
  echo -e "${CYAN}  • Stopping vLLM Docker containers...${NC}"
  docker stop rapidalert_vllm_0 rapidalert_vllm_1 2>/dev/null || true
fi

echo -e "${BOLD}${GREEN}✅ RapidAlert stopped successfully.${NC}"
echo ""
