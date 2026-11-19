#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════
#  RapidAlert — Production Launcher
#  Handles orphaned processes, starts vLLM in Docker (Jetson-optimized),
#  waits for health, and launches the FastApi backend.
# ═══════════════════════════════════════════════════════════════════
set -euo pipefail

# Disable core dumps to prevent multi-gigabyte crash dumps from freezing the system on abort
ulimit -c 0

# Prevent multiple concurrent instances of run.sh from colliding
LOCK_FILE="/tmp/rapidalert_launcher.lock"
exec 200>"$LOCK_FILE"
if ! flock -n 200; then
  echo -e "\033[1;33m[Warning]\033[0m Another instance of run.sh is active. Terminating previous instance..."
  OLD_PID=$(cat "$LOCK_FILE" 2>/dev/null || echo "")
  if [[ -n "$OLD_PID" ]] && kill -0 "$OLD_PID" 2>/dev/null; then
    kill -15 "$OLD_PID" 2>/dev/null || true
    sleep 0.5
    kill -9 "$OLD_PID" 2>/dev/null || true
  fi
  flock -x 200
fi
echo "$$" > "$LOCK_FILE"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

export PYTHONWARNINGS="ignore:resource_tracker:UserWarning"

# ── Colours ─────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

log()  { echo -e "${CYAN}[$(date '+%H:%M:%S')]${NC} $*"; }
ok()   { echo -e "${GREEN}[$(date '+%H:%M:%S')] ✅ $*${NC}"; }
warn() { echo -e "${YELLOW}[$(date '+%H:%M:%S')] ⚠  $*${NC}"; }
die()  { echo -e "${RED}[$(date '+%H:%M:%S')] ❌ $*${NC}"; exit 1; }

# ── Config defaults (overriden by config/system.json) ───────────────
DASHBOARD_PORT=7000
VLLM_PORT_START=8000
VLLM_INSTANCES=1
VLLM_MODEL="Qwen/Qwen3-VL-4B-Instruct"
VLLM_MAX_MODEL_LEN=10000
VLLM_GPU_UTILIZATION="0.28"
VLLM_QUANTIZATION=""  # empty = no quantization (BF16); set to "awq", "gptq", etc.
AUTO_START_VLLM="true"

if command -v python3 &>/dev/null && [[ -f config/system.json ]]; then
  DASHBOARD_PORT=$(python3 -c "import json; d=json.load(open('config/system.json')); print(d.get('dashboard_port', 7000))" 2>/dev/null || echo 7000)
  VLLM_PORT_START=$(python3 -c "import json; d=json.load(open('config/system.json')); print(d.get('vllm_port_start', 8000))" 2>/dev/null || echo 8000)
  VLLM_INSTANCES=$(python3 -c "import json; d=json.load(open('config/system.json')); print(d.get('vllm_instances', 1))" 2>/dev/null || echo 1)
  VLLM_MODEL=$(python3 -c "import json; d=json.load(open('config/system.json')); print(d.get('vllm_model', 'Qwen/Qwen3-VL-4B-Instruct'))" 2>/dev/null || echo "Qwen/Qwen3-VL-4B-Instruct")
  VLLM_MAX_MODEL_LEN=$(python3 -c "import json; d=json.load(open('config/system.json')); print(d.get('vllm_max_model_len', 10000))" 2>/dev/null || echo 10000)
  VLLM_GPU_UTILIZATION=$(python3 -c "import json; d=json.load(open('config/system.json')); print(d.get('vllm_gpu_utilization', 0.28))" 2>/dev/null || echo 0.28)
  VLLM_MAX_SEQS=$(python3 -c "import json; d=json.load(open('config/system.json')); print(d.get('vllm_max_seqs', 4))" 2>/dev/null || echo 4)
  VLLM_QUANTIZATION=$(python3 -c "import json; d=json.load(open('config/system.json')); print(d.get('vllm_quantization', ''))" 2>/dev/null || echo "")
  AUTO_START_VLLM=$(python3 -c "import json; d=json.load(open('config/system.json')); print(str(d.get('auto_start_vllm', True)).lower())" 2>/dev/null || echo "true")
fi

# Override auto start if user passes --no-vllm, check for dev/reload flag
DEV_MODE="false"
for arg in "$@"; do
  [[ "$arg" == "--no-vllm" ]] && AUTO_START_VLLM="false"
  [[ "$arg" == "--reload" || "$arg" == "--dev" ]] && DEV_MODE="true"
done

# Exact command line of the backend. Kill patterns below match this and nothing else:
# the old patterns ("backend.main", "uvicorn.*backend") were unanchored regexes that also matched
# editors, greps and test runs touching backend/main.py and SIGKILLed them.
BACKEND_PGREP='^python3 -m uvicorn backend\.main:app '

# If systemd background service is running, stop it to prevent conflicting port 7000 restart loop.
# Skipped when systemd itself launched this script (INVOCATION_ID is set for service processes):
# the unit is already "active" at that point, so the stop below would be the service stopping
# itself on every start. That only failed to happen because `sudo -n` needed a password.
if [[ -z "${INVOCATION_ID:-}" ]] && systemctl is-active --quiet rapidalert 2>/dev/null; then
  echo -e "\033[1;33m[Notice]\033[0m RapidAlert systemd background service is active."
  echo -e "         Stopping background service to run in interactive terminal mode..."
  sudo -n systemctl stop rapidalert 2>/dev/null || systemctl stop rapidalert 2>/dev/null || true
  sleep 0.5
fi

# Docker config for Jetson Thor
# A running container that has not answered /v1/models within this window is treated as wedged.
# Observed model-load times on this box: ~70s (6SM slice) to ~270s (12SM slice shared with backend).
VLLM_BOOT_GRACE_SEC=600
VLLM_IMAGE="ghcr.io/nvidia-ai-iot/vllm:latest-jetson-thor"
HF_CACHE="$HOME/huggingface"
declare -a LOG_PIDS=()

cleanup() {
    local exit_code=$?
    trap - EXIT INT TERM HUP
    echo ""
    warn "Shutting down RapidAlert (Exit code: ${exit_code})..."
    for pid in "${LOG_PIDS[@]:-}"; do
        if kill -0 "$pid" 2>/dev/null; then kill "$pid" 2>/dev/null || true; fi
    done
    if [[ -n "${UVICORN_PID:-}" ]] && kill -0 "${UVICORN_PID}" 2>/dev/null; then
        kill -15 "${UVICORN_PID}" 2>/dev/null || true
        for _ in {1..20}; do
            if ! kill -0 "${UVICORN_PID}" 2>/dev/null; then break; fi
            sleep 0.1
        done
        if kill -0 "${UVICORN_PID}" 2>/dev/null; then
            kill -9 "${UVICORN_PID}" 2>/dev/null || true
        fi
    fi
    pkill -15 -f "${BACKEND_PGREP}" 2>/dev/null || true
    fuser -k "${DASHBOARD_PORT}/tcp" 2>/dev/null || true
    # Restore terminal state (prevents frozen/noecho terminal on exit)
    stty sane 2>/dev/null || true
    stty echo icanon 2>/dev/null || true
    echo -e "${BOLD}${GREEN}  ✓ Backend gracefully stopped and terminal restored.${NC}"
}
trap cleanup EXIT INT TERM HUP

# ── Banner ───────────────────────────────────────────────────────────
echo -e "${BOLD}"
echo "  ██████╗  █████╗ ██████╗ ██╗██████╗  █████╗ ██╗     ███████╗██████╗ ████████╗"
echo "  ██╔══██╗██╔══██╗██╔══██╗██║██╔══██╗██╔══██╗██║     ██╔════╝██╔══██╗╚══██╔══╝"
echo "  ██████╔╝███████║██████╔╝██║██║  ██║███████║██║     █████╗  ██████╔╝   ██║   "
echo "  ██╔══██╗██╔══██║██╔═══╝ ██║██║  ██║██╔══██║██║     ██╔══╝  ██╔══██╗   ██║   "
echo "  ██║  ██║██║  ██║██║     ██║██████╔╝██║  ██║███████╗███████╗██║  ██║   ██║   "
echo "  ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝     ╚═╝╚═════╝ ╚═╝  ╚═╝╚══════╝╚══════╝╚═╝  ╚═╝   ╚═╝   "
echo -e "${NC}"
echo -e "  ${CYAN}Hybrid VLM Surveillance Engine${NC}  —  Jetson Thor Edition"
echo "  ─────────────────────────────────────────────────────"
echo -e "  Ingest   : ${BOLD}NVIDIA DeepStream (NVDEC Hardware Decode)${NC}"
echo -e "  Trigger  : ${BOLD}DINOv2 Scene Drift (CUDA Real-Time)${NC}"
echo -e "  VLM      : ${BOLD}${VLLM_MODEL}${NC}"
echo -e "  Dashboard: ${BOLD}http://0.0.0.0:${DASHBOARD_PORT}${NC}"
echo "  ─────────────────────────────────────────────────────"
echo ""

# ── Pre-flight checks ────────────────────────────────────────────────
log "Running pre-flight checks ..."
command -v docker >/dev/null 2>&1 || die "docker not found. Required for vLLM."
command -v python3 >/dev/null 2>&1 || die "python3 not found."
[[ -d "${HF_CACHE}" ]] || { warn "HF cache not found — creating ${HF_CACHE}"; mkdir -p "${HF_CACHE}"; }

if ! python3 -c "import fastapi, uvicorn, aiohttp, cv2, numpy, torch, transformers, PIL" 2>/dev/null; then
  warn "Missing dependencies — installing from requirements.txt"
  pip install --break-system-packages -q -r requirements.txt || die "Failed to install dependencies"
fi

if python3 -c "from backend.services.nvidia_ingest import is_nvidia_available; exit(0 if is_nvidia_available() else 1)" 2>/dev/null; then
  ok "NVIDIA DeepStream / NVDEC hardware decoding available."
else
  warn "NVIDIA DeepStream plugins not found — will use OpenCV fallback."
fi

ok "Pre-flight checks passed."
echo ""

# ── Pre-flight checks & Automated System Audit ───────────────────────
log "Running automated pre-flight system health & integrity audit ..."
python3 scripts/system_health_audit.py --fix || warn "System audit completed with warnings; continuing..."

# ── Kill orphaned processes from previous runs ───────────────────────
log "Cleaning up any orphaned backend processes ..."
pkill -9 -f "apport" 2>/dev/null || true
rm -f /var/crash/_usr_bin_python3* 2>/dev/null || true
pkill -CONT -f "${BACKEND_PGREP}" 2>/dev/null || true
sleep 0.1
pkill -15 -f "${BACKEND_PGREP}" 2>/dev/null || true
sleep 0.5
pkill -9 -f "${BACKEND_PGREP}" 2>/dev/null || true
fuser -k "${DASHBOARD_PORT}/tcp" 2>/dev/null || true
sleep 0.2

# Check system swap memory
SWAP_TOTAL=$(free -m | awk '/Swap:/ {print $2}')
if [[ "${SWAP_TOTAL:-0}" -eq 0 ]]; then
    warn "Zero swap space detected (Swap: 0B). On Jetson Thor unified memory,"
    warn "creating a swapfile prevents kernel memory stalls under burst loads."
fi
ok "Process cleanup and pre-flight audit done."
echo ""

# ── Manage vLLM Docker Containers ─────────────────────────────────────
if [[ "$AUTO_START_VLLM" == "true" ]]; then
    log "Checking ${VLLM_INSTANCES} vLLM container(s) ..."
    
    # Clean up containers from previous instances count (e.g. if we reduced instances)
    all_vllm_containers=$(docker ps -aq --filter "name=rapidalert_vllm_")
    if [[ -n "$all_vllm_containers" ]]; then
        for cid in $all_vllm_containers; do
            cname=$(docker inspect --format '{{.Name}}' "$cid" | sed 's/^\///')
            idx=$(echo "$cname" | grep -o '[0-9]*$')
            if [[ -n "$idx" ]] && (( idx >= VLLM_INSTANCES )); then
                warn "Removing obsolete container $cname ..."
                docker rm -f "$cname" >/dev/null 2>&1 || true
            fi
        done
    fi

    # Boot instances SEQUENTIALLY — each must be healthy before the next starts.
    # On Jetson Thor unified memory, parallel startup causes all instances to
    # profile GPU memory simultaneously, giving -ve KV cache readings and crashes.
    for (( i=0; i<VLLM_INSTANCES; i++ )); do
        PORT=$(( VLLM_PORT_START + i ))
        CNAME="rapidalert_vllm_${i}"
        C_API_URL="http://localhost:${PORT}/v1/models"
        NEEDS_START=1

        # Extract per-endpoint config from system.json
        EP_MODEL=$(python3 -c "import json; d=json.load(open('config/system.json')); eps=d.get('vllm_endpoints', []); print(eps[$i]['model'] if $i < len(eps) else d.get('vllm_model'))" 2>/dev/null || echo "$VLLM_MODEL")
        EP_TOKENIZER=$(python3 -c "import json; d=json.load(open('config/system.json')); eps=d.get('vllm_endpoints', []); print(eps[$i].get('tokenizer', '') if $i < len(eps) else '')" 2>/dev/null || echo "")
        EP_QUANTIZATION=$(python3 -c "import json; d=json.load(open('config/system.json')); eps=d.get('vllm_endpoints', []); print(eps[$i].get('quantization', d.get('vllm_quantization', '')) if $i < len(eps) else d.get('vllm_quantization', ''))" 2>/dev/null || echo "$VLLM_QUANTIZATION")
        # MIG UUID — pins this container exclusively to its MIG slice
        EP_MIG_UUID=$(python3 -c "import json; d=json.load(open('config/system.json')); eps=d.get('vllm_endpoints', []); print(eps[$i].get('mig_uuid', '') if $i < len(eps) else '')" 2>/dev/null || echo "")
        EP_SM=$(python3 -c "import json; d=json.load(open('config/system.json')); eps=d.get('vllm_endpoints', []); print(eps[$i].get('sm_count', '?') if $i < len(eps) else '?')" 2>/dev/null || echo "?")
        # Per-endpoint GPU memory utilization — lower on the 12SM shard to protect NVDEC/GUI
        EP_GPU_UTIL=$(python3 -c "import json; d=json.load(open('config/system.json')); eps=d.get('vllm_endpoints', []); print(eps[$i].get('gpu_utilization', d.get('vllm_gpu_utilization', 0.95)) if $i < len(eps) else d.get('vllm_gpu_utilization', 0.95))" 2>/dev/null || echo "${VLLM_GPU_UTILIZATION}")

        # Optional fixed KV-cache size (GiB -> bytes); empty = vLLM auto-sizes from gpu_utilization.
        # See VLMEndpointConfig.kv_cache_gb in backend/core/config.py for why this exists.
        EP_KV_BYTES=$(python3 -c "import json; d=json.load(open('config/system.json')); eps=d.get('vllm_endpoints', []); g=float(eps[$i].get('kv_cache_gb', 0) or 0) if $i < len(eps) else 0; print(int(g * 1024**3) if g > 0 else '')" 2>/dev/null || echo "")

        if [[ -n "$EP_MIG_UUID" ]]; then
            log "Instance ${i}: MIG=${EP_MIG_UUID} (${EP_SM} SMs)  port=${PORT}  gpu_util=${EP_GPU_UTIL}  kv_cache_bytes=${EP_KV_BYTES:-auto}"
        else
            warn "Instance ${i}: no mig_uuid set — container will use any available GPU"
        fi

        # Check if running and serving correct model
        if curl -sf "${C_API_URL}" >/dev/null 2>&1; then
            CURRENT_MODEL=$(curl -sf "${C_API_URL}" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['data'][0]['id'])" 2>/dev/null || echo "")
            if [[ "$CURRENT_MODEL" == "$EP_MODEL" ]]; then
                ok "Instance ${i} (${CNAME}) already running on port ${PORT} serving ${EP_MODEL}."
                NEEDS_START=0
            else
                warn "Instance ${i} is serving '${CURRENT_MODEL}' instead of '${EP_MODEL}'."
                warn "Restarting container ${CNAME}..."
                docker rm -f "${CNAME}" >/dev/null 2>&1 || true
                sleep 2
            fi
        else
            # Not answering yet. A container that is running and still inside its model-load
            # window is booting, not dead: removing it here threw away minutes of load time every
            # time the backend restarted while a shard was starting or being recycled.
            C_RUNNING=$(docker inspect -f '{{.State.Running}}' "${CNAME}" 2>/dev/null || echo "missing")
            C_AGE=999999
            if [[ "$C_RUNNING" == "true" ]]; then
                C_STARTED=$(docker inspect -f '{{.State.StartedAt}}' "${CNAME}" 2>/dev/null || echo "")
                C_AGE=$(( $(date +%s) - $(date -d "${C_STARTED}" +%s 2>/dev/null || echo 0) ))
            fi
            if [[ "$C_RUNNING" == "true" ]] && (( C_AGE < VLLM_BOOT_GRACE_SEC )); then
                ok "Instance ${i} (${CNAME}) is still loading its model (${C_AGE}s since start) — leaving it running."
                NEEDS_START=0
            elif [[ "$C_RUNNING" != "missing" ]]; then
                warn "Found dead/unresponsive container '${CNAME}' — removing it ..."
                docker rm -f "${CNAME}" >/dev/null 2>&1 || true
            fi
        fi

        if [[ ${NEEDS_START} -eq 1 ]]; then
            log "Starting container ${CNAME} on port ${PORT} ..."
            # The launch command lives in one place, shared with the backend's container manager.
            "${SCRIPT_DIR}/scripts/start_vllm.sh" "$i" >/dev/null
        fi
    done

    echo ""
    log "vLLM containers initialized. Starting backend & dashboard immediately..."
else
    warn "vLLM auto-start disabled via config/flag. Assuming model is running externally."
fi

echo ""
echo "  ─────────────────────────────────────────────────────"
log "Launching RapidAlert Dashboard & Surveillance Backend ..."
echo "  ─────────────────────────────────────────────────────"
echo ""

# ── MIG device selection for backend (DeepStream NVDEC + DINOv2) ────────────
BACKEND_MIG_UUID=$(python3 -c "
import json
d = json.load(open('config/system.json'))
eps = d.get('vllm_endpoints', [])
print(d.get('backend_mig_uuid') or (eps[0].get('mig_uuid', '') if eps else ''))
" 2>/dev/null || echo "")

if [[ -n "${BACKEND_MIG_UUID}" ]] && nvidia-smi -L 2>/dev/null | grep -q "${BACKEND_MIG_UUID}"; then
    export CUDA_VISIBLE_DEVICES="${BACKEND_MIG_UUID}"
    ok "Backend pinned to MIG device: ${BACKEND_MIG_UUID}"
else
    if [[ -n "${BACKEND_MIG_UUID}" ]]; then
        warn "Configured MIG UUID '${BACKEND_MIG_UUID}' not active on host (MIG mode is currently disabled)."
    fi
    export CUDA_VISIBLE_DEVICES=0
    ok "Backend using primary GPU (device 0)"
fi

LAN_IPS=$(hostname -I 2>/dev/null || echo "")

echo ""
echo -e "${BOLD}${GREEN}  ╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}${GREEN}  ║   RapidAlert AI Surveillance Dashboard is LIVE!              ║${NC}"
echo -e "${BOLD}${GREEN}  ║   • Local:   ${CYAN}http://localhost:${DASHBOARD_PORT}${GREEN}                                ║${NC}"
for ip in $LAN_IPS; do
  if [[ "$ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ && "$ip" != "127.0.0.1" && "$ip" != "172.17.0.1" ]]; then
    printf "${BOLD}${GREEN}  ║   • Network: ${CYAN}http://%-15s:${DASHBOARD_PORT}${GREEN}                    ║${NC}\n" "$ip"
  fi
done
echo -e "${BOLD}${GREEN}  ╚═══════════════════════════════════════════════════════════════╝${NC}"
echo ""

# ── Run the dashboard backend in foreground with TTY protection ───────────
# --loop asyncio (not uvicorn's default uvloop): uvloop starts every subprocess with a full fork()
# of this multi-GB, ~500-thread process, which froze the event loop for ~0.75-1.3s per call. The
# network watchdog alone spawns ~10 nmcli/ip processes every 10s, so the loop was frozen most of
# the time (measured: GET /api/network/status took 4-14s). asyncio's loop spawns via vfork/posix_spawn.
UVICORN_CMD=(python3 -m uvicorn backend.main:app --host 0.0.0.0 --port "${DASHBOARD_PORT}" --loop asyncio --timeout-graceful-shutdown 2 --log-level info)
if [[ "${DEV_MODE}" == "true" ]]; then
    warn "Running with --reload (Development Mode). Child processes may be spawned."
    UVICORN_CMD+=(--reload)
else
    ok "Running in direct production mode (single-process, zero reload zombies)."
fi

"${UVICORN_CMD[@]}" &
UVICORN_PID=$!

wait "${UVICORN_PID}" 2>/dev/null || true
cleanup

