#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════
#  RapidAlert — Production Launcher
#  Handles orphaned processes, starts vLLM in Docker (Jetson-optimized),
#  waits for health, and launches the FastApi backend.
# ═══════════════════════════════════════════════════════════════════
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

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
  VLLM_QUANTIZATION=$(python3 -c "import json; d=json.load(open('config/system.json')); print(d.get('vllm_quantization', ''))" 2>/dev/null || echo "")
  AUTO_START_VLLM=$(python3 -c "import json; d=json.load(open('config/system.json')); print(str(d.get('auto_start_vllm', True)).lower())" 2>/dev/null || echo "true")
fi

# Override auto start if user passes --no-vllm
for arg in "$@"; do
  [[ "$arg" == "--no-vllm" ]] && AUTO_START_VLLM="false"
done

# Docker config for Jetson Thor
VLLM_IMAGE="ghcr.io/nvidia-ai-iot/vllm:latest-jetson-thor"
HF_CACHE="$HOME/huggingface"
declare -a LOG_PIDS=()

# ── Cleanup on exit ─────────────────────────────────────────────────
cleanup() {
    echo ""
    warn "Shutting down RapidAlert..."
    for pid in "${LOG_PIDS[@]:-}"; do
        if kill -0 "$pid" 2>/dev/null; then kill "$pid" 2>/dev/null || true; fi
    done
    if [[ -n "${UVICORN_PID:-}" ]] && kill -0 "${UVICORN_PID}" 2>/dev/null; then
        kill -15 "${UVICORN_PID}" 2>/dev/null || true
    fi
    # Don't kill vLLM docker here, keep it running for faster subsequent restarts!
    # The user can manage Docker independently, or the orphan killer will restart it if needed.
    ok "Dashboard closed. vLLM container left running for speed."
}
trap cleanup EXIT INT TERM

# ── Banner ───────────────────────────────────────────────────────────
echo -e "${BOLD}"
echo "  ██████╗  █████╗ ██████╗ ██╗██████╗  █████╗ ██╗     ███████╗██████╗ ████████╗"
echo "  ██╔══██╗██╔══██╗██╔══██╗██║██╔══██╗██╔══██╗██║     ██╔════╝██╔══██╗╚══██╔══╝"
echo "  ██████╔╝███████║██████╔╝██║██║  ██║███████║██║     █████╗  ██████╔╝   ██║   "
echo "  ██╔══██╗██╔══██║██╔═══╝ ██║██║  ██║██╔══██║██║     ██╔══╝  ██╔══██╗   ██║   "
echo "  ██║  ██║██║  ██║██║     ██║██████╔╝██║  ██║███████╗███████╗██║  ██║   ██║   "
echo "  ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝     ╚═╝╚═════╝ ╚═╝  ╚═╝╚══════╝╚══════╝╚═╝  ╚═╝   ╚═╝   "
echo -e "${NC}"
echo -e "  ${CYAN}Ultra-Fast VLM CCTV Engine${NC}  —  Jetson Thor Edition"
echo "  ─────────────────────────────────────────────────────"
echo -e "  Model  : ${BOLD}${VLLM_MODEL}${NC}"
echo -e "  Dashboard: ${BOLD}http://0.0.0.0:${DASHBOARD_PORT}${NC}"
echo "  ─────────────────────────────────────────────────────"
echo ""

# ── Pre-flight checks ────────────────────────────────────────────────
log "Running pre-flight checks ..."
command -v docker >/dev/null 2>&1 || die "docker not found. Required for vLLM."
command -v python3 >/dev/null 2>&1 || die "python3 not found."
[[ -d "${HF_CACHE}" ]] || { warn "HF cache not found — creating ${HF_CACHE}"; mkdir -p "${HF_CACHE}"; }

if ! python3 -c "import fastapi, uvicorn, aiohttp, cv2, numpy" 2>/dev/null; then
  warn "Missing dependencies — installing from requirements.txt"
  pip install --break-system-packages -q -r requirements.txt || die "Failed to install dependencies"
fi
ok "Pre-flight checks passed."
echo ""

# ── Kill orphaned processes from previous runs ───────────────────────
log "Cleaning up any orphaned backend processes ..."
pids=$(pgrep -f "backend.main:app" 2>/dev/null || true)
if [[ -n "${pids}" ]]; then
    warn "  Killing orphaned uvicorn backend (PIDs: ${pids}) ..."
    # If the process was suspended (CTRL+Z), it can't process SIGTERM. Wake it up first!
    pkill -CONT -f "backend.main:app" 2>/dev/null || true
    sleep 0.1
    pkill -15 -f "backend.main:app" 2>/dev/null || true
    sleep 1
    pkill -9 -f "backend.main:app" 2>/dev/null || true
fi
ok "Cleanup done."
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
        
        # Extract the specific model for this endpoint
        EP_MODEL=$(python3 -c "import json; d=json.load(open('config/system.json')); eps=d.get('vllm_endpoints', []); print(eps[$i]['model'] if $i < len(eps) else d.get('vllm_model'))" 2>/dev/null || echo "$VLLM_MODEL")

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
            if docker ps -aq --filter "name=${CNAME}" | grep -q .; then
                warn "Found dead/unresponsive container '${CNAME}' — removing it ..."
                docker rm -f "${CNAME}" >/dev/null 2>&1 || true
            fi
        fi

        if [[ ${NEEDS_START} -eq 1 ]]; then
            log "Starting container ${CNAME} on port ${PORT} ..."
            docker run -d \
                --name "${CNAME}" \
                --runtime nvidia \
                --network host \
                --shm-size=4g \
                -e HF_HOME=/data/models/huggingface \
                -v "${HF_CACHE}:/data/models/huggingface" \
                "${VLLM_IMAGE}" \
                vllm serve "${EP_MODEL}" \
                    --host 0.0.0.0 \
                    --port "${PORT}" \
                    --max-model-len "${VLLM_MAX_MODEL_LEN}" \
                    --gpu-memory-utilization "${VLLM_GPU_UTILIZATION}" \
                    --dtype auto \
                    ${VLLM_QUANTIZATION:+--quantization "${VLLM_QUANTIZATION}"} \
                    --disable-log-stats \
                    --no-enable-log-requests >/dev/null

            # Stream logs while we wait
            docker logs -f "${CNAME}" 2>&1 | sed "s/^/  ${YELLOW}[${CNAME}]${NC} /" &
            LOG_PIDS+=($!)

            log "Waiting for ${CNAME} on port ${PORT} to become ready..."
            MAX_WAIT=600
            ELAPSED=0
            while true; do
                if curl -sf "${C_API_URL}" >/dev/null 2>&1; then
                    ok "${CNAME} is ready."
                    break
                fi
                if ! docker ps -q --filter "name=${CNAME}" | grep -q .; then
                    echo ""
                    die "${CNAME} crashed! Check logs."
                fi
                if [[ ${ELAPSED} -ge ${MAX_WAIT} ]]; then
                    die "${CNAME} did not become ready within ${MAX_WAIT}s."
                fi
                sleep 5
                ELAPSED=$((ELAPSED + 5))
            done

            # Kill this container's log tailer now it's healthy
            if [[ ${#LOG_PIDS[@]} -gt 0 ]]; then
                _last="${LOG_PIDS[-1]}"
                kill -0 "$_last" 2>/dev/null && kill "$_last" 2>/dev/null || true
                unset 'LOG_PIDS[-1]'
            fi
        fi
    done

    echo ""
    ok "All ${VLLM_INSTANCES} vLLM instances are online and ready."
else
    warn "vLLM auto-start disabled via config/flag. Assuming model is running externally."
fi

echo ""
echo "  ─────────────────────────────────────────────────────"
log "Launching RapidAlert Dashboard ..."
echo "  ─────────────────────────────────────────────────────"
echo ""

mkdir -p logs data

# ── Run the dashboard ────────────────────────────────────────────────
python3 -m uvicorn backend.main:app \
  --host 0.0.0.0 \
  --port "${DASHBOARD_PORT}" \
  --log-level info &
UVICORN_PID=$!

wait $UVICORN_PID
