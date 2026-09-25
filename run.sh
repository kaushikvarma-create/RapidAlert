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
  VLLM_MAX_SEQS=$(python3 -c "import json; d=json.load(open('config/system.json')); print(d.get('vllm_max_seqs', 4))" 2>/dev/null || echo 4)
  VLLM_QUANTIZATION=$(python3 -c "import json; d=json.load(open('config/system.json')); print(d.get('vllm_quantization', ''))" 2>/dev/null || echo "")
  AUTO_START_VLLM=$(python3 -c "import json; d=json.load(open('config/system.json')); print(str(d.get('auto_start_vllm', True)).lower())" 2>/dev/null || echo "true")
fi

# Override auto start if user passes --no-vllm
for arg in "$@"; do
  [[ "$arg" == "--no-vllm" ]] && AUTO_START_VLLM="false"
done

# If systemd background service is running, stop it to prevent conflicting port 7000 restart loop
if systemctl is-active --quiet rapidalert 2>/dev/null; then
  echo -e "\033[1;33m[Notice]\033[0m RapidAlert systemd background service is active."
  echo -e "         Stopping background service to run in interactive terminal mode..."
  sudo -n systemctl stop rapidalert 2>/dev/null || systemctl stop rapidalert 2>/dev/null || true
  sleep 0.5
fi

# Docker config for Jetson Thor
VLLM_IMAGE="ghcr.io/nvidia-ai-iot/vllm:latest-jetson-thor"
HF_CACHE="$HOME/huggingface"
declare -a LOG_PIDS=()

cleanup() {
    local exit_code=$?
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
    fuser -k "${DASHBOARD_PORT}/tcp" 2>/dev/null || true
    echo -e "${BOLD}${GREEN}  ✓ Backend gracefully stopped.${NC}"
}
trap cleanup INT TERM

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
echo -e "  VLM      : ${BOLD}Cosmos Reason2 8B (${VLLM_MODEL})${NC}"
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
pkill -CONT -f "backend.main" 2>/dev/null || true
pkill -CONT -f "uvicorn.*backend" 2>/dev/null || true
sleep 0.1
pkill -15 -f "backend.main" 2>/dev/null || true
pkill -15 -f "uvicorn.*backend" 2>/dev/null || true
sleep 0.5
pkill -9 -f "backend.main" 2>/dev/null || true
pkill -9 -f "uvicorn.*backend" 2>/dev/null || true
fuser -k "${DASHBOARD_PORT}/tcp" 2>/dev/null || true
sleep 0.2
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

        if [[ -n "$EP_MIG_UUID" ]]; then
            log "Instance ${i}: MIG=${EP_MIG_UUID} (${EP_SM} SMs)  port=${PORT}  gpu_util=${EP_GPU_UTIL}"
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
            if docker ps -aq --filter "name=${CNAME}" | grep -q .; then
                warn "Found dead/unresponsive container '${CNAME}' — removing it ..."
                docker rm -f "${CNAME}" >/dev/null 2>&1 || true
            fi
        fi

        # Collect MIG capability device nodes if MIG is enabled
        CAP_FLAGS=()
        if [[ -d /dev/nvidia-caps ]]; then
            GI_ID=$(( i + 1 ))
            for cap_file in /dev/nvidia-caps/nvidia-cap${GI_ID} /dev/nvidia-caps/nvidia-cap${GI_ID}*; do
                if [[ -e "$cap_file" ]]; then
                    CAP_FLAGS+=(--device "$cap_file")
                fi
            done
        fi

        if [[ ${NEEDS_START} -eq 1 ]]; then
            log "Starting container ${CNAME} on port ${PORT} ..."
            docker run -d \
                --name "${CNAME}" \
                --runtime nvidia \
                --network host \
                --shm-size=4g \
                -e NVIDIA_VISIBLE_DEVICES=all \
                -e CUDA_VISIBLE_DEVICES=0 \
                -e HF_TOKEN="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" \
                -e HF_HOME=/data/models/huggingface \
                -e EP_MODEL="${EP_MODEL}" \
                -e EP_QUANTIZATION="${EP_QUANTIZATION}" \
                "${CAP_FLAGS[@]}" \
                -v "${HF_CACHE}:/data/models/huggingface" \
                "${VLLM_IMAGE}" \
                bash -c " \
                    TARGET_MODEL=\"\${EP_MODEL}\"; \
                    if [[ \"\${EP_QUANTIZATION}\" == \"gguf\" ]]; then \
                        TARGET_MODEL=\$(python3 -c \"import sys; from huggingface_hub import HfApi, hf_hub_download; r='\${EP_MODEL}'; fs=HfApi().list_repo_files(r); g=[f for f in fs if f.endswith('.gguf')]; print(hf_hub_download(r, g[0])) if g else print(r)\" 2>/dev/null || echo \"\${EP_MODEL}\"); \
                    else \
                        python3 -c \"from huggingface_hub import hf_hub_download; import json; p = hf_hub_download('\${EP_MODEL}', 'tokenizer_config.json'); d = json.load(open(p)); d['extra_special_tokens'] = {} if isinstance(d.get('extra_special_tokens'), list) else d.get('extra_special_tokens'); [d.pop('extra_special_tokens') for _ in [1] if d.get('extra_special_tokens') is None]; json.dump(d, open(p, 'w'))\" 2>/dev/null || true; \
                    fi; \
                    vllm serve \"\${TARGET_MODEL}\" \
                    --host 0.0.0.0 \
                    --port \"${PORT}\" \
                    ${EP_TOKENIZER:+--tokenizer \"${EP_TOKENIZER}\"} \
                    --max-model-len \"${VLLM_MAX_MODEL_LEN}\" \
                    --gpu-memory-utilization \"${EP_GPU_UTIL}\" \
                    --dtype auto \
                    --enforce-eager \
                    --max-num-seqs \"${VLLM_MAX_SEQS}\" \
                    --trust-remote-code \
                    ${EP_QUANTIZATION:+--quantization \"${EP_QUANTIZATION}\"} \
                    --disable-log-stats \
                    --no-enable-log-requests"
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
print(eps[0].get('mig_uuid', '') if eps else '')
" 2>/dev/null || echo "")

if [[ -n "${BACKEND_MIG_UUID}" ]]; then
    export CUDA_VISIBLE_DEVICES="${BACKEND_MIG_UUID}"
    ok "Backend pinned to MIG device: ${BACKEND_MIG_UUID}"
else
    warn "No mig_uuid in config — backend will use default CUDA device selection"
fi

# Auto-open dashboard in browser immediately across desktop sessions
(
  sleep 1.2
  TARGET_URL="http://localhost:${DASHBOARD_PORT}"
  export DISPLAY="${DISPLAY:-:1}"
  export XAUTHORITY="${XAUTHORITY:-$HOME/.Xauthority}"
  
  if command -v xdg-open &>/dev/null; then
    xdg-open "$TARGET_URL" >/dev/null 2>&1 || true
  elif command -v brave &>/dev/null; then
    brave "$TARGET_URL" >/dev/null 2>&1 &
  elif command -v google-chrome &>/dev/null; then
    google-chrome "$TARGET_URL" >/dev/null 2>&1 &
  elif command -v firefox &>/dev/null; then
    firefox "$TARGET_URL" >/dev/null 2>&1 &
  fi
) &

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

# ── Run the dashboard backend directly in foreground ─────────────────
exec python3 -m uvicorn backend.main:app \
  --host 0.0.0.0 \
  --port "${DASHBOARD_PORT}" \
  --log-level info
