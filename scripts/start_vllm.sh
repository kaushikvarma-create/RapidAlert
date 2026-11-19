#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════
#  RapidAlert — start one vLLM container (the ONLY place the launch
#  command is defined).
#
#  Used by run.sh at boot and by the backend's container manager when
#  the watchdog has to recreate a container, so both always start a
#  container the same way. Settings come from config/system.json.
#
#  Usage: scripts/start_vllm.sh <instance index>     (0, 1, ...)
#  Removes any existing container of that name first, then starts a
#  fresh one and prints its id.
# ═══════════════════════════════════════════════════════════════════
set -euo pipefail

i="${1:?usage: start_vllm.sh <instance index>}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

VLLM_IMAGE="ghcr.io/nvidia-ai-iot/vllm:latest-jetson-thor"
HF_CACHE="$HOME/huggingface"
cfg() { python3 -c "import json; d=json.load(open('config/system.json')); print(d.get('$1', '$2'))" 2>/dev/null || echo "$2"; }
VLLM_PORT_START="$(cfg vllm_port_start 8000)"
VLLM_MODEL="$(cfg vllm_model '')"
VLLM_MAX_MODEL_LEN="$(cfg vllm_max_model_len 4096)"
VLLM_MAX_SEQS="$(cfg vllm_max_seqs 4)"
VLLM_GPU_UTILIZATION="$(cfg vllm_gpu_utilization 0.2)"
VLLM_QUANTIZATION="$(cfg vllm_quantization '')"

PORT=$(( VLLM_PORT_START + i ))
CNAME="rapidalert_vllm_${i}"

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

# Ensure persistent JIT compiler cache exists on host so restarts and rebuilds never recompile
VLLM_HOST_CACHE="$HOME/.cache/rapidalert/vllm"
mkdir -p "${VLLM_HOST_CACHE}/triton" "${VLLM_HOST_CACHE}/nv" "${VLLM_HOST_CACHE}/vllm_cache"

# Collect MIG capability device nodes if MIG is enabled (clean slice isolation as in commit aeb128af)
CAP_FLAGS=()
if [[ -d /dev/nvidia-caps ]]; then
    GI_ID=$(( i + 1 ))
    for cap_file in /dev/nvidia-caps/nvidia-cap${GI_ID}*; do
        if [[ -e "$cap_file" ]]; then
            CAP_FLAGS+=(--device "$cap_file")
        fi
    done
fi

docker rm -f "${CNAME}" >/dev/null 2>&1 || true

# --restart unless-stopped: Docker itself brings a crashed container back, even when the backend
# (and so its watchdog) is not running. `docker stop` / `docker rm` still stop it for good.
docker run -d \
    --name "${CNAME}" \
    --restart unless-stopped \
    --runtime nvidia \
    --network host \
    --shm-size=4g \
    -e NVIDIA_VISIBLE_DEVICES=all \
    -e CUDA_VISIBLE_DEVICES=0 \
    -e HF_TOKEN="${HF_TOKEN:-}" \
    -e HF_HOME=/data/models/huggingface \
    -e HF_HUB_OFFLINE=1 \
    -e TRANSFORMERS_OFFLINE=1 \
    -e EP_MODEL="${EP_MODEL}" \
    -e EP_QUANTIZATION="${EP_QUANTIZATION}" \
    "${CAP_FLAGS[@]}" \
    -v "${HF_CACHE}:/data/models/huggingface" \
    -v "${VLLM_HOST_CACHE}/triton:/root/.triton" \
    -v "${VLLM_HOST_CACHE}/nv:/root/.nv" \
    -v "${VLLM_HOST_CACHE}/vllm_cache:/root/.cache/vllm" \
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
        ${EP_KV_BYTES:+--kv-cache-memory-bytes \"${EP_KV_BYTES}\"} \
        --dtype auto \
        --enforce-eager \
        --max-num-seqs \"${VLLM_MAX_SEQS}\" \
        --limit-mm-per-prompt '{\"image\": 4}' \
        --mm-processor-cache-gb 0 \
        --trust-remote-code \
        ${EP_QUANTIZATION:+--quantization \"${EP_QUANTIZATION}\"} \
        --disable-log-stats \
        --no-enable-log-requests"
