#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════
#  RapidAlert — on-demand live view transcoder (started by MediaMTX)
#
#  MediaMTX runs this (runOnDemand) when the first viewer opens a
#  "<slug>_live" path and kills it shortly after the last viewer leaves,
#  so a camera is only transcoded while somebody is actually watching it.
#
#  Most cameras deliver H.265, which browsers cannot play over WebRTC.
#  Whatever the camera sends (H.265 or H.264), this
#  decodes on the Thor's hardware decoder, scales on the GPU, re-encodes
#  to H.264 on the hardware encoder and publishes to MediaMTX over local
#  RTMP. No frame ever touches Python.
#
#  Environment (set by MediaMTX): MTX_PATH = "<slug>_live"
#  Optional: RTMP_PORT (default 1935), LIVE_WIDTH/LIVE_HEIGHT/LIVE_BITRATE
# ═══════════════════════════════════════════════════════════════════
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCES="${LIVE_SOURCES_FILE:-${ROOT_DIR}/stream/live_sources.json}"
PATH_NAME="${MTX_PATH:?MTX_PATH not set (this script is meant to be started by MediaMTX)}"
RTMP_PORT="${RTMP_PORT:-1935}"
WIDTH="${LIVE_WIDTH:-1280}"
HEIGHT="${LIVE_HEIGHT:-720}"
BITRATE="${LIVE_BITRATE:-2500000}"

# Camera URL for this path (file is written by MediaMTXService.generate_config).
URL="$(python3 - "$SOURCES" "$PATH_NAME" <<'PY'
import json, sys
print(json.load(open(sys.argv[1])).get(sys.argv[2], ""))
PY
)"
[[ -n "$URL" ]] || { echo "[stream_transcode] no source for path '${PATH_NAME}'" >&2; exit 1; }

# Under MIG the NVIDIA GStreamer elements need a concrete MIG device. Prefer the slice that is
# NOT shared with the backend (DINOv2 + analysis decoders), fall back to the backend slice.
if [[ -z "${CUDA_VISIBLE_DEVICES:-}" || "${CUDA_VISIBLE_DEVICES}" != MIG-* ]]; then
  MIG_UUID="$(python3 - "${ROOT_DIR}/config/system.json" <<'PY'
import json, sys
try:
    d = json.load(open(sys.argv[1]))
    eps = d.get("vllm_endpoints", [])
    backend = d.get("backend_mig_uuid", "")
    other = [e.get("mig_uuid", "") for e in eps if e.get("mig_uuid") and e.get("mig_uuid") != backend]
    print((other or [backend] or [""])[0])
except Exception:
    print("")
PY
)"
  [[ -n "$MIG_UUID" ]] && export CUDA_VISIBLE_DEVICES="$MIG_UUID"
fi

# Baseline profile, no B-frames, one keyframe per second: plays in every browser's WebRTC stack
# and lets a new viewer start within a second.
exec gst-launch-1.0 -q -e \
  nvurisrcbin uri="$URL" select-rtp-protocol=4 latency=200 disable-audio=true \
  ! nvvideoconvert \
  ! "video/x-raw(memory:NVMM),width=${WIDTH},height=${HEIGHT},format=NV12" \
  ! nvv4l2h264enc bitrate="${BITRATE}" profile=0 iframeinterval=25 idrinterval=25 insert-sps-pps=true \
  ! h264parse config-interval=1 \
  ! flvmux streamable=true \
  ! rtmpsink location="rtmp://127.0.0.1:${RTMP_PORT}/${PATH_NAME} live=1"
