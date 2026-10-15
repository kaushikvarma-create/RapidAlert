#!/bin/bash
# ==============================================================================
# create-thor-mig.sh — Clean NVIDIA Thor MIG Partitioning on Boot
# ==============================================================================
# Officially documented JetPack 7.2 pairing on NVIDIA Jetson Thor:
# - Slice 0: Profile 83 (2g.0gb+gfx, 12 SMs) — Primary graphics/display + NVDEC
# - Slice 1: Profile 78 (1g.0gb+me, 6/8 SMs) — Secondary compute + media engine
#   Fallback: Profile 80 (1g.0gb, 6 SMs) if 78 unavailable
# ==============================================================================

LOG="/var/log/nvidia-thor-mig.log"
exec >> "$LOG" 2>&1
echo "=== Starting NVIDIA Thor MIG initialization at $(date) ==="

# Wait up to 30 seconds for the NVIDIA kernel driver to be ready
echo "Waiting for NVIDIA driver to be accessible..."
READY=0
for i in {1..30}; do
    if nvidia-smi >/dev/null 2>&1; then
        READY=1
        echo "NVIDIA driver ready after ${i}s."
        break
    fi
    sleep 1
done

if [ "$READY" -ne 1 ]; then
    echo "❌ ERROR: NVIDIA driver did not become ready within 30 seconds."
    exit 1
fi

set -e

# 1. Enable Persistence Mode
echo "Enabling Persistence Mode..."
nvidia-smi -pm 1 || true

# 2. Enable MIG mode on GPU 0
echo "Ensuring MIG mode is enabled on GPU 0..."
nvidia-smi -i 0 -mig 1 || true

# 3. Check existing MIG instances
HAS_2G=$(nvidia-smi mig -lgi 2>/dev/null | grep -E "2g|Profile[[:space:]]*8[23]" | grep -c "MIG" || true)
HAS_1G=$(nvidia-smi mig -lgi 2>/dev/null | grep -E "1g|Profile[[:space:]]*78|Profile[[:space:]]*8[01]" | grep -c "MIG" || true)

if [ "$HAS_2G" -ge 1 ] && [ "$HAS_1G" -ge 1 ]; then
    echo "✅ MIG instances already exist (2g and 1g found). Ensuring compute instances are active..."
    nvidia-smi mig -i 0 -gi 1 -cci 2>/dev/null || true
    nvidia-smi mig -i 0 -gi 2 -cci 2>/dev/null || true
else
    if [ "$HAS_2G" -eq 0 ] && [ "$HAS_1G" -eq 0 ]; then
        echo "Creating batch Thor slices: Profile 83 (2g.0gb+gfx) + Profile 78 (1g.0gb+me)..."
        if ! nvidia-smi mig -i 0 -cgi 83,78 -C 2>/dev/null; then
            nvidia-smi mig -i 0 -cgi 83,80 -C 2>/dev/null || true
        fi
    fi

    # Check again if 2g is still missing
    HAS_2G=$(nvidia-smi mig -lgi 2>/dev/null | grep -E "2g|Profile[[:space:]]*8[23]" | grep -c "MIG" || true)
    if [ "$HAS_2G" -eq 0 ]; then
        echo "Creating 2g slice (Profile 83 / 82)..."
        nvidia-smi mig -i 0 -cgi 83 -C 2>/dev/null || nvidia-smi mig -i 0 -cgi 82 -C 2>/dev/null || true
    fi

    # Check again if 1g is still missing
    HAS_1G=$(nvidia-smi mig -lgi 2>/dev/null | grep -E "1g|Profile[[:space:]]*78|Profile[[:space:]]*8[01]" | grep -c "MIG" || true)
    if [ "$HAS_1G" -eq 0 ]; then
        echo "Creating 1g slice (Profile 78 / 80)..."
        nvidia-smi mig -i 0 -cgi 78 -C 2>/dev/null || nvidia-smi mig -i 0 -cgi 80 -C 2>/dev/null || true
    fi
fi

# 4. Set container permissions on capability devices
if [ -d /dev/nvidia-caps ]; then
    chmod 666 /dev/nvidia-caps/nvidia-cap* 2>/dev/null || true
    echo "✅ Set permissions on /dev/nvidia-caps"
fi

# 5. Automatically synchronize config/system.json with detected MIG UUIDs
CONFIG_FILE="/home/clove/RapidAlert/config/system.json"
if [ -f "$CONFIG_FILE" ]; then
    python3 - << 'PYEOF' || true
import json, subprocess, sys

try:
    output = subprocess.check_output(["nvidia-smi", "-L"], text=True)
    uuids_2g = []
    uuids_1g = []
    for line in output.splitlines():
        if "MIG" in line and "UUID:" in line:
            parts = line.split("UUID:")
            if len(parts) > 1:
                u = parts[1].strip().rstrip(")")
                if "2g" in line:
                    uuids_2g.append(u)
                else:
                    uuids_1g.append(u)
    
    if uuids_2g and uuids_1g:
        config_path = "/home/clove/RapidAlert/config/system.json"
        with open(config_path, "r") as f:
            cfg = json.load(f)
        
        eps = cfg.get("vllm_endpoints", [])
        if len(eps) >= 2:
            eps[0]["mig_uuid"] = uuids_2g[0]
            eps[1]["mig_uuid"] = uuids_1g[0]
        cfg["backend_mig_uuid"] = uuids_2g[0]
        
        with open(config_path, "w") as f:
            json.dump(cfg, f, indent=2)
        print(f"✅ Auto-updated system.json: Slice 0 (12SM)={uuids_2g[0]}, Slice 1 (6SM)={uuids_1g[0]}")
except Exception as e:
    print(f"Warning: could not auto-update system.json: {e}")
PYEOF
fi

echo "=== Current GPU & MIG Configuration ==="
nvidia-smi -L
echo "=== Initialization Complete at $(date) ==="
