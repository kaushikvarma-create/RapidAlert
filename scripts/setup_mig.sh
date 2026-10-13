#!/usr/bin/env bash
# ==============================================================================
# setup_mig.sh — Enable & Partition MIG on NVIDIA Jetson Thor for RapidAlert
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CONFIG_FILE="${PROJECT_ROOT}/config/system.json"

echo "=========================================================="
echo "  RapidAlert NVIDIA Thor MIG Partitioning Setup"
echo "=========================================================="
echo ""

# 1. Check root privileges
if [[ $EUID -ne 0 ]]; then
   echo "❌ Please run this script with sudo:"
   echo "   sudo ./scripts/setup_mig.sh"
   exit 1
fi

# 2. Check current MIG mode
CURRENT_MIG=$(nvidia-smi --query-gpu=mig.mode.current --format=csv,noheader 2>/dev/null || echo "Unknown")
echo "Current MIG status: ${CURRENT_MIG}"

if [[ "${CURRENT_MIG}" != "Enabled" ]]; then
    echo "⚡ Enabling MIG mode on GPU 0..."
    nvidia-smi -i 0 -mig 1
    
    # Verify if immediate or reboot required
    NEW_MIG=$(nvidia-smi --query-gpu=mig.mode.current --format=csv,noheader 2>/dev/null || echo "Unknown")
    PENDING_MIG=$(nvidia-smi --query-gpu=mig.mode.pending --format=csv,noheader 2>/dev/null || echo "Unknown")
    
    if [[ "${NEW_MIG}" != "Enabled" ]]; then
        if [[ "${PENDING_MIG}" == "Enabled" ]]; then
            echo "⚠️  MIG mode is PENDING reboot."
            echo "   Please reboot the Thor system now: 'sudo reboot'"
            exit 0
        else
            echo "❌ Failed to enable MIG mode on GPU 0. Check 'dmesg' or GPU driver logs."
            exit 1
        fi
    fi
    echo "✅ MIG mode successfully Enabled on GPU 0."
fi

# 3. List available profiles
echo ""
echo "Available GPU Instance Profiles:"
nvidia-smi mig -lgip

# 4. Check existing instances or clean up if needed
EXISTING_GI=$(nvidia-smi mig -lgi 2>&1 || true)
if echo "${EXISTING_GI}" | grep -q "MIG"; then
    echo ""
    echo "Found existing MIG instances:"
    nvidia-smi -L
else
    echo ""
    echo "Creating MIG GPU Instances (2g + 1g)..."
    
    # Profile IDs on NVIDIA Thor:
    # ID 83: MIG 2g.0gb+gfx (12 SMs, display/NVDEC capable)
    # ID 81: MIG 1g.0gb+gfx (6 SMs)
    # ID 82: MIG 2g.0gb (12 SMs)
    # ID 80: MIG 1g.0gb (6 SMs)
    # ID 78: MIG 1g.0gb+me (6 SMs + media engine)
    
    echo "Attempting to create Thor partitions: 2g (12 SM) + 1g (6/8 SM)..."
    # First try +gfx profiles (preferred when Xorg/desktop is running)
    if nvidia-smi mig -cgi 83,81 -C 2>/dev/null; then
        echo "✅ Created MIG instances with profiles 83 (2g+gfx) and 81 (1g+gfx)."
    elif nvidia-smi mig -cgi 82,80 -C 2>/dev/null; then
        echo "✅ Created MIG instances with profiles 82 (2g) and 80 (1g)."
    elif nvidia-smi mig -cgi 83,78 -C 2>/dev/null; then
        echo "✅ Created MIG instances with profiles 83 (2g+gfx) and 78 (1g+me)."
    else
        echo "⚠️ Batch creation failed, attempting sequential creation..."
        nvidia-smi mig -cgi 83 -C || nvidia-smi mig -cgi 82 -C
        nvidia-smi mig -cgi 81 -C || nvidia-smi mig -cgi 80 -C || nvidia-smi mig -cgi 78 -C
    fi
fi

# 5. Extract newly created MIG UUIDs
echo ""
echo "MIG Device Topology:"
nvidia-smi -L

MIG_UUIDS=($(nvidia-smi -L | grep "MIG-" | sed -E 's/.*UUID: (MIG-[^)]+)\)/\1/'))

if [[ ${#MIG_UUIDS[@]} -eq 0 ]]; then
    echo "❌ No MIG instances found in 'nvidia-smi -L'."
    exit 1
fi

echo ""
echo "Detected MIG UUIDs:"
for i in "${!MIG_UUIDS[@]}"; do
    echo "  Slice ${i}: ${MIG_UUIDS[$i]}"
done

# 6. Update config/system.json automatically
if [[ -f "${CONFIG_FILE}" ]]; then
    echo ""
    echo "Updating ${CONFIG_FILE} with current MIG UUIDs..."
    python3 - <<EOF
import json

path = "${CONFIG_FILE}"
with open(path, "r") as f:
    d = json.load(f)

uuids = [${MIG_UUIDS[*]/#/\"}]
uuids = [u.strip() for u in "${MIG_UUIDS[*]}".split()]

eps = d.get("vllm_endpoints", [])
for i, u in enumerate(uuids):
    if i < len(eps):
        eps[i]["mig_uuid"] = u
        print(f"  • Endpoint {i} (port {eps[i].get('url')}): mig_uuid -> {u}")

if uuids:
    d["backend_mig_uuid"] = uuids[0]
    print(f"  • Backend NVDEC: backend_mig_uuid -> {uuids[0]}")

with open(path, "w") as f:
    json.dump(d, f, indent=2)

print("✅ Configuration updated successfully.")
EOF
fi

# 7. Ensure permissions on /dev/nvidia-caps for container access
if [[ -d /dev/nvidia-caps ]]; then
    chmod 666 /dev/nvidia-caps/nvidia-cap* 2>/dev/null || true
fi

echo ""
echo "=========================================================="
echo "  MIG Setup Completed Successfully!"
echo "  You can now start RapidAlert with: ./run.sh"
echo "=========================================================="
