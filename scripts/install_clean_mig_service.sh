#!/bin/bash
# ==============================================================================
# install_clean_mig_service.sh — Setup Clean NVIDIA Thor MIG Boot Initialization
# ==============================================================================
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
   echo "❌ Please run this script with sudo:"
   echo "   sudo ./scripts/install_clean_mig_service.sh"
   exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "=========================================================="
echo "  Installing Clean NVIDIA Thor MIG Boot Configuration"
echo "=========================================================="

# 1. Install /usr/local/bin/create-thor-mig.sh
echo "• [1/4] Installing /usr/local/bin/create-thor-mig.sh..."
cp "${SCRIPT_DIR}/create-thor-mig.sh" /usr/local/bin/create-thor-mig.sh
chmod 755 /usr/local/bin/create-thor-mig.sh
chown root:root /usr/local/bin/create-thor-mig.sh

# 2. Configure /etc/systemd/system/nvidia-mig.service
echo "• [2/4] Configuring /etc/systemd/system/nvidia-mig.service..."
cat << 'EOF' > /etc/systemd/system/nvidia-mig.service
[Unit]
Description=NVIDIA Thor MIG Slices Initialization (JetPack 7.2)
After=nv-load-display-modules.service
Before=display-manager.service gdm.service gdm3.service lightdm.service nvargus-daemon.service rapidalert.service

[Service]
Type=oneshot
ExecStart=/usr/local/bin/create-thor-mig.sh
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF
chmod 644 /etc/systemd/system/nvidia-mig.service

# 3. Ensure rapidalert.service waits for nvidia-mig.service if present
if [[ -f /etc/systemd/system/rapidalert.service ]]; then
    echo "• [3/4] Ensuring rapidalert.service starts after nvidia-mig.service..."
    if ! grep -q "nvidia-mig.service" /etc/systemd/system/rapidalert.service; then
        sed -i 's/After=network.target docker.service nv-oem-config.service/After=network.target docker.service nv-oem-config.service nvidia-mig.service/' /etc/systemd/system/rapidalert.service
    fi
fi

# 4. Enable nvidia-mig.service
echo "• [4/4] Reloading systemd and enabling service..."
systemctl daemon-reload
systemctl enable nvidia-mig.service

echo ""
echo "=========================================================="
echo "  ✅ Clean MIG boot service installed successfully!"
echo "  On reboot, systemd will:"
echo "    1. Wait for NVIDIA kernel display drivers to load"
echo "    2. Create Slice 0 (Profile 83: 2g+gfx, 12 SM) and Slice 1 (Profile 78: 1g+me, 6 SM)"
echo "    3. Set permissions on /dev/nvidia-caps"
echo "    4. Auto-update config/system.json with the exact slice UUIDs"
echo "    5. Launch GDM3 and RapidAlert only after slicing completes"
echo "=========================================================="
echo ""
read -p "Would you like to reboot the Thor system now to apply? (y/N): " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Rebooting system..."
    reboot
fi
