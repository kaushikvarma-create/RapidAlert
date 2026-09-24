#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════
#  RapidAlert — Automatic Startup Installer
#  Configures systemd service and/or desktop autostart for Jetson Thor.
# ═══════════════════════════════════════════════════════════════════
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVICE_FILE="$SCRIPT_DIR/rapidalert.service"
DESKTOP_DIR="$HOME/.config/autostart"
DESKTOP_FILE="$DESKTOP_DIR/rapidalert.desktop"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

log()  { echo -e "${CYAN}[setup]${NC} $*"; }
ok()   { echo -e "${GREEN}[setup] ✅ $*${NC}"; }
warn() { echo -e "${YELLOW}[setup] ⚠  $*${NC}"; }

echo ""
echo -e "${BOLD}${CYAN}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}${CYAN}   RapidAlert AI Surveillance — Automatic Startup Setup        ${NC}"
echo -e "${BOLD}${CYAN}═══════════════════════════════════════════════════════════════${NC}"
echo ""

# Ensure run.sh is executable
chmod +x "$SCRIPT_DIR/run.sh"

MODE="${1:-all}"

case "$MODE" in
  --systemd|systemd)
    log "Configuring systemd background service..."
    if [[ -f "$SERVICE_FILE" ]]; then
      sudo cp "$SERVICE_FILE" /etc/systemd/system/rapidalert.service
      sudo systemctl daemon-reload
      sudo systemctl enable rapidalert.service
      ok "Installed and enabled /etc/systemd/system/rapidalert.service"
      echo ""
      echo "  • Start service now:    sudo systemctl start rapidalert"
      echo "  • Check status / logs:  sudo systemctl status rapidalert -l"
      echo "  • Follow live logs:     sudo journalctl -u rapidalert -f"
      echo "  • Stop service:         sudo systemctl stop rapidalert"
      echo "  • Disable autostart:    sudo systemctl disable rapidalert"
    else
      warn "Service file not found at $SERVICE_FILE"
    fi
    ;;

  --desktop|desktop)
    log "Configuring GNOME/XDG Desktop Login Autostart..."
    mkdir -p "$DESKTOP_DIR"
    cat > "$DESKTOP_FILE" << EOF
[Desktop Entry]
Type=Application
Exec=/home/clove/RapidAlert/run.sh
Hidden=false
NoDisplay=false
X-GNOME-Autostart-enabled=true
Name=RapidAlert AI Surveillance
Comment=Launch RapidAlert surveillance pipeline & dashboard on login
Terminal=true
Icon=utilities-system-monitor
EOF
    chmod +x "$DESKTOP_FILE"
    ok "Created desktop autostart entry: $DESKTOP_FILE"
    ;;

  --disable|disable)
    log "Disabling all autostart configurations..."
    sudo systemctl disable rapidalert.service 2>/dev/null || true
    sudo systemctl stop rapidalert.service 2>/dev/null || true
    sudo rm -f /etc/systemd/system/rapidalert.service
    sudo systemctl daemon-reload
    rm -f "$DESKTOP_FILE"
    ok "RapidAlert autostart disabled and removed."
    ;;

  all|*)
    log "Setting up systemd service and desktop autostart..."
    
    # 1. Systemd service
    if [[ -f "$SERVICE_FILE" ]]; then
      sudo cp "$SERVICE_FILE" /etc/systemd/system/rapidalert.service
      sudo systemctl daemon-reload
      sudo systemctl enable rapidalert.service
      ok "Systemd service installed and enabled."
    fi

    # 2. Desktop entry
    mkdir -p "$DESKTOP_DIR"
    cat > "$DESKTOP_FILE" << EOF
[Desktop Entry]
Type=Application
Exec=/home/clove/RapidAlert/run.sh
Hidden=false
NoDisplay=false
X-GNOME-Autostart-enabled=true
Name=RapidAlert AI Surveillance
Comment=Launch RapidAlert surveillance pipeline & dashboard on login
Terminal=true
Icon=utilities-system-monitor
EOF
    chmod +x "$DESKTOP_FILE"
    ok "Desktop autostart entry installed."
    echo ""
    echo -e "${BOLD}${GREEN}✅ RapidAlert is now configured to start automatically on system boot!${NC}"
    echo ""
    echo "  Commands to manage the background service:"
    echo "    • Start now:      sudo systemctl start rapidalert"
    echo "    • Stop:           sudo systemctl stop rapidalert"
    echo "    • Check status:   sudo systemctl status rapidalert"
    echo "    • View logs:      sudo journalctl -u rapidalert -f"
    echo "    • Disable:        ./scripts/setup_autostart.sh --disable"
    echo ""
    ;;
esac
