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
    log "Configuring Desktop Login Autostart (Interactive Terminal)..."
    mkdir -p "$DESKTOP_DIR"
    
    # Detect terminal emulator
    TERM_CMD="x-terminal-emulator -e"
    if command -v gnome-terminal &>/dev/null; then
      TERM_CMD="gnome-terminal --title='RapidAlert AI Surveillance' --"
    elif command -v terminator &>/dev/null; then
      TERM_CMD="terminator -T 'RapidAlert AI Surveillance' -x"
    fi

    cat > "$DESKTOP_FILE" << EOF
[Desktop Entry]
Type=Application
Exec=$TERM_CMD /home/clove/RapidAlert/run.sh
Hidden=false
NoDisplay=false
X-GNOME-Autostart-enabled=true
Name=RapidAlert AI Surveillance
Comment=Launch RapidAlert surveillance pipeline & dashboard on login
Terminal=false
Icon=utilities-system-monitor
EOF
    chmod +x "$DESKTOP_FILE"
    ok "Created interactive terminal desktop autostart: $DESKTOP_FILE"
    echo ""
    echo "  • On login, a terminal window will open running ./run.sh"
    echo "  • You can press Ctrl+C inside that window to stop it anytime."
    echo "  • Or run './stop.sh' from any terminal."
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
    log "Configuring Desktop Interactive Terminal Autostart & Systemd Service..."
    
    # 1. Desktop interactive terminal autostart (preferred for GUI login + Ctrl+C)
    mkdir -p "$DESKTOP_DIR"
    TERM_CMD="x-terminal-emulator -e"
    if command -v gnome-terminal &>/dev/null; then
      TERM_CMD="gnome-terminal --title='RapidAlert AI Surveillance' --"
    elif command -v terminator &>/dev/null; then
      TERM_CMD="terminator -T 'RapidAlert AI Surveillance' -x"
    fi

    cat > "$DESKTOP_FILE" << EOF
[Desktop Entry]
Type=Application
Exec=$TERM_CMD /home/clove/RapidAlert/run.sh
Hidden=false
NoDisplay=false
X-GNOME-Autostart-enabled=true
Name=RapidAlert AI Surveillance
Comment=Launch RapidAlert surveillance pipeline & dashboard on login
Terminal=false
Icon=utilities-system-monitor
EOF
    chmod +x "$DESKTOP_FILE"
    ok "Desktop autostart entry installed ($DESKTOP_FILE)."

    echo ""
    echo -e "${BOLD}${GREEN}✅ RapidAlert is now configured to start automatically on login!${NC}"
    echo ""
    echo "  How it works:"
    echo "    1. On desktop login, a visible terminal window opens running ./run.sh."
    echo "    2. The browser automatically opens to http://localhost:7000."
    echo "    3. You can press Ctrl+C in that terminal at any time to stop it."
    echo "    4. Or run './stop.sh' from any terminal to gracefully shut everything down."
    echo "    5. If already running, run.sh automatically cleans up stale processes and reuses healthy containers."
    echo ""
    ;;
esac
