#!/bin/bash
# ==============================================================================
# setup_network_identity.sh — Wi-Fi country code + a stable name for the dashboard
# ==============================================================================
# Run once with sudo:   sudo ./scripts/setup_network_identity.sh [COUNTRY]
# COUNTRY is a two-letter code (default IN).
#
# 1. Wi-Fi country. With no country set the radio uses the worldwide default ("00"),
#    in which every 5 GHz channel is passive-scan: it may only listen for networks,
#    never ask. 5 GHz networks then show up slowly or not at all in a scan, and the
#    hotspot is limited to 2.4 GHz. Setting the country fixes both.
#      - now:        iw reg set  (no reconnect, nothing is interrupted)
#      - every boot: driver option rtw_country_code (the Realtek driver's own setting)
#
# 2. Stable name. The Thor announces itself as <hostname>.local on whatever network
#    it joins, so the dashboard is always http://<hostname>.local:7000 even though
#    the IP address changes per network. Avahi was also announcing the Docker bridge
#    address under that name; this limits it to real network interfaces.
# ==============================================================================
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
    echo "Please run with sudo:  sudo $0 ${1:-IN}"
    exit 1
fi

COUNTRY="${1:-IN}"
if [[ ! "$COUNTRY" =~ ^[A-Z]{2}$ ]]; then
    echo "Country must be a two-letter code such as IN, US, GB."
    exit 1
fi

echo "• [1/3] Setting Wi-Fi country to ${COUNTRY} for this session..."
iw reg set "$COUNTRY"

echo "• [2/3] Making it permanent (driver option, applied at each boot)..."
cat > /etc/modprobe.d/rapidalert-wifi-country.conf <<EOF
# Written by RapidAlert scripts/setup_network_identity.sh
options rtl8852ce rtw_country_code=${COUNTRY}
options cfg80211 ieee80211_regdom=${COUNTRY}
EOF
chmod 644 /etc/modprobe.d/rapidalert-wifi-country.conf

echo "• [3/3] Limiting the .local name to real network interfaces..."
AVAHI_CONF=/etc/avahi/avahi-daemon.conf
if [[ -f "$AVAHI_CONF" ]]; then
    if grep -qE '^\s*deny-interfaces=' "$AVAHI_CONF"; then
        sed -i -E 's/^\s*deny-interfaces=.*/deny-interfaces=docker0,l4tbr0/' "$AVAHI_CONF"
    else
        sed -i -E 's/^\[server\]/[server]\ndeny-interfaces=docker0,l4tbr0/' "$AVAHI_CONF"
    fi
    systemctl restart avahi-daemon
fi

echo ""
echo "Done."
echo "  Wi-Fi country now : $(iw reg get | awk '/^country/{print $2; exit}')"
echo "  Dashboard name    : http://$(hostname).local:7000"
