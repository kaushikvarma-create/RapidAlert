"""
RapidAlert Network Management Service
Handles:
- Ethernet carrier status & LAN addressing
- Wi-Fi scanning (nearby SSIDs, signal strength, security)
- Wi-Fi client connections (joining site wireless networks)
- Access Point Hotspot ("godofthunder" WPA2 setup hotspot)
- Dual-Mode State Engine (Ethernet-aware Hotspot with failover rescue mode)
"""

import asyncio
import json
import logging
import os
import re
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger("RapidAlert.Network")


class NetworkService:
    def __init__(self, config_manager, broadcaster=None, dashboard_port: int = 7000):
        self.config_manager = config_manager
        self.broadcast = broadcaster
        self.dashboard_port = dashboard_port
        self._watchdog_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        self._last_state_hash: Optional[str] = None

        # Cached interface names (auto-detected if None)
        self.eth_interface: str = "enP2p1s0"
        self.wifi_interface: str = "wlP1p1s0"
        self.hotspot_profile_name: str = "godofthunder"

        self._auto_detect_interfaces()

    def _auto_detect_interfaces(self):
        """Detect primary physical ethernet and wireless interfaces from sysfs."""
        try:
            net_dir = Path("/sys/class/net")
            if not net_dir.exists():
                return

            eth_candidates = []
            wifi_candidates = []

            for iface in net_dir.iterdir():
                name = iface.name
                if name.startswith(("lo", "docker", "l4tbr", "dummy", "can", "usb")):
                    continue
                wireless_dir = iface / "wireless"
                phy80211 = iface / "phy80211"
                if wireless_dir.exists() or phy80211.exists() or name.startswith("wl"):
                    wifi_candidates.append(name)
                elif name.startswith(("en", "eth")):
                    eth_candidates.append(name)

            if eth_candidates:
                # Prefer enP2p1s0 if present, else first ethernet
                if "enP2p1s0" in eth_candidates:
                    self.eth_interface = "enP2p1s0"
                else:
                    self.eth_interface = eth_candidates[0]

            if wifi_candidates:
                if "wlP1p1s0" in wifi_candidates:
                    self.wifi_interface = "wlP1p1s0"
                else:
                    self.wifi_interface = wifi_candidates[0]

            logger.info(f"[Network] Detected interfaces — Ethernet: {self.eth_interface}, Wi-Fi: {self.wifi_interface}")
        except Exception as e:
            logger.warning(f"[Network] Failed to auto-detect interfaces: {e}")

    def set_broadcaster(self, broadcaster):
        self.broadcast = broadcaster

    # ══════════════════════════════════════════════════════════════════════
    #  Helper Execution
    # ══════════════════════════════════════════════════════════════════════

    async def _run_cmd(self, *args: str, timeout: float = 12.0) -> tuple[int, str, str]:
        """Run system command asynchronously without blocking the event loop."""
        try:
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
            return (
                proc.returncode or 0,
                stdout.decode("utf-8", errors="replace").strip(),
                stderr.decode("utf-8", errors="replace").strip()
            )
        except asyncio.TimeoutError:
            try:
                proc.kill()
            except Exception:
                pass
            return (-1, "", f"Command timed out after {timeout}s")
        except Exception as e:
            return (-1, "", str(e))

    def _get_sysfs_carrier(self, iface: str) -> bool:
        """Check physical link carrier in sysfs."""
        try:
            path = Path(f"/sys/class/net/{iface}/carrier")
            if path.exists():
                return path.read_text().strip() == "1"
        except Exception:
            pass
        return False

    async def _get_ip_info(self, iface: str) -> Dict[str, Any]:
        """Retrieve IP, MAC, and flags from `ip -j addr show`."""
        rc, out, _ = await self._run_cmd("ip", "-j", "addr", "show", iface)
        info = {"ip": None, "mac": None, "operstate": "DOWN", "prefixlen": None}
        if rc != 0 or not out:
            return info

        try:
            data = json.loads(out)
            if data and isinstance(data, list):
                item = data[0]
                info["operstate"] = item.get("operstate", "DOWN")
                info["mac"] = item.get("address")
                for addr in item.get("addr_info", []):
                    if addr.get("family") == "inet" and addr.get("scope") == "global":
                        info["ip"] = addr.get("local")
                        info["prefixlen"] = addr.get("prefixlen")
                        break
        except Exception as e:
            logger.debug(f"[Network] Error parsing ip -j addr: {e}")
        return info

    async def _get_connected_hotspot_clients(self) -> int:
        """Count clients connected to hotspot on wifi_interface via ip neigh / arp."""
        rc, out, _ = await self._run_cmd("ip", "-j", "neigh", "show", "dev", self.wifi_interface)
        if rc == 0 and out:
            try:
                entries = json.loads(out)
                # Filter for reachable or stale clients with valid IP/MAC
                active = [
                    e for e in entries
                    if e.get("dst") and e.get("lladdr") and e.get("state", [""])[0] not in ("FAILED", "INCOMPLETE")
                ]
                return len(active)
            except Exception:
                pass
        return 0

    # ══════════════════════════════════════════════════════════════════════
    #  Network Status
    # ══════════════════════════════════════════════════════════════════════

    async def get_status(self) -> Dict[str, Any]:
        """Comprehensive network status across Ethernet, Wi-Fi, and Hotspot."""
        cfg = self.config_manager.get()
        net_cfg = getattr(cfg, "network", None)
        hotspot_ssid = getattr(net_cfg, "hotspot_ssid", "godofthunder") if net_cfg else "godofthunder"
        hotspot_pwd = getattr(net_cfg, "hotspot_password", "Cl0vernd@123") if net_cfg else "Cl0vernd@123"
        hotspot_mode = getattr(net_cfg, "hotspot_mode", "auto") if net_cfg else "auto"

        # 1. Inspect NetworkManager device table
        rc, dev_out, _ = await self._run_cmd("nmcli", "-t", "-f", "DEVICE,TYPE,STATE,CONNECTION", "dev", "status")
        nm_devices: Dict[str, Dict[str, str]] = {}
        if rc == 0 and dev_out:
            for line in dev_out.splitlines():
                parts = line.split(":")
                if len(parts) >= 4:
                    nm_devices[parts[0]] = {
                        "type": parts[1],
                        "state": parts[2],
                        "connection": parts[3]
                    }

        # 2. Ethernet Status
        eth_info = await self._get_ip_info(self.eth_interface)
        eth_carrier = self._get_sysfs_carrier(self.eth_interface)
        eth_nm = nm_devices.get(self.eth_interface, {})
        eth_connected = eth_carrier and bool(eth_info.get("ip"))

        # 3. Wi-Fi Status
        wifi_info = await self._get_ip_info(self.wifi_interface)
        wifi_nm = nm_devices.get(self.wifi_interface, {})
        wifi_state = wifi_nm.get("state", "disconnected")
        wifi_conn = wifi_nm.get("connection", "")

        is_hotspot_active = (wifi_conn == self.hotspot_profile_name or wifi_conn == hotspot_ssid) and wifi_state == "connected"
        is_client_connected = wifi_state == "connected" and not is_hotspot_active

        # Check Wi-Fi radio status
        rc_rad, out_rad, _ = await self._run_cmd("nmcli", "radio", "wifi")
        wifi_radio_on = (out_rad.strip().lower() == "enabled") if rc_rad == 0 else True

        # Mode tag
        if is_hotspot_active:
            wifi_mode = "hotspot"
        elif is_client_connected:
            wifi_mode = "client"
        else:
            wifi_mode = "disconnected"

        # Clients count on hotspot
        hotspot_clients = await self._get_connected_hotspot_clients() if is_hotspot_active else 0

        # Description of active state
        if eth_connected and is_hotspot_active:
            active_desc = f"Ethernet Active ({eth_info['ip']}) + AP Hotspot Online ('{hotspot_ssid}')"
        elif eth_connected:
            active_desc = f"Ethernet Active ({eth_info['ip']}) (Hotspot idle)"
        elif is_hotspot_active:
            active_desc = f"Rescue AP Hotspot Online ('{hotspot_ssid}' on 10.42.0.1)"
        elif is_client_connected:
            active_desc = f"Wi-Fi Client Connected to '{wifi_conn}' ({wifi_info['ip']})"
        else:
            active_desc = "Offline / No Active Link"

        access_urls = []
        if is_hotspot_active:
            access_urls.append({
                "label": f"Direct Hotspot ({hotspot_ssid})",
                "url": f"http://{wifi_info.get('ip') or '10.42.0.1'}:{self.dashboard_port}"
            })
        if eth_info.get("ip"):
            access_urls.append({
                "label": f"Ethernet LAN ({self.eth_interface})",
                "url": f"http://{eth_info['ip']}:{self.dashboard_port}"
            })
        if is_client_connected and wifi_info.get("ip"):
            access_urls.append({
                "label": f"Wi-Fi Network ({wifi_conn})",
                "url": f"http://{wifi_info['ip']}:{self.dashboard_port}"
            })

        return {
            "status": "ok",
            "active_mode_desc": active_desc,
            "dashboard_port": self.dashboard_port,
            "ethernet": {
                "interface": self.eth_interface,
                "connected": eth_connected,
                "carrier": eth_carrier,
                "ip": eth_info.get("ip"),
                "mac": eth_info.get("mac"),
                "operstate": eth_info.get("operstate"),
            },
            "wifi": {
                "interface": self.wifi_interface,
                "enabled": wifi_radio_on,
                "mode": wifi_mode,
                "connected_ssid": wifi_conn if is_client_connected else None,
                "ip": wifi_info.get("ip") if is_client_connected else None,
                "mac": wifi_info.get("mac"),
                "operstate": wifi_info.get("operstate"),
            },
            "hotspot": {
                "active": is_hotspot_active,
                "ssid": hotspot_ssid,
                "password": hotspot_pwd,
                "mode": hotspot_mode,
                "ip": wifi_info.get("ip") or "10.42.0.1" if is_hotspot_active else "10.42.0.1",
                "clients_connected": hotspot_clients,
                "access_url": f"http://10.42.0.1:{self.dashboard_port}",
            },
            "access_urls": [u for u in access_urls if u],
        }

    # ══════════════════════════════════════════════════════════════════════
    #  Wi-Fi Scanning
    # ══════════════════════════════════════════════════════════════════════

    async def scan_wifi(self) -> List[Dict[str, Any]]:
        """
        Scan for nearby Wi-Fi networks using `nmcli dev wifi list`.
        Returns deduplicated, signal-sorted AP list.
        """
        # Make sure wifi radio is turned on
        await self._run_cmd("nmcli", "radio", "wifi", "on")

        rc, out, err = await self._run_cmd(
            "nmcli", "-t", "-f", "IN-USE,BSSID,SSID,MODE,CHAN,RATE,SIGNAL,SECURITY",
            "dev", "wifi", "list", "--rescan", "yes",
            timeout=10.0
        )
        if rc != 0 and not out:
            logger.warning(f"[Network] Scan failed: {err}")
            return []

        networks: Dict[str, Dict[str, Any]] = {}
        cfg = self.config_manager.get()
        net_cfg = getattr(cfg, "network", None)
        hotspot_ssid = getattr(net_cfg, "hotspot_ssid", "godofthunder") if net_cfg else "godofthunder"

        for line in out.splitlines():
            if not line:
                continue
            # Regex split to handle escaped colons (\:) in BSSID
            # Pattern: in-use, bssid, ssid, mode, chan, rate, signal, security
            parts = re.split(r"(?<!\\):", line)
            if len(parts) < 8:
                continue

            in_use = parts[0].strip() == "*"
            bssid = parts[1].replace("\\:", ":").strip()
            ssid = parts[2].replace("\\:", ":").strip()
            mode = parts[3].strip()
            chan = parts[4].strip()
            rate = parts[5].strip()
            try:
                signal = int(parts[6].strip())
            except ValueError:
                signal = 0
            security = parts[7].strip() or "Open"

            if not ssid:
                continue  # Skip hidden/unnamed SSIDs

            is_own_hotspot = (ssid == hotspot_ssid or ssid == self.hotspot_profile_name)

            # Deduplicate by SSID, keep highest signal
            if ssid not in networks or signal > networks[ssid]["signal"]:
                networks[ssid] = {
                    "ssid": ssid,
                    "bssid": bssid,
                    "signal": signal,
                    "security": security,
                    "channel": chan,
                    "rate": rate,
                    "mode": mode,
                    "in_use": in_use,
                    "is_hotspot": is_own_hotspot,
                }

        sorted_aps = sorted(networks.values(), key=lambda x: x["signal"], reverse=True)
        return sorted_aps

    # ══════════════════════════════════════════════════════════════════════
    #  Hotspot Management ("godofthunder")
    # ══════════════════════════════════════════════════════════════════════

    async def ensure_hotspot_profile(self, ssid: str = "godofthunder", password: str = "Cl0vernd@123") -> bool:
        """Create or update NetworkManager connection profile for the Hotspot."""
        # Check if profile exists
        rc, out, _ = await self._run_cmd("nmcli", "-t", "-f", "NAME,UUID", "con", "show")
        profile_exists = False
        if rc == 0:
            for line in out.splitlines():
                if line.startswith(f"{self.hotspot_profile_name}:"):
                    profile_exists = True
                    break

        if not profile_exists:
            # Create fresh hotspot profile
            rc_add, out_add, err_add = await self._run_cmd(
                "nmcli", "con", "add",
                "type", "wifi",
                "ifname", self.wifi_interface,
                "con-name", self.hotspot_profile_name,
                "autoconnect", "no",
                "ssid", ssid,
                "802-11-wireless.mode", "ap",
                "ipv4.method", "shared",
                "ipv6.method", "ignore",
                "wifi-sec.key-mgmt", "wpa-psk",
                "wifi-sec.psk", password
            )
            if rc_add != 0:
                logger.error(f"[Network] Failed to create hotspot profile: {err_add}")
                return False
            logger.info(f"[Network] Created hotspot profile '{self.hotspot_profile_name}' (SSID: {ssid})")
            return True
        else:
            # Update SSID and password if changed
            await self._run_cmd("nmcli", "con", "modify", self.hotspot_profile_name, "802-11-wireless.ssid", ssid)
            await self._run_cmd("nmcli", "con", "modify", self.hotspot_profile_name, "wifi-sec.psk", password)
            await self._run_cmd("nmcli", "con", "modify", self.hotspot_profile_name, "802-11-wireless.mode", "ap")
            await self._run_cmd("nmcli", "con", "modify", self.hotspot_profile_name, "ipv4.method", "shared")
            return True

    async def start_hotspot(self, ssid: Optional[str] = None, password: Optional[str] = None) -> Dict[str, Any]:
        """Activate the godofthunder hotspot."""
        async with self._lock:
            cfg = self.config_manager.get()
            net_cfg = getattr(cfg, "network", None)
            target_ssid = ssid or (getattr(net_cfg, "hotspot_ssid", "godofthunder") if net_cfg else "godofthunder")
            target_pwd = password or (getattr(net_cfg, "hotspot_password", "Cl0vernd@123") if net_cfg else "Cl0vernd@123")

            await self._run_cmd("nmcli", "radio", "wifi", "on")
            ok = await self.ensure_hotspot_profile(target_ssid, target_pwd)
            if not ok:
                return {"status": "error", "message": "Failed to configure hotspot profile"}

            rc, out, err = await self._run_cmd("nmcli", "con", "up", self.hotspot_profile_name, timeout=15.0)
            if rc != 0:
                logger.error(f"[Network] Failed to activate hotspot: {err}")
                return {"status": "error", "message": f"Failed to activate hotspot: {err}"}

            logger.info(f"[Network] ✅ Hotspot '{target_ssid}' active on {self.wifi_interface} (IP: 10.42.0.1)")
            status = await self.get_status()
            if self.broadcast:
                await self.broadcast({"type": "network_status", "data": status})
            return {"status": "ok", "message": f"Hotspot '{target_ssid}' online", "network": status}

    async def stop_hotspot(self) -> Dict[str, Any]:
        """Deactivate the godofthunder hotspot."""
        async with self._lock:
            rc, out, err = await self._run_cmd("nmcli", "con", "down", self.hotspot_profile_name)
            logger.info(f"[Network] Hotspot stopped (rc={rc})")
            status = await self.get_status()
            if self.broadcast:
                await self.broadcast({"type": "network_status", "data": status})
            return {"status": "ok", "message": "Hotspot stopped", "network": status}

    # ══════════════════════════════════════════════════════════════════════
    #  Wi-Fi Client Connection (Joining LAN)
    # ══════════════════════════════════════════════════════════════════════

    async def connect_wifi(self, ssid: str, password: str = "") -> Dict[str, Any]:
        """
        Connect Wi-Fi interface to a wireless LAN access point as a client.
        Brings down the hotspot first (since chip cannot do simultaneous AP+STA).
        """
        async with self._lock:
            # 1. Take down hotspot if active
            await self._run_cmd("nmcli", "con", "down", self.hotspot_profile_name)
            await asyncio.sleep(0.5)

            # 2. Attempt connection to target Wi-Fi
            cmd = ["nmcli", "dev", "wifi", "connect", ssid, "ifname", self.wifi_interface]
            if password:
                cmd.extend(["password", password])

            rc, out, err = await self._run_cmd(*cmd, timeout=25.0)
            if rc != 0:
                logger.warning(f"[Network] Failed to connect to Wi-Fi '{ssid}': {err}")
                # If in auto mode, revive the hotspot so device isn't stranded!
                cfg = self.config_manager.get()
                net_cfg = getattr(cfg, "network", None)
                mode = getattr(net_cfg, "hotspot_mode", "auto") if net_cfg else "auto"
                if mode == "auto":
                    await self._run_cmd("nmcli", "con", "up", self.hotspot_profile_name)

                return {
                    "status": "error",
                    "message": f"Failed to connect to '{ssid}': {err or out}",
                }

            logger.info(f"[Network] ✅ Successfully joined Wi-Fi '{ssid}'")
            # Wait briefly for DHCP lease
            await asyncio.sleep(2.0)
            status = await self.get_status()
            if self.broadcast:
                await self.broadcast({"type": "network_status", "data": status})
            return {
                "status": "ok",
                "message": f"Connected to Wi-Fi '{ssid}'",
                "network": status
            }

    async def disconnect_wifi(self) -> Dict[str, Any]:
        """Disconnect active Wi-Fi connection."""
        async with self._lock:
            await self._run_cmd("nmcli", "dev", "disconnect", self.wifi_interface)
            status = await self.get_status()
            if self.broadcast:
                await self.broadcast({"type": "network_status", "data": status})
            return {"status": "ok", "message": "Disconnected Wi-Fi", "network": status}

    async def forget_wifi(self, ssid: str) -> Dict[str, Any]:
        """Forget/delete saved connection profile for an SSID."""
        rc, out, err = await self._run_cmd("nmcli", "con", "delete", "id", ssid)
        return {
            "status": "ok" if rc == 0 else "error",
            "message": f"Profile for '{ssid}' deleted" if rc == 0 else err
        }

    # ══════════════════════════════════════════════════════════════════════
    #  Hotspot Configuration Update
    # ══════════════════════════════════════════════════════════════════════

    async def update_config(self, hotspot_ssid: Optional[str] = None, hotspot_password: Optional[str] = None, hotspot_mode: Optional[str] = None) -> Dict[str, Any]:
        """Update system network settings and hot-reload."""
        updates: Dict[str, Any] = {}
        net_updates: Dict[str, Any] = {}

        if hotspot_ssid is not None and hotspot_ssid.strip():
            net_updates["hotspot_ssid"] = hotspot_ssid.strip()
        if hotspot_password is not None and hotspot_password.strip():
            if len(hotspot_password.strip()) < 8:
                return {"status": "error", "message": "Hotspot WPA2 password must be at least 8 characters"}
            net_updates["hotspot_password"] = hotspot_password.strip()
        if hotspot_mode is not None:
            if hotspot_mode not in ("auto", "always_on", "disabled"):
                return {"status": "error", "message": "Invalid hotspot_mode. Choose 'auto', 'always_on', or 'disabled'"}
            net_updates["hotspot_mode"] = hotspot_mode

        if net_updates:
            cfg = self.config_manager.get()
            curr_net = getattr(cfg, "network", None)
            merged = {
                "hotspot_ssid": curr_net.hotspot_ssid if curr_net else "godofthunder",
                "hotspot_password": curr_net.hotspot_password if curr_net else "Cl0vernd@123",
                "hotspot_mode": curr_net.hotspot_mode if curr_net else "auto",
                "wifi_interface": self.wifi_interface,
                "ethernet_interface": self.eth_interface,
            }
            merged.update(net_updates)
            updates["network"] = merged
            self.config_manager.update(updates)

            # Update NM profile
            await self.ensure_hotspot_profile(merged["hotspot_ssid"], merged["hotspot_password"])

            # Enforce mode immediately
            if merged["hotspot_mode"] == "always_on":
                await self.start_hotspot()
            elif merged["hotspot_mode"] == "disabled":
                await self.stop_hotspot()
            elif merged["hotspot_mode"] == "auto":
                await self._enforce_auto_failover()

        status = await self.get_status()
        if self.broadcast:
            await self.broadcast({"type": "network_status", "data": status})
        return {"status": "ok", "network": status}

    # ══════════════════════════════════════════════════════════════════════
    #  Background Watchdog (Ethernet-Aware Hotspot & Failover)
    # ══════════════════════════════════════════════════════════════════════

    async def _enforce_auto_failover(self):
        """
        Dual-State Logic:
        1. When on Ethernet (90% of the time):
           Wi-Fi runs purely as the Hotspot ("godofthunder") so users can connect anytime.
        2. When no Ethernet:
           - If connected to client Wi-Fi: keep client Wi-Fi running.
           - If disconnected from client Wi-Fi: activate Hotspot ("godofthunder") as rescue AP!
        """
        status = await self.get_status()
        eth_connected = status["ethernet"]["connected"]
        wifi_mode = status["wifi"]["mode"]
        is_hotspot_active = status["hotspot"]["active"]

        if eth_connected:
            # On Ethernet: Keep Hotspot running
            if not is_hotspot_active:
                logger.info("[Network Watchdog] Ethernet connected. Activating AP Hotspot 'godofthunder' for direct access.")
                await self.start_hotspot()
        else:
            # No Ethernet
            if wifi_mode == "client":
                # Successfully connected to site Wi-Fi — leave it as client
                pass
            elif not is_hotspot_active:
                # No Ethernet and no client Wi-Fi — bring up Rescue Hotspot!
                logger.info("[Network Watchdog] No active Ethernet or client Wi-Fi. Activating Rescue Hotspot.")
                await self.start_hotspot()

    async def start_watchdog(self, interval_sec: float = 10.0):
        """Start background loop monitoring physical link transitions."""
        if self._watchdog_task and not self._watchdog_task.done():
            return

        async def _loop():
            logger.info(f"[Network Watchdog] Started (checking every {interval_sec}s)")
            # Initial run on startup
            try:
                cfg = self.config_manager.get()
                net_cfg = getattr(cfg, "network", None)
                mode = getattr(net_cfg, "hotspot_mode", "auto") if net_cfg else "auto"
                if mode == "auto":
                    await self._enforce_auto_failover()
                elif mode == "always_on":
                    await self.start_hotspot()
            except Exception as e:
                logger.warning(f"[Network Watchdog] Startup error: {e}")

            while True:
                try:
                    await asyncio.sleep(interval_sec)
                    cfg = self.config_manager.get()
                    net_cfg = getattr(cfg, "network", None)
                    mode = getattr(net_cfg, "hotspot_mode", "auto") if net_cfg else "auto"

                    if mode == "auto":
                        await self._enforce_auto_failover()
                    elif mode == "always_on":
                        status = await self.get_status()
                        if not status["hotspot"]["active"]:
                            await self.start_hotspot()
                    elif mode == "disabled":
                        status = await self.get_status()
                        if status["hotspot"]["active"]:
                            await self.stop_hotspot()

                    # Check for state change to broadcast
                    status = await self.get_status()
                    state_sig = f"{status['ethernet']['connected']}-{status['wifi']['mode']}-{status['hotspot']['active']}-{status['hotspot']['clients_connected']}"
                    if state_sig != self._last_state_hash:
                        self._last_state_hash = state_sig
                        if self.broadcast:
                            await self.broadcast({"type": "network_status", "data": status})

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.debug(f"[Network Watchdog] Loop tick error: {e}")

        self._watchdog_task = asyncio.create_task(_loop(), name="rapidalert-network-watchdog")

    def stop_watchdog(self):
        if self._watchdog_task:
            self._watchdog_task.cancel()
            self._watchdog_task = None
