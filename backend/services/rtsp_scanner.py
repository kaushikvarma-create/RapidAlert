"""
RTSPScanner: network discovery for RTSP / ONVIF IP cameras.
Two scan methods run in parallel:
  1. ONVIF WS-Discovery (UDP broadcast to 239.255.255.250:3702)
  2. TCP port 554 port scan
Combines results, deduplicates by IP, and applies vendor RTSP path templates loaded from config/scanner.json.
"""
from __future__ import annotations

import asyncio
import ipaddress
import socket
import time
from typing import Optional

from backend.core.config import load_scanner_config
from backend.core.error_tracker import error_tracker


class RTSPScanner:

    @staticmethod
    def _get_scanner_params():
        """Retrieve scanner templates and defaults dynamically from config/scanner.json."""
        return load_scanner_config()

    # ── ONVIF WS-Discovery ───────────────────────────────────────
    @staticmethod
    async def ws_discover(timeout: Optional[float] = None) -> list[str]:
        """
        Broadcasts WS-Discovery Probe to 239.255.255.250:3702 (UDP).
        Returns list of responding IP addresses.
        """
        cfg = RTSPScanner._get_scanner_params()
        probe_timeout = timeout if timeout is not None else float(cfg.get("default_ws_timeout", 3.0))
        probe_xml_template = cfg.get("ws_discovery_probe", "")

        loop = asyncio.get_event_loop()

        def _blocking_discover() -> list[str]:
            found = set()
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
            sock.settimeout(probe_timeout)
            try:
                probe_payload = probe_xml_template.format(int(time.time())).encode("utf-8")
                sock.sendto(probe_payload, ("239.255.255.250", 3702))
                while True:
                    try:
                        data, addr = sock.recvfrom(65536)
                        found.add(addr[0])
                    except socket.timeout:
                        break
            except Exception as exc:
                error_tracker.capture_exception(
                    exc,
                    component="RTSPScanner",
                    effect="ONVIF WS-Discovery probe encountered an error; discovered IPs may be incomplete",
                    severity="WARNING",
                )
            finally:
                try:
                    sock.close()
                except Exception:
                    pass
            return list(found)

        return await loop.run_in_executor(None, _blocking_discover)

    # ── TCP port 554 scan ────────────────────────────────────────
    @staticmethod
    async def port_scan(
        subnet: Optional[str] = None,
        port: Optional[int] = None,
        timeout: Optional[float] = None,
        concurrency: Optional[int] = None,
    ) -> list[str]:
        """
        Scan an entire /24 subnet for open TCP port (RTSP).
        If subnet is None, auto-detects the local /24.
        Returns list of IPs with the port open.
        """
        cfg = RTSPScanner._get_scanner_params()
        scan_port = port if port is not None else int(cfg.get("default_port", 554))
        scan_timeout = timeout if timeout is not None else float(cfg.get("default_port_timeout", 0.4))
        scan_concurrency = concurrency if concurrency is not None else int(cfg.get("scan_concurrency", 128))

        if subnet is None:
            subnet = RTSPScanner._local_subnet()
        if subnet is None:
            error_tracker.capture_error(
                message="Cannot determine local subnet for port scan",
                component="RTSPScanner",
                effect="RTSP port sweep skipped due to unknown subnet",
                severity="WARNING",
            )
            return []

        # Enumerate /24 (or whatever prefix)
        try:
            net = ipaddress.ip_network(subnet, strict=False)
        except ValueError:
            # Treat as a bare /24 prefix like "192.168.1"
            net = ipaddress.ip_network(f"{subnet}.0/24", strict=False)

        hosts = [str(h) for h in net.hosts()]
        sem = asyncio.Semaphore(scan_concurrency)
        open_ips: list[str] = []

        async def check(ip: str):
            async with sem:
                try:
                    _, writer = await asyncio.wait_for(
                        asyncio.open_connection(ip, scan_port), timeout=scan_timeout
                    )
                    writer.close()
                    try:
                        await writer.wait_closed()
                    except Exception:
                        pass
                    open_ips.append(ip)
                except Exception:
                    pass  # Connection refused or timed out — expected for non-camera IPs

        await asyncio.gather(*[check(ip) for ip in hosts])
        return sorted(open_ips)

    # ── Full scan: WS-Discovery + port scan ─────────────────────
    @staticmethod
    async def full_scan(
        subnet: Optional[str] = None,
        ws_timeout: Optional[float] = None,
        port_timeout: Optional[float] = None,
        credentials: Optional[tuple[str, str]] = None,
    ) -> list[dict]:
        """
        Run both WS-Discovery and port scan in parallel.
        Returns deduplicated list of discovered cameras with vendor URL templates.
        """
        cfg = RTSPScanner._get_scanner_params()
        default_port = int(cfg.get("default_port", 554))
        path_templates = cfg.get("rtsp_path_templates", [])

        ws_task = asyncio.create_task(RTSPScanner.ws_discover(timeout=ws_timeout))
        port_task = asyncio.create_task(
            RTSPScanner.port_scan(subnet=subnet, port=default_port, timeout=port_timeout)
        )

        ws_ips, port_ips = await asyncio.gather(ws_task, port_task)

        # Merge: prefer ONVIF-discovered IPs
        all_ips: dict[str, str] = {}  # ip → method
        for ip in ws_ips:
            all_ips[ip] = "onvif"
        for ip in port_ips:
            if ip not in all_ips:
                all_ips[ip] = "tcp-554"

        creds_str = ""
        if credentials:
            user, pwd = credentials
            if user:
                creds_str = f"{user}:{pwd}@" if pwd else f"{user}@"

        results = []
        for ip, method in sorted(all_ips.items()):
            urls = [
                t.replace("{creds}", creds_str)
                 .replace("{ip}", ip)
                 .replace("{port}", str(default_port))
                for t in path_templates
            ]
            results.append({
                "ip": ip,
                "port": default_port,
                "method": method,
                "rtsp_urls": urls,
            })

        return results

    # ── Utility ──────────────────────────────────────────────────
    @staticmethod
    def _local_subnet() -> Optional[str]:
        """Best-guess local /24 subnet, e.g. '192.168.1.0/24'."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(("8.8.8.8", 80))
                ip = s.getsockname()[0]
            # Convert to /24
            parts = ip.split(".")
            return f"{parts[0]}.{parts[1]}.{parts[2]}.0/24"
        except Exception as exc:
            error_tracker.capture_exception(
                exc,
                component="RTSPScanner",
                effect="Could not automatically resolve local subnet",
                severity="WARNING",
            )
            return None

    @staticmethod
    def local_subnet() -> Optional[str]:
        return RTSPScanner._local_subnet()
