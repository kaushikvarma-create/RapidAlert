"""
RTSPScanner: discovers cameras on the local network.

Two methods:
  1. WS-Discovery (ONVIF multicast) — finds ONVIF-compliant cameras instantly
  2. TCP port scan on 554 (RTSP) — catches non-ONVIF cameras

Returns a list of dicts: {ip, port, method, rtsp_urls}
where rtsp_urls are common path guesses to try (tested against each IP).
"""
import asyncio
import ipaddress
import re
import socket
import uuid
from typing import Optional

# Common RTSP paths for popular DVR/NVR brands
RTSP_PATH_TEMPLATES = [
    "rtsp://{creds}{ip}:{port}/cam/realmonitor?channel=1&subtype=0",  # Dahua
    "rtsp://{creds}{ip}:{port}/h264/ch1/main/av_stream",              # Hikvision
    "rtsp://{creds}{ip}:{port}/stream1",                              # generic
    "rtsp://{creds}{ip}:{port}/live",                                 # generic
    "rtsp://{creds}{ip}:{port}/video1",                               # generic
    "rtsp://{creds}{ip}:{port}/ch0_0.264",                            # Axis-style
    "rtsp://{creds}{ip}:{port}/mpeg4/media.amp",                      # Axis
    "rtsp://{creds}{ip}:{port}/1",                                    # simple
    "rtsp://{creds}{ip}:{port}/live/ch00_0",                          # Reolink
    "rtsp://{creds}{ip}:{port}/11",                                   # Bosch
]

WS_DISCOVERY_MSG = (
    '<?xml version="1.0" encoding="utf-8"?>'
    '<Envelope xmlns:tds="http://www.onvif.org/ver10/device/wsdl"'
    ' xmlns="http://www.w3.org/2003/05/soap-envelope">'
    '<Header>'
    '<wsa:MessageID xmlns:wsa="http://schemas.xmlsoap.org/ws/2004/08/addressing">'
    "uuid:{msg_id}"
    "</wsa:MessageID>"
    '<wsa:To xmlns:wsa="http://schemas.xmlsoap.org/ws/2004/08/addressing">'
    "urn:schemas-xmlsoap-org:ws:2005:04:discovery"
    "</wsa:To>"
    '<wsa:Action xmlns:wsa="http://schemas.xmlsoap.org/ws/2004/08/addressing">'
    "http://schemas.xmlsoap.org/ws/2005/04/discovery/Probe"
    "</wsa:Action>"
    "</Header>"
    "<Body>"
    '<Probe xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'
    ' xmlns:xsd="http://www.w3.org/2001/XMLSchema"'
    ' xmlns="http://schemas.xmlsoap.org/ws/2005/04/discovery">'
    '<Types xmlns:dn="http://www.onvif.org/ver10/network/wsdl">'
    "dn:NetworkVideoTransmitter"
    "</Types>"
    "</Probe>"
    "</Body>"
    "</Envelope>"
)


class RTSPScanner:
    """Async RTSP/ONVIF camera discovery."""

    # ── WS-Discovery (ONVIF multicast) ──────────────────────────
    @staticmethod
    async def ws_discover(timeout: float = 3.0) -> list[str]:
        """
        Send ONVIF WS-Discovery probe to 239.255.255.250:3702.
        Returns list of IPs that responded.
        """
        msg = WS_DISCOVERY_MSG.replace("{msg_id}", str(uuid.uuid4()))
        found: set[str] = set()
        loop = asyncio.get_event_loop()

        def _blocking_discover():
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.settimeout(timeout)
                sock.sendto(msg.encode(), ("239.255.255.250", 3702))
                while True:
                    try:
                        data, addr = sock.recvfrom(65536)
                        found.add(addr[0])
                    except socket.timeout:
                        break
            except Exception as exc:
                print(f"[Scanner] WS-Discovery error: {exc}")
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
        port: int = 554,
        timeout: float = 0.4,
        concurrency: int = 128,
    ) -> list[str]:
        """
        Scan an entire /24 subnet for open TCP port 554 (RTSP).
        If subnet is None, auto-detects the local /24.
        Returns list of IPs with the port open.
        """
        if subnet is None:
            subnet = RTSPScanner._local_subnet()
        if subnet is None:
            print("[Scanner] Cannot determine local subnet")
            return []

        # Enumerate /24 (or whatever prefix)
        try:
            net = ipaddress.ip_network(subnet, strict=False)
        except ValueError:
            # Treat as a bare /24 prefix like "192.168.1"
            net = ipaddress.ip_network(f"{subnet}.0/24", strict=False)

        hosts = [str(h) for h in net.hosts()]
        sem = asyncio.Semaphore(concurrency)
        open_ips: list[str] = []

        async def check(ip: str):
            async with sem:
                try:
                    _, writer = await asyncio.wait_for(
                        asyncio.open_connection(ip, port), timeout=timeout
                    )
                    writer.close()
                    try:
                        await writer.wait_closed()
                    except Exception:
                        pass
                    open_ips.append(ip)
                except Exception:
                    pass

        await asyncio.gather(*[check(ip) for ip in hosts])
        return sorted(open_ips)

    # ── Full scan: WS-Discovery + port scan ─────────────────────
    @staticmethod
    async def full_scan(
        subnet: Optional[str] = None,
        ws_timeout: float = 3.0,
        port_timeout: float = 0.4,
        credentials: Optional[tuple[str, str]] = None,
    ) -> list[dict]:
        """
        Run both WS-Discovery and port scan in parallel.
        Returns deduplicated list of discovered cameras.
        """
        ws_task   = asyncio.create_task(RTSPScanner.ws_discover(timeout=ws_timeout))
        port_task = asyncio.create_task(RTSPScanner.port_scan(subnet=subnet, timeout=port_timeout))

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
                 .replace("{port}", "554")
                for t in RTSP_PATH_TEMPLATES
            ]
            results.append({
                "ip": ip,
                "port": 554,
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
        except Exception:
            return None

    @staticmethod
    def local_subnet() -> Optional[str]:
        return RTSPScanner._local_subnet()
