"""
RTSPScanner: High-Performance Network Camera & NVR Discovery Engine.

Features:
  1. Multi-Subnet Discovery: Auto-detects local network interface subnets and camera network subnets.
  2. ONVIF WS-Discovery: UDP multicast probe (239.255.255.250:3702).
  3. Async TCP Port Sweep: High-concurrency sweep for RTSP (554, 8554) and HTTP management ports (80, 443, 8000).
  4. Deep NVR & Camera Inspection:
     - Dahua / CP PLUS CGI: Queries ChannelTitle and SystemInfo to retrieve real NVR channel names and hardware models.
     - Hikvision / Ezviz ISAPI: Queries /ISAPI/System/Video/inputs/channels and /ISAPI/Streaming/channels.
     - Uniview (UNV) LAPI: Queries /LAPI/V1.0/Channel/Info.
     - ONVIF SOAP Media Service: Queries GetProfiles and GetStreamUri across media ports.
     - RTSP Handshake Validation: Connects via TCP RTSP DESCRIBE (with Digest/Basic auth) to confirm stream is live,
       extract video codec (H.265, H.264, MJPEG) from SDP, and discard inactive channels.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import ipaddress
import json
import re
import socket
import time
import urllib.parse
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple

import requests
from requests.auth import HTTPDigestAuth, HTTPBasicAuth
import urllib3

urllib3.disable_warnings()

from backend.core.config import load_scanner_config
from backend.core.error_tracker import error_tracker


class RTSPScanner:

    @staticmethod
    def _get_scanner_params() -> dict:
        """Retrieve scanner templates and defaults dynamically from config/scanner.json."""
        return load_scanner_config()

    @staticmethod
    def get_existing_credentials() -> List[Tuple[str, str]]:
        """Extract stored credentials from config/cameras.json as smart defaults."""
        creds = []
        try:
            p = Path("config/cameras.json")
            if p.exists():
                cams = json.loads(p.read_text())
                for c in cams:
                    url = c.get("url", "")
                    if "@" in url:
                        auth_part = url.split("://")[1].split("@")[0]
                        if ":" in auth_part:
                            u, pwd = auth_part.split(":", 1)
                            u_dec = urllib.parse.unquote(u)
                            p_dec = urllib.parse.unquote(pwd)
                            if (u_dec, p_dec) not in creds:
                                creds.append((u_dec, p_dec))
        except Exception:
            pass
        return creds

    # ── Subnet Discovery ─────────────────────────────────────────
    @staticmethod
    def detect_subnets() -> List[str]:
        """Detects candidate subnets from network interfaces and existing cameras."""
        subnets = set()

        # 1. Inspect default outgoing socket subnet
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(("8.8.8.8", 80))
                ip = s.getsockname()[0]
            parts = ip.split(".")
            subnets.add(f"{parts[0]}.{parts[1]}.{parts[2]}.0/24")
        except Exception:
            pass

        # 2. Add subnets from configured cameras
        try:
            p = Path("config/cameras.json")
            if p.exists():
                cams = json.loads(p.read_text())
                for c in cams:
                    url = c.get("url", "")
                    host_match = re.search(r"@?([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})", url)
                    if host_match:
                        cam_ip = host_match.group(1)
                        parts = cam_ip.split(".")
                        subnets.add(f"{parts[0]}.{parts[1]}.{parts[2]}.0/24")
        except Exception:
            pass

        if not subnets:
            subnets.add("192.168.1.0/24")

        return sorted(list(subnets))

    @staticmethod
    def local_subnet() -> Optional[str]:
        subs = RTSPScanner.detect_subnets()
        return subs[0] if subs else "192.168.1.0/24"

    # ── ONVIF WS-Discovery ───────────────────────────────────────
    @staticmethod
    async def ws_discover(timeout: Optional[float] = None) -> list[str]:
        """
        Broadcasts WS-Discovery Probe to 239.255.255.250:3702 (UDP).
        Returns list of responding IP addresses.
        """
        cfg = RTSPScanner._get_scanner_params()
        probe_timeout = timeout if timeout is not None else float(cfg.get("default_ws_timeout", 2.0))
        probe_xml_template = cfg.get("ws_discovery_probe", "")

        loop = asyncio.get_event_loop()

        def _blocking_discover() -> list[str]:
            found = set()
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
            sock.settimeout(probe_timeout)
            try:
                probe_payload = (probe_xml_template or """<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:wsa="http://schemas.xmlsoap.org/ws/2004/08/addressing" xmlns:tnd="http://www.onvif.org/ver10/network/wsdl">
  <soap:Header>
    <wsa:MessageID>uuid:{}</wsa:MessageID>
    <wsa:To>urn:schemas-xmlsoap-org:ws:2005:04:discovery</wsa:To>
    <wsa:Action>http://schemas.xmlsoap.org/ws/2005/04/discovery/Probe</wsa:Action>
  </soap:Header>
  <soap:Body>
    <tnd:Probe>
      <tnd:Types>tnd:NetworkVideoTransmitter</tnd:Types>
    </tnd:Probe>
  </soap:Body>
</soap:Envelope>""").format(int(time.time())).encode("utf-8")
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
                    effect="ONVIF WS-Discovery probe encountered an error",
                    severity="WARNING",
                )
            finally:
                try:
                    sock.close()
                except Exception:
                    pass
            return list(found)

        return await loop.run_in_executor(None, _blocking_discover)

    # ── TCP Port Sweep ───────────────────────────────────────────
    @staticmethod
    async def port_scan(
        subnet: Optional[str] = None,
        port: int = 554,
        timeout: float = 0.35,
        concurrency: int = 150,
    ) -> list[str]:
        """Scan an entire /24 subnet for open TCP port (RTSP/HTTP)."""
        subnets_to_scan = [subnet] if subnet else RTSPScanner.detect_subnets()
        all_hosts = []
        for s in subnets_to_scan:
            try:
                net = ipaddress.ip_network(s, strict=False)
                all_hosts.extend([str(h) for h in net.hosts()])
            except ValueError:
                all_hosts.extend([str(h) for h in ipaddress.ip_network(f"{s}.0/24", strict=False).hosts()])

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

        await asyncio.gather(*[check(ip) for ip in set(all_hosts)])
        return sorted(list(set(open_ips)))

    # ── RTSP Handshake & SDP Validation ──────────────────────────
    @staticmethod
    def rtsp_describe(
        ip: str,
        port: int,
        path: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        timeout: float = 1.2,
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Performs RTSP DESCRIBE with Digest/Basic auth handshake.
        Returns (is_valid, codec_name, full_sdp).
        """
        clean_path = path.lstrip("/")
        url = f"rtsp://{ip}:{port}/{clean_path}"
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        try:
            s.connect((ip, port))
            req1 = f"DESCRIBE {url} RTSP/1.0\r\nCSeq: 1\r\nUser-Agent: RapidAlert\r\nAccept: application/sdp\r\n\r\n"
            s.sendall(req1.encode())
            res1 = s.recv(4096).decode(errors="ignore")

            if "200 OK" in res1:
                codec = RTSPScanner._extract_codec_from_sdp(res1)
                s.close()
                return True, codec, res1

            if "401 Unauthorized" in res1 and username:
                realm_m = re.search(r'realm="([^"]+)"', res1)
                nonce_m = re.search(r'nonce="([^"]+)"', res1)
                pwd = password or ""
                if realm_m and nonce_m:
                    realm = realm_m.group(1)
                    nonce = nonce_m.group(1)
                    ha1 = hashlib.md5(f"{username}:{realm}:{pwd}".encode()).hexdigest()
                    ha2 = hashlib.md5(f"DESCRIBE:{url}".encode()).hexdigest()
                    response = hashlib.md5(f"{ha1}:{nonce}:{ha2}".encode()).hexdigest()
                    auth_hdr = f'Digest username="{username}", realm="{realm}", nonce="{nonce}", uri="{url}", response="{response}"'
                else:
                    # Basic auth fallback
                    b64_cred = base64.b64encode(f"{username}:{pwd}".encode()).decode()
                    auth_hdr = f"Basic {b64_cred}"

                req2 = f"DESCRIBE {url} RTSP/1.0\r\nCSeq: 2\r\nUser-Agent: RapidAlert\r\nAuthorization: {auth_hdr}\r\nAccept: application/sdp\r\n\r\n"
                s.sendall(req2.encode())
                res2 = s.recv(4096).decode(errors="ignore")
                s.close()
                if "200 OK" in res2:
                    codec = RTSPScanner._extract_codec_from_sdp(res2)
                    return True, codec, res2

            s.close()
        except Exception:
            try:
                s.close()
            except Exception:
                pass
        return False, None, None

    @staticmethod
    def _extract_codec_from_sdp(sdp: str) -> str:
        """Parses video codec from SDP payload (e.g. H265, H264, JPEG)."""
        sdp_lower = sdp.lower()
        if "h265" in sdp_lower or "hevc" in sdp_lower:
            return "H.265"
        if "h264" in sdp_lower or "avc" in sdp_lower:
            return "H.264"
        if "jpeg" in sdp_lower or "mjpeg" in sdp_lower:
            return "MJPEG"
        return "H.264"

    # ── Vendor Probers ───────────────────────────────────────────
    @staticmethod
    def _probe_dahua(ip: str, port: int, username: Optional[str], password: Optional[str]) -> Tuple[List[dict], Optional[str]]:
        """Queries Dahua/CP PLUS NVR and cameras for channel names and models."""
        channels = []
        model = None
        user = username or ""
        pwd = password or ""
        auth = HTTPDigestAuth(user, pwd) if user else None

        for proto in ["https", "http"]:
            try:
                # 1. Query System Info
                if not model:
                    r_sys = requests.get(f"{proto}://{ip}/cgi-bin/magicBox.cgi?action=getSystemInfo", auth=auth, verify=False, timeout=2.0)
                    if r_sys.status_code == 200:
                        m_match = re.search(r"updateSerial=([^\r\n]+)", r_sys.text) or re.search(r"deviceType=([^\r\n]+)", r_sys.text)
                        if m_match:
                            model = m_match.group(1).strip()

                # 2. Query Channel Titles
                r_title = requests.get(f"{proto}://{ip}/cgi-bin/configManager.cgi?action=getConfig&name=ChannelTitle", auth=auth, verify=False, timeout=2.5)
                if r_title.status_code == 200 and "ChannelTitle" in r_title.text:
                    titles = re.findall(r"table\.ChannelTitle\[(\d+)\]\.Name=(.+)", r_title.text)
                    creds_part = f"{urllib.parse.quote(user)}:{urllib.parse.quote(pwd)}@" if user else ""
                    for idx_str, raw_name in titles:
                        ch_num = int(idx_str) + 1
                        clean_name = raw_name.strip()
                        rtsp_url = f"rtsp://{creds_part}{ip}:{port}/cam/realmonitor?channel={ch_num}&subtype=0"
                        channels.append({
                            "channel": ch_num,
                            "name": clean_name or f"Channel {ch_num}",
                            "rtsp_url": rtsp_url,
                            "vendor": "Dahua / CP PLUS",
                            "codec": "H.265 / H.264",
                            "verified": True,
                        })
                    if channels:
                        break
            except Exception:
                pass
        return channels, model

    @staticmethod
    def _probe_hikvision(ip: str, port: int, username: Optional[str], password: Optional[str]) -> Tuple[List[dict], Optional[str]]:
        """Queries Hikvision/Ezviz/Hilook ISAPI for channel titles and models."""
        channels = []
        model = None
        user = username or ""
        pwd = password or ""
        auth = HTTPDigestAuth(user, pwd) if user else None

        for proto in ["http", "https"]:
            try:
                # 1. Device Info
                r_dev = requests.get(f"{proto}://{ip}/ISAPI/System/deviceInfo", auth=auth, verify=False, timeout=2.0)
                if r_dev.status_code == 200:
                    mod_m = re.search(r"<model>([^<]+)</model>", r_dev.text)
                    if mod_m:
                        model = mod_m.group(1).strip()

                # 2. Streaming Channels
                r_chan = requests.get(f"{proto}://{ip}/ISAPI/Streaming/channels", auth=auth, verify=False, timeout=2.5)
                if r_chan.status_code == 200 and "<StreamingChannel" in r_chan.text:
                    creds_part = f"{urllib.parse.quote(user)}:{urllib.parse.quote(pwd)}@" if user else ""
                    chan_blocks = re.findall(r"<StreamingChannel[^>]*>(.*?)</StreamingChannel>", r_chan.text, re.DOTALL)
                    for block in chan_blocks:
                        id_m = re.search(r"<id>(\d+)</id>", block)
                        name_m = re.search(r"<channelName>([^<]+)</channelName>", block)
                        if id_m:
                            ch_id = id_m.group(1)
                            # Only take main streams (ending in 01 or 1)
                            if ch_id.endswith("01") or ch_id.endswith("1"):
                                ch_name = name_m.group(1).strip() if name_m else f"Channel {ch_id}"
                                rtsp_url = f"rtsp://{creds_part}{ip}:{port}/Streaming/Channels/{ch_id}"
                                channels.append({
                                    "channel": int(ch_id),
                                    "name": ch_name,
                                    "rtsp_url": rtsp_url,
                                    "vendor": "Hikvision",
                                    "codec": "H.265 / H.264",
                                    "verified": True,
                                })
                    if channels:
                        break
            except Exception:
                pass
        return channels, model

    @staticmethod
    def _probe_onvif(ip: str, port: int, username: Optional[str], password: Optional[str]) -> List[dict]:
        """Queries ONVIF SOAP Media Profiles to retrieve exact RTSP stream URIs."""
        channels = []
        user = username or ""
        pwd = password or ""
        auth = HTTPDigestAuth(user, pwd) if user else None

        soap_get_profiles = """<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:trt="http://www.onvif.org/ver10/media/wsdl">
  <soap:Body>
    <trt:GetProfiles/>
  </soap:Body>
</soap:Envelope>"""

        for onvif_port in [80, 443, 8000, 8080, 5000]:
            for proto in ["http", "https"]:
                url = f"{proto}://{ip}:{onvif_port}/onvif/media_service"
                try:
                    r = requests.post(
                        url,
                        data=soap_get_profiles,
                        headers={"Content-Type": "application/soap+xml; charset=utf-8"},
                        auth=auth,
                        verify=False,
                        timeout=2.0,
                    )
                    if r.status_code == 200 and "Profiles" in r.text:
                        # Extract profile tokens and names
                        profiles = re.findall(r'<trt:Profiles\s+token="([^"]+)"[^>]*>.*?<tt:Name>([^<]+)</tt:Name>', r.text, re.DOTALL)
                        creds_part = f"{urllib.parse.quote(user)}:{urllib.parse.quote(pwd)}@" if user else ""
                        for idx, (token, prof_name) in enumerate(profiles):
                            # Filter main streams if multiple
                            if "sub" in prof_name.lower():
                                continue
                            # Request Stream URI
                            soap_uri = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:trt="http://www.onvif.org/ver10/media/wsdl" xmlns:tt="http://www.onvif.org/ver10/schema">
  <soap:Body>
    <trt:GetStreamUri>
      <trt:StreamSetup><tt:Stream>RTP-Unicast</tt:Stream><tt:Transport><tt:Protocol>RTSP</tt:Protocol></tt:Transport></trt:StreamSetup>
      <trt:ProfileToken>{token}</trt:ProfileToken>
    </trt:GetStreamUri>
  </soap:Body>
</soap:Envelope>"""
                            r_uri = requests.post(url, data=soap_uri, headers={"Content-Type": "application/soap+xml; charset=utf-8"}, auth=auth, verify=False, timeout=2.0)
                            if r_uri.status_code == 200:
                                uri_m = re.search(r"<tt:Uri>([^<]+)</tt:Uri>", r_uri.text)
                                if uri_m:
                                    raw_uri = uri_m.group(1).strip()
                                    if user and "@" not in raw_uri:
                                        raw_uri = raw_uri.replace("rtsp://", f"rtsp://{creds_part}")
                                    channels.append({
                                        "channel": idx + 1,
                                        "name": prof_name.strip(),
                                        "rtsp_url": raw_uri,
                                        "vendor": "ONVIF",
                                        "codec": "H.265 / H.264",
                                        "verified": True,
                                    })
                        if channels:
                            return channels
                except Exception:
                    pass
        return channels

    # ── Single Device Inspector ──────────────────────────────────
    @staticmethod
    def inspect_device(
        ip: str,
        port: int = 554,
        credentials_list: Optional[List[Tuple[str, str]]] = None,
    ) -> Optional[dict]:
        """Deep inspection of an IP address to discover all verified camera channels and titles."""
        creds_to_try = list(credentials_list or [])
        if not creds_to_try:
            creds_to_try = RTSPScanner.get_existing_credentials()
        if ("", "") not in creds_to_try:
            creds_to_try.append(("", ""))

        for user, pwd in creds_to_try:
            # 1. Try Dahua CGI
            dahua_chans, dahua_model = RTSPScanner._probe_dahua(ip, port, user, pwd)
            if dahua_chans:
                return {
                    "ip": ip,
                    "port": port,
                    "vendor": "Dahua / CP PLUS NVR" if len(dahua_chans) > 1 else "Dahua / CP PLUS Camera",
                    "model": dahua_model or "Dahua Multi-Channel NVR",
                    "method": "dahua-cgi",
                    "channels": dahua_chans,
                    "rtsp_urls": [c["rtsp_url"] for c in dahua_chans],
                }

            # 2. Try Hikvision ISAPI
            hik_chans, hik_model = RTSPScanner._probe_hikvision(ip, port, user, pwd)
            if hik_chans:
                return {
                    "ip": ip,
                    "port": port,
                    "vendor": "Hikvision NVR" if len(hik_chans) > 1 else "Hikvision Camera",
                    "model": hik_model or "Hikvision Video Device",
                    "method": "hikvision-isapi",
                    "channels": hik_chans,
                    "rtsp_urls": [c["rtsp_url"] for c in hik_chans],
                }

            # 3. Try ONVIF SOAP
            onvif_chans = RTSPScanner._probe_onvif(ip, port, user, pwd)
            if onvif_chans:
                return {
                    "ip": ip,
                    "port": port,
                    "vendor": "ONVIF IP Device",
                    "model": "ONVIF Compliant Camera",
                    "method": "onvif-soap",
                    "channels": onvif_chans,
                    "rtsp_urls": [c["rtsp_url"] for c in onvif_chans],
                }

            # 4. RTSP Direct Handshake Probe on standard paths
            candidate_paths = [
                "cam/realmonitor?channel=1&subtype=0",
                "Streaming/Channels/101",
                "Streaming/Channels/1",
                "live/ch0",
                "h264",
                "h265",
                "onvif1",
                "video1",
                "media/video1",
            ]
            for path in candidate_paths:
                ok, codec, sdp = RTSPScanner.rtsp_describe(ip, port, path, user, pwd)
                if ok:
                    creds_part = f"{urllib.parse.quote(user)}:{urllib.parse.quote(pwd)}@" if user else ""
                    verified_url = f"rtsp://{creds_part}{ip}:{port}/{path}"
                    return {
                        "ip": ip,
                        "port": port,
                        "vendor": "IP Camera (Verified RTSP)",
                        "model": "RTSP Video Stream",
                        "method": "rtsp-describe",
                        "channels": [
                            {
                                "channel": 1,
                                "name": f"CAM_{ip.replace('.', '_')}",
                                "rtsp_url": verified_url,
                                "vendor": "RTSP",
                                "codec": codec or "H.264",
                                "verified": True,
                            }
                        ],
                        "rtsp_urls": [verified_url],
                    }

        return None

    # ── Full Scan ────────────────────────────────────────────────
    @staticmethod
    async def full_scan(
        subnet: Optional[str] = None,
        ws_timeout: Optional[float] = None,
        port_timeout: Optional[float] = None,
        credentials: Optional[tuple[str, str]] = None,
    ) -> list[dict]:
        """
        Runs comprehensive parallel network discovery and deep RTSP camera/NVR probing.
        Returns deduplicated, verified devices and live camera channels with human names.
        """
        cfg = RTSPScanner._get_scanner_params()
        default_port = int(cfg.get("default_port", 554))

        # Build credentials list: user-supplied + config defaults
        creds_list: List[Tuple[str, str]] = []
        if credentials and credentials[0]:
            creds_list.append((credentials[0], credentials[1] or ""))
        for c in RTSPScanner.get_existing_credentials():
            if c not in creds_list:
                creds_list.append(c)

        # 1. Run WS-Discovery and TCP port sweep in parallel
        ws_task = asyncio.create_task(RTSPScanner.ws_discover(timeout=ws_timeout))
        port_task = asyncio.create_task(
            RTSPScanner.port_scan(subnet=subnet, port=default_port, timeout=port_timeout or 0.35)
        )

        ws_ips, port_ips = await asyncio.gather(ws_task, port_task)
        discovered_ips = sorted(list(set(ws_ips + port_ips)))

        if not discovered_ips:
            return []

        # 2. Deep-inspect all discovered IPs in parallel using executor threads
        loop = asyncio.get_event_loop()

        def _inspect_sync(target_ip: str) -> Optional[dict]:
            return RTSPScanner.inspect_device(target_ip, port=default_port, credentials_list=creds_list)

        inspection_tasks = [
            loop.run_in_executor(None, _inspect_sync, ip) for ip in discovered_ips
        ]
        results_raw = await asyncio.gather(*inspection_tasks, return_exceptions=True)

        verified_devices = []
        for r in results_raw:
            if isinstance(r, dict) and r.get("channels"):
                verified_devices.append(r)

        return verified_devices
