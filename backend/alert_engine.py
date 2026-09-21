"""
AlertEngine: fires and logs alerts for AI incidents (HIGH/MEDIUM severity, DANGER/WARNING safety),
and DINOv2 scene drift shift triggers.
Broadcasts to all WebSocket clients via an injected broadcast function.
"""
import time
from collections import deque
from typing import Callable, Optional, List

# Conditions that trigger an alert
_SEVERITY_TRIGGER = {"HIGH", "MEDIUM"}
_SAFETY_TRIGGER = {"DANGER", "WARNING"}


class AlertEngine:
    def __init__(self, max_alerts: int = 200):
        self._alerts: deque = deque(maxlen=max_alerts)
        self._broadcast_fn: Optional[Callable] = None

    def set_broadcaster(self, fn: Callable) -> None:
        """Inject the async broadcast function (from WSManager)."""
        self._broadcast_fn = fn

    async def record_scene_shift(
        self,
        cam_name: str,
        drift_score: float,
        thumbnail_b64: Optional[str] = None,
        thumbnails_b64: Optional[List[str]] = None,
        timestamp: Optional[str] = None,
    ) -> dict:
        """Scene shift event from DINOv2. Dispatches scene_shift signal without polluting alerts with jargon."""
        payload = {
            "type": "scene_shift",
            "cam": cam_name,
            "drift": round(drift_score, 4),
            "timestamp": timestamp or time.strftime("%Y-%m-%d %H:%M:%S"),
            "thumbnail_b64": thumbnail_b64,
            "thumbnails_b64": thumbnails_b64 or [],
        }
        if self._broadcast_fn:
            await self._broadcast_fn(payload)
        return payload

    async def process(
        self,
        cam_name: str,
        result: dict,
        thumbnail_b64: Optional[str] = None,
        thumbnails_b64: Optional[List[str]] = None,
        is_incident: bool = False,
        drift: Optional[float] = None,
        e2e_latency: Optional[float] = None,
        latency: Optional[float] = None,
    ) -> Optional[dict]:
        """Called for every VLM result. Fires alert with actual scene analysis when conditions are met."""
        raw_sev = (result.get("severity") or "LOW").upper()
        raw_safety = (result.get("safety") or "UNKNOWN").upper()

        is_alert = (
            raw_sev in _SEVERITY_TRIGGER
            or raw_safety in _SAFETY_TRIGGER
            or is_incident
        )

        if not is_alert:
            return None

        # Effective severity & safety (elevate to MEDIUM/WARNING if incident was triggered)
        severity = raw_sev if raw_sev in _SEVERITY_TRIGGER else ("MEDIUM" if is_incident else "LOW")
        safety = raw_safety if raw_safety in _SAFETY_TRIGGER else ("WARNING" if is_incident else "OK")

        trigger_mode = "TRIGGER" if is_incident else "PERIODIC"
        trigger_badge = "⚡ TRIGGER" if is_incident else "⏱️ PERIODIC"

        alert = {
            "id": f"alert_{cam_name}_{int(time.time() * 1000)}",
            "cam": cam_name,
            "ts": time.time(),
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "severity": severity,
            "safety": safety,
            "trigger_mode": trigger_mode,
            "trigger_badge": trigger_badge,
            "observation": result.get("observation", ""),
            "activity": result.get("activity", "UNKNOWN"),
            "workers": result.get("workers", "0"),
            "machinery": result.get("machinery", "None"),
            "evolution": result.get("evolution", ""),
            "model": result.get("model", "vrfai/Cosmos-Reason2-8B-NVFP4"),
            "is_incident": is_incident,
            "is_periodic": not is_incident,
            "is_drift": False,
            "drift": drift,
            "latency": latency or result.get("latency"),
            "e2e_latency": e2e_latency or result.get("e2e_latency"),
            "thumbnail_b64": thumbnail_b64 or (thumbnails_b64[-1] if thumbnails_b64 else None),
            "thumbnails_b64": thumbnails_b64 or [],
        }
        self._alerts.append(alert)

        if self._broadcast_fn:
            await self._broadcast_fn({"type": "alert", "data": alert})

        return alert

    def get_recent(self, n: int = 50) -> list:
        # Filter out any legacy drift jargon alerts so only real VLM analyses are displayed
        alerts = [
            a for a in self._alerts
            if not a.get("is_drift") and not str(a.get("observation", "")).startswith("⚡ DINOv2")
        ]
        return alerts[-n:] if len(alerts) > n else alerts

    def clear(self) -> None:
        self._alerts.clear()

