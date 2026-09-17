"""
AlertEngine: fires alerts for HIGH severity or DANGER/WARNING safety events.
Broadcasts to all WebSocket clients via an injected broadcast function.
"""
import time
from collections import deque
from typing import Callable, Optional

# Conditions that trigger an alert
_SEVERITY_TRIGGER = {"HIGH"}
_SAFETY_TRIGGER = {"DANGER", "WARNING"}


class AlertEngine:
    def __init__(self, max_alerts: int = 200):
        self._alerts: deque = deque(maxlen=max_alerts)
        self._broadcast_fn: Optional[Callable] = None

    def set_broadcaster(self, fn: Callable) -> None:
        """Inject the async broadcast function (from WSManager)."""
        self._broadcast_fn = fn

    async def process(
        self,
        cam_name: str,
        result: dict,
        thumbnail_b64: Optional[str] = None,
    ) -> None:
        """Called for every VLM result. Fires alert when conditions are met."""
        severity = result.get("severity", "LOW")
        safety = result.get("safety", "UNKNOWN")

        if severity not in _SEVERITY_TRIGGER and safety not in _SAFETY_TRIGGER:
            return

        alert = {
            "cam": cam_name,
            "ts": time.time(),
            "severity": severity,
            "safety": safety,
            "observation": result.get("observation", ""),
            "activity": result.get("activity", ""),
            "workers": result.get("workers", "0"),
            "machinery": result.get("machinery", "None"),
            "thumbnail_b64": thumbnail_b64,
        }
        self._alerts.append(alert)

        if self._broadcast_fn:
            await self._broadcast_fn({"type": "alert", "data": alert})

    def get_recent(self, n: int = 50) -> list:
        alerts = list(self._alerts)
        return alerts[-n:] if len(alerts) > n else alerts

    def clear(self) -> None:
        self._alerts.clear()
