"""
AlertEngine: fires and logs alerts for AI incidents and scene drift shift triggers.
Reads alert conditions dynamically from system configuration (no hardcoded severity sets).
Broadcasts to all WebSocket clients via an injected broadcast function.
"""
from __future__ import annotations

import time
from collections import deque
from typing import Callable, Optional, List

from backend.core.config import DEFAULT_VLM_MODEL, config_manager
from backend.core.error_tracker import error_tracker


class AlertEngine:
    def __init__(self, max_alerts: int = 200):
        self._alerts: deque = deque(maxlen=max_alerts)
        self._broadcast_fn: Optional[Callable] = None

    def set_broadcaster(self, fn: Callable) -> None:
        """Inject the async broadcast function (from WSManager)."""
        self._broadcast_fn = fn

    def _get_trigger_conditions(self):
        """Dynamic retrieval of alert trigger levels from configuration."""
        cfg = config_manager.get()
        return set(cfg.alert_severity_triggers), set(cfg.alert_safety_triggers)

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
            try:
                await self._broadcast_fn(payload)
            except Exception as exc:
                error_tracker.capture_exception(
                    exc,
                    component="AlertEngine",
                    camera=cam_name,
                    effect=f"Failed to broadcast scene shift event for {cam_name}",
                    severity="WARNING",
                )
        return payload

    async def process(
        self,
        cam_name: str,
        result: dict,
        thumbnail_b64: Optional[str] = None,
        thumbnails_b64: Optional[List[str]] = None,
        is_incident: bool = False,
        is_followup: bool = False,
        incident_id: Optional[str] = None,
        parent_id: Optional[str] = None,
        labels: Optional[List[str]] = None,
        drift: Optional[float] = None,
        e2e_latency: Optional[float] = None,
        latency: Optional[float] = None,
        cycle: int = 1,
        delay_sec: float = 10.0,
        clip_path: Optional[str] = None,
    ) -> Optional[dict]:
        """Called for every VLM result. Fires alert with actual scene analysis when conditions are met."""
        if result.get("verdict") == "WARMUP":
            return None

        raw_sev = (result.get("severity") or "LOW").upper()
        raw_safety = (result.get("safety") or "UNKNOWN").upper()

        severity_triggers, safety_triggers = self._get_trigger_conditions()

        is_alert = (
            raw_sev in severity_triggers
            or raw_safety in safety_triggers
            or is_incident
            or is_followup
        )

        if not is_alert:
            return None

        # Effective severity & safety:
        # Respect camera-specific rules and objective model evaluations
        if is_followup:
            severity = raw_sev
            safety = raw_safety
        elif is_incident:
            # If evaluated as normal/routine (LOW / OK), preserve it
            if raw_sev == "LOW" and raw_safety in ("OK", "UNKNOWN"):
                severity = "LOW"
                safety = "OK"
            else:
                severity = raw_sev if raw_sev in severity_triggers else "MEDIUM"
                safety = raw_safety if raw_safety in safety_triggers else "WARNING"
        else:
            severity = raw_sev
            safety = raw_safety

        ts_code = time.strftime("%Y%m%d%H%M%S")
        unique_suffix = f"{int(time.time() * 1000) % 1000:03d}"
        cam_slug = cam_name.upper().replace(" ", "_")

        if is_followup:
            trigger_mode = "FOLLOWUP"
            cycle_suffix = f"-C{cycle}" if cycle > 1 else ""
            event_id = f"EVT-{cam_slug}-{ts_code}-{unique_suffix}-FOLLOWUP{cycle_suffix}"
            badge_delay = f"+{int(delay_sec)}s"
            trigger_badge = f"🔄 FOLLOW-UP #{cycle} ({badge_delay})" if cycle > 1 else f"🔄 FOLLOW-UP ({badge_delay})"
            if not incident_id and parent_id:
                # Inherit incident_id from parent if available
                for a in self._alerts:
                    if a.get("id") == parent_id:
                        incident_id = a.get("incident_id")
                        break
            if not incident_id:
                incident_id = f"INC-{cam_slug}-{ts_code}-{unique_suffix}"
        elif is_incident:
            trigger_mode = "TRIGGER"
            trigger_badge = "⚡ TRIGGER"
            event_id = f"EVT-{cam_slug}-{ts_code}-{unique_suffix}-TRIGGER"
            if not incident_id:
                incident_id = f"INC-{cam_slug}-{ts_code}-{unique_suffix}"
        else:
            trigger_mode = "PERIODIC"
            trigger_badge = "⏱️ PERIODIC"
            event_id = f"EVT-{cam_slug}-{ts_code}-{unique_suffix}-PERIODIC"
            if not incident_id:
                incident_id = f"PER-{cam_slug}-{ts_code}-{unique_suffix}"

        # Link to parent alert if this is a follow-up
        if is_followup and parent_id:
            for a in self._alerts:
                if a.get("id") == parent_id:
                    a["followup_id"] = event_id
                    break

        # Generate clip if not provided but multi-frame thumbnails are present
        final_clip_path = clip_path
        if not final_clip_path and thumbnails_b64 and len(thumbnails_b64) >= 2:
            try:
                from backend.services.clip_recorder import clip_recorder
                final_clip_path = await clip_recorder.async_create_clip(
                    cam_name=cam_name,
                    event_id=event_id,
                    frames=thumbnails_b64,
                )
            except Exception as exc:
                error_tracker.capture_exception(
                    exc,
                    component="AlertEngine",
                    camera=cam_name,
                    effect=f"Failed to record incident clip for {event_id}",
                    severity="WARNING",
                )

        alert = {
            "id": event_id,
            "incident_id": incident_id,
            "parent_id": parent_id,
            "followup_id": None,
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
            "model": result.get("model", DEFAULT_VLM_MODEL),
            "is_incident": is_incident,
            "is_followup": is_followup,
            "is_periodic": not is_incident and not is_followup,
            "cycle": cycle if is_followup else 0,
            "delay_sec": delay_sec if is_followup else 0.0,
            "is_drift": False,
            "drift": drift,
            "latency": latency or result.get("latency"),
            "e2e_latency": e2e_latency or result.get("e2e_latency"),
            "clip_path": final_clip_path,
            "thumbnail_b64": thumbnail_b64 or (thumbnails_b64[-1] if thumbnails_b64 else None),
            "thumbnails_b64": thumbnails_b64 or [],
            "labels": labels or [],
        }
        self._alerts.append(alert)

        if self._broadcast_fn:
            try:
                await self._broadcast_fn({"type": "alert", "data": alert})
                if is_followup and parent_id:
                    await self._broadcast_fn({
                        "type": "alert_linked",
                        "parent_id": parent_id,
                        "followup_id": event_id,
                        "incident_id": incident_id,
                    })
            except Exception as exc:
                error_tracker.capture_exception(
                    exc,
                    component="AlertEngine",
                    camera=cam_name,
                    effect=f"Failed to broadcast alert {event_id} over WebSocket",
                    severity="WARNING",
                )

        return alert

    def get_alert(self, alert_id: str) -> Optional[dict]:
        """Fetch full alert data by ID including multi-frame sequence."""
        for a in self._alerts:
            if a.get("id") == alert_id:
                return a
        return None

    def get_recent(self, n: int = 50, summary: bool = False) -> list:
        # Filter out any legacy drift jargon alerts so only real VLM analyses are displayed
        alerts = [
            a for a in self._alerts
            if not a.get("is_drift") and not str(a.get("observation", "")).startswith("⚡ DINOv2")
        ]
        recent = alerts[-n:] if len(alerts) > n else alerts
        # Return newest first (chronological descending) so API consumers get the latest alerts first
        recent = list(reversed(recent))
        if not summary:
            return recent
        summaries = []
        for a in recent:
            item = dict(a)
            # Remove multi-frame temporal array to keep payload lightweight and prevent WS 1009 drops
            item["thumbnails_b64"] = []
            summaries.append(item)
        return summaries

    def clear(self) -> None:
        self._alerts.clear()
