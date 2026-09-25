"""
RapidAlert — FastAPI Backend Application
Serves REST API + WebSocket + Live Telemetry + Static Dashboard Frontend.
"""
from __future__ import annotations

import asyncio
import base64
import json
import os
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import List, Optional

import cv2
import numpy as np

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query
from fastapi.responses import Response, StreamingResponse
from fastapi.staticfiles import StaticFiles

# ── Path setup ─────────────────────────────────────────────────────
# Ensure both backend root and rapidalert root are in python path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(ROOT))

# Centralized configuration & error tracking
from backend.core.config import (
    CONFIG_DIR,
    FRONTEND_DIR,
    DATABASE_PATH,
    CLIPS_DIR,
    SYSTEM_CONFIG_PATH,
    CAMERAS_CONFIG_PATH,
    PROMPTS_CONFIG_PATH,
    DEFAULT_DASHBOARD_PORT,
    PREVIEW_FRAME_WIDTH,
    PREVIEW_JPEG_QUALITY,
    HIGH_RES_FRAME_WIDTH,
    HIGH_RES_JPEG_QUALITY,
    TEMPORAL_THUMB_WIDTH,
    TEMPORAL_THUMB_QUALITY,
    config_manager,
)
from backend.core.error_tracker import error_tracker

# Schemas (Request and Response models)
from backend.schemas.requests import (
    CameraBody,
    SystemConfigBody,
    PromptBody,
    ScanBody,
    TestAlertBody,
)
from backend.schemas.responses import (
    StandardStatusResponse,
    SystemConfigResponse,
    ErrorListResponse,
    ErrorSummaryResponse,
)

# Services
from backend.services.camera_manager import CameraManager
from backend.services.frame_store import FrameStore
from backend.services.vlm_client import VLMPool
from backend.services.prompt_manager import PromptManager
from backend.services.result_store import ResultStore
from backend.services.alert_engine import AlertEngine
from backend.services.scheduler import DeadlineScheduler
from backend.services.ws_manager import WSManager
from backend.services.storage import StorageManager
from backend.services.rtsp_scanner import RTSPScanner
from backend.services.metrics_monitor import metrics_loop
from backend.services.scene_trigger import SceneTriggerEngine
from backend.services.watchdog import SystemWatchdog
from backend.core.shutdown_logger import log_system_event


# ══════════════════════════════════════════════════════════════════
#  Singletons Initialization
# ══════════════════════════════════════════════════════════════════

sys_cfg = config_manager.get()

frame_store = FrameStore(sys_cfg.frame_width, sys_cfg.jpeg_quality)
camera_manager = CameraManager(CAMERAS_CONFIG_PATH, frame_store)
vlm_pool = VLMPool([ep.model_dump() for ep in sys_cfg.vllm_endpoints])
result_store = ResultStore()
alert_engine = AlertEngine()
ws_manager = WSManager()
storage = StorageManager(DATABASE_PATH)
prompt_manager = PromptManager(
    PROMPTS_CONFIG_PATH,
    cameras_config_provider=camera_manager.get_config,
)
scheduler = DeadlineScheduler(
    camera_manager=camera_manager,
    frame_store=frame_store,
    vlm_pool=vlm_pool,
    prompt_manager=prompt_manager,
    result_store=result_store,
    alert_engine=alert_engine,
    initial_concurrency=sys_cfg.initial_concurrency,
    max_concurrency=sys_cfg.max_concurrency,
    broadcast_fn=ws_manager.broadcast,
    storage=storage,
    default_heartbeat_sec=sys_cfg.default_heartbeat_sec,
)
alert_engine.set_broadcaster(ws_manager.broadcast)
error_tracker.set_broadcaster(ws_manager.broadcast)

scene_trigger = SceneTriggerEngine(
    frame_store=frame_store,
    camera_manager=camera_manager,
    on_incident_callback=scheduler.queue_incident,
    broadcast_fn=ws_manager.broadcast,
    alert_engine=alert_engine,
    model_name=sys_cfg.dinov2_model,
    device="cpu",
    default_threshold=sys_cfg.scene_threshold,
    major_threshold=sys_cfg.dino_major_threshold,
    minor_threshold=sys_cfg.dino_minor_threshold,
    semantic_interval=sys_cfg.semantic_interval,
    event_cooldown=sys_cfg.event_cooldown,
)

watchdog = SystemWatchdog(
    camera_manager=camera_manager,
    frame_store=frame_store,
    vlm_pool=vlm_pool,
    ws_manager=ws_manager,
    interval_sec=15.0,
    broadcast_fn=ws_manager.broadcast,
)


# ══════════════════════════════════════════════════════════════════
#  Application Lifespan
# ══════════════════════════════════════════════════════════════════

_bg_tasks = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ─────────────────────────────────────────────────
    t_start = time.monotonic()
    camera_manager.sync()
    await vlm_pool.start()
    await scheduler.start()
    scene_trigger.start()
    watchdog.start()
    _bg_tasks.append(asyncio.create_task(prompt_manager.watch_loop()))
    _bg_tasks.append(asyncio.create_task(_config_sync_loop()))
    _bg_tasks.append(asyncio.create_task(metrics_loop(ws_manager.broadcast)))
    _bg_tasks.append(asyncio.create_task(_snapshot_stream_loop()))
    
    port = config_manager.get().dashboard_port
    print(f"\n[RapidAlert] ✅ Dashboard → http://localhost:{port}\n")
    yield
    # ── Shutdown ────────────────────────────────────────────────
    uptime = time.monotonic() - t_start
    summary = watchdog.get_summary()
    watchdog.stop()
    for task in _bg_tasks:
        task.cancel()
    scene_trigger.stop()
    await scheduler.stop()
    await vlm_pool.stop()
    camera_manager.stop_all()

    log_system_event(
        event_type="Shutdown",
        reason="Application Shutdown Triggered (Ctrl+C / SIGINT / SIGTERM)",
        uptime_sec=uptime,
        details={
            "Active Feeds": summary.get("cameras_active"),
            "vLLM Shards": summary.get("vlm_shards_healthy"),
            "Analyses Done": scheduler._total_analyzed,
            "Logged Errors": len(error_tracker.get_errors()),
        },
        action_required="None. All streams, models, and workers released cleanly. To resume: ./run.sh",
    )


async def _config_sync_loop() -> None:
    """Poll cameras.json and system.json for changes, hot-apply to engines, and notify WS clients."""
    sys_path = SYSTEM_CONFIG_PATH
    last_sys_mtime = 0.0
    try:
        if sys_path.exists():
            last_sys_mtime = os.path.getmtime(sys_path)
    except Exception as exc:
        error_tracker.capture_exception(
            exc,
            component="ConfigManager",
            effect="Could not check initial system.json timestamp",
            severity="WARNING",
        )

    while True:
        try:
            await asyncio.sleep(2)
            # 1. Camera changes
            if camera_manager.sync():
                await ws_manager.broadcast({
                    "type": "cameras",
                    "data": camera_manager.get_config(),
                })

            # 2. System config changes
            if sys_path.exists():
                mtime = os.path.getmtime(sys_path)
                if mtime > last_sys_mtime:
                    last_sys_mtime = mtime
                    cfg = config_manager.load()
                    
                    scene_trigger.set_default_threshold(cfg.default_threshold)
                    scene_trigger.set_thresholds(cfg.dino_major_threshold, cfg.dino_minor_threshold)
                    scheduler.default_heartbeat_sec = cfg.default_heartbeat_sec
                    scene_trigger.event_cooldown = cfg.event_cooldown
                    scene_trigger.semantic_interval = cfg.semantic_interval
                    scheduler.followup_interval_sec = cfg.followup_interval_sec
                    scheduler.persistent_followup = cfg.persistent_followup

                    print(
                        f"[Main] 🔄 Hot-reloaded system.json (major_thresh: {cfg.dino_major_threshold}, "
                        f"minor_thresh: {cfg.dino_minor_threshold}, hb: {cfg.default_heartbeat_sec}s, "
                        f"followup: {scheduler.followup_interval_sec}s, persistent: {scheduler.persistent_followup})"
                    )
                    await ws_manager.broadcast({
                        "type": "config_updated",
                        "system": config_manager.as_dict(),
                        "cameras": camera_manager.get_config(),
                    })
        except asyncio.CancelledError:
            break
        except Exception as e:
            error_tracker.capture_exception(
                e,
                component="ConfigManager",
                effect="Error during periodic config sync loop; pausing 2s",
                severity="WARNING",
            )
            await asyncio.sleep(2.0)


async def _snapshot_stream_loop() -> None:
    """Broadcast live camera snapshots over WebSocket for fluid grid streaming."""
    last_pushed_ts: dict[str, float] = {}
    while True:
        try:
            if not ws_manager.has_clients:
                await asyncio.sleep(0.5)
                continue

            await asyncio.sleep(0.2)  # 5 FPS balanced refresh (silky smooth, zero UI lag)
            active = camera_manager.get_active_cameras()
            for cam in active:
                latest = frame_store.get_latest(cam)
                if latest is not None:
                    _, ts = latest
                    if ts > last_pushed_ts.get(cam, 0.0):
                        last_pushed_ts[cam] = ts
                        snap = frame_store.get_cached_snapshot_b64(cam)
                        if snap:
                            await ws_manager.broadcast({
                                "type": "camera_frame",
                                "cam": cam,
                                "thumbnail_b64": snap,
                            })
        except asyncio.CancelledError:
            break
        except Exception as exc:
            error_tracker.capture_exception(
                exc,
                component="CameraManager",
                effect="Error in live snapshot broadcast loop; pausing 1s",
                severity="WARNING",
            )
            await asyncio.sleep(1.0)


# ══════════════════════════════════════════════════════════════════
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="RapidAlert",
    version="2.0.0",
    description="High-Speed Hybrid VLM Surveillance & Incident Detection Engine",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ══════════════════════════════════════════════════════════════════
#  WebSocket Endpoint
# ══════════════════════════════════════════════════════════════════

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws_manager.connect(ws)
    try:
        # Send live snapshots for all active cameras so preview is never blank
        active_cams = camera_manager.get_active_cameras()
        live_thumbs = {}
        for c in active_cams:
            snap = frame_store.get_snapshot_b64(
                c, max_w=PREVIEW_FRAME_WIDTH, quality=PREVIEW_JPEG_QUALITY
            )
            if snap:
                live_thumbs[c] = [snap]

        # Send full initial state so the dashboard can render immediately
        await ws.send_json({
            "type": "init",
            "cameras": camera_manager.get_config(),
            "results": result_store.get_all_latest(),
            "concurrent_results": result_store.get_all_latest_concurrent(),
            "thumbnails": live_thumbs,
            "drifts": scene_trigger.latest_drifts,
            "alerts": alert_engine.get_recent(25, summary=True),
            "system": config_manager.as_dict(),
            "metrics": {
                **result_store.get_metrics(),
                "concurrency": scheduler.concurrency,
                "queue_depth": scheduler.queue_depth,
                "in_flight": scheduler.in_flight_count,
                "vlm_shards": vlm_pool.get_stats(),
                "vlm_mode": "mig" if vlm_pool.is_mig() else "shared",
                "is_mig": vlm_pool.is_mig(),
            },
            "prompts": {
                "master": prompt_manager.get_master(),
                "cameras": prompt_manager.get_cam_overrides(),
            },
            "recent_errors": error_tracker.get_recent(limit=10),
        })

        # Keep connection alive with periodic pings
        while True:
            await asyncio.sleep(25)
            await ws.send_json({"type": "ping"})
    except WebSocketDisconnect:
        pass
    except asyncio.CancelledError:
        pass
    except Exception as exc:
        error_tracker.capture_exception(
            exc,
            component="WebSocketManager",
            effect="Unhandled exception in WebSocket client loop; closing socket",
            severity="WARNING",
        )
    finally:
        await ws_manager.disconnect(ws)


# ══════════════════════════════════════════════════════════════════
#  REST — Cameras
# ══════════════════════════════════════════════════════════════════

@app.get("/api/cameras")
def api_get_cameras():
    return camera_manager.get_config()


@app.get("/api/drifts")
def api_get_drifts():
    return scene_trigger.latest_drifts


@app.post("/api/cameras")
async def api_upsert_camera(cam: CameraBody):
    camera_manager.update_camera(cam.model_dump())
    _persist_cameras()
    await ws_manager.broadcast({
        "type": "cameras",
        "data": camera_manager.get_config(),
    })
    return {"status": "ok", "cameras": camera_manager.get_config()}


@app.post("/api/cameras/batch")
async def api_batch_update_cameras(cams: List[CameraBody]):
    for cam in cams:
        camera_manager.update_camera(cam.model_dump())
    _persist_cameras()
    await ws_manager.broadcast({
        "type": "cameras",
        "data": camera_manager.get_config(),
    })
    return {"status": "ok", "cameras": camera_manager.get_config()}


@app.delete("/api/cameras/{name}")
async def api_delete_camera(name: str):
    camera_manager.remove_camera(name)
    _persist_cameras()
    await ws_manager.broadcast({
        "type": "cameras",
        "data": camera_manager.get_config(),
    })
    return {"status": "ok"}


def _persist_cameras() -> None:
    try:
        with open(CAMERAS_CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(camera_manager.get_config(), f, indent=2)
    except Exception as e:
        error_tracker.capture_exception(
            e,
            component="CameraManager",
            effect=f"Failed to persist camera config to {CAMERAS_CONFIG_PATH}",
            severity="ERROR",
        )


# ══════════════════════════════════════════════════════════════════
#  REST — Configuration
# ══════════════════════════════════════════════════════════════════

@app.get("/api/config")
def api_get_config():
    return {
        "system": config_manager.as_dict(),
        "cameras": camera_manager.get_config(),
    }


@app.post("/api/config")
@app.patch("/api/config")
@app.post("/api/config/system")
@app.patch("/api/config/system")
async def api_update_system_config(body: SystemConfigBody):
    updates = body.model_dump(exclude_none=True)
    if not updates:
        return {"status": "ok", "system": config_manager.as_dict()}

    try:
        cfg = config_manager.update(updates)
        if "dino_major_threshold" in updates or "dino_minor_threshold" in updates:
            scene_trigger.set_thresholds(
                float(cfg.dino_major_threshold),
                float(cfg.dino_minor_threshold),
            )
        if "default_threshold" in updates or "scene_threshold" in updates:
            scene_trigger.set_default_threshold(cfg.default_threshold)
        if "default_heartbeat_sec" in updates:
            scheduler.default_heartbeat_sec = float(cfg.default_heartbeat_sec)
        if "event_cooldown" in updates:
            scene_trigger.event_cooldown = float(cfg.event_cooldown)
        if "semantic_interval" in updates:
            scene_trigger.semantic_interval = float(cfg.semantic_interval)
        if "followup_interval_sec" in updates:
            scheduler.followup_interval_sec = float(cfg.followup_interval_sec)
        if "persistent_followup" in updates:
            scheduler.persistent_followup = bool(cfg.persistent_followup)
        if "clip_retention_hours" in updates:
            scheduler.clip_retention_hours = float(cfg.clip_retention_hours)
        if "clip_rolling_buffer_enabled" in updates:
            scheduler.clip_rolling_buffer_enabled = bool(cfg.clip_rolling_buffer_enabled)
        if "clip_recording_enabled" in updates:
            if hasattr(alert_engine, "clip_recorder") and alert_engine.clip_recorder:
                alert_engine.clip_recorder.enabled = bool(cfg.clip_recording_enabled)

        await ws_manager.broadcast({
            "type": "config_updated",
            "system": config_manager.as_dict(),
            "cameras": camera_manager.get_config(),
        })
        return {"status": "ok", "system": config_manager.as_dict()}
    except Exception as e:
        error_tracker.capture_exception(
            e,
            component="ConfigManager",
            effect="Failed to update and persist system configuration",
            severity="ERROR",
        )
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════════════════════════
#  REST — Frames & MJPEG Stream
# ══════════════════════════════════════════════════════════════════

def _get_placeholder_jpeg(name: str, width: int = 640, height: int = 360) -> bytes:
    img = np.zeros((height, width, 3), dtype=np.uint8)
    img[:] = (18, 18, 24)
    cv2.putText(img, f"Connecting to {name}...", (30, height // 2), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (120, 140, 160), 2, cv2.LINE_AA)
    ok, buf = cv2.imencode(".jpg", img, [cv2.IMWRITE_JPEG_QUALITY, 70])
    return buf.tobytes() if ok else b""


@app.get("/api/cameras/{name}/frame")
async def api_get_frame(name: str, width: int = 640, quality: int = 80):
    for _ in range(12):
        b64 = frame_store.get_snapshot_b64(name, max_w=width, quality=quality)
        if b64 is not None:
            return Response(
                content=base64.b64decode(b64),
                media_type="image/jpeg",
                headers={"Cache-Control": "no-cache"},
            )
        await asyncio.sleep(0.1)

    ph_bytes = _get_placeholder_jpeg(name, width=width)
    return Response(
        content=ph_bytes,
        media_type="image/jpeg",
        headers={"Cache-Control": "no-cache"},
    )


@app.get("/api/cameras/{name}/stream")
async def api_camera_stream(name: str, width: int = 640, quality: int = 70):
    """Continuous real-time MJPEG live video stream (25+ FPS, multipart/x-mixed-replace)."""
    async def frame_generator():
        last_ts = 0.0
        try:
            while True:
                entry = frame_store.get_latest_jpeg_entry(name, max_w=width, quality=quality)
                if entry is not None:
                    ts, jpeg_bytes = entry
                    if ts > last_ts:
                        last_ts = ts
                        yield (
                            b"--frame\r\n"
                            b"Content-Type: image/jpeg\r\n\r\n" + jpeg_bytes + b"\r\n"
                        )
                await asyncio.sleep(0.02)  # 50Hz poll for instant push as soon as camera thread writes
        except (asyncio.CancelledError, GeneratorExit):
            pass

    return StreamingResponse(
        frame_generator(),
        media_type="multipart/x-mixed-replace; boundary=frame",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
        },
    )


# ══════════════════════════════════════════════════════════════════
#  REST — Results / Status
# ══════════════════════════════════════════════════════════════════

@app.get("/api/status")
def api_get_status():
    return result_store.get_all_latest()


@app.get("/api/cameras/{name}/history")
def api_get_history(name: str, limit: int = 50, offset: int = 0):
    """Persistent history from SQLite (survives restarts)."""
    return storage.query(cam=name, limit=limit, offset=offset)


@app.get("/api/history")
def api_get_all_history(
    cam: Optional[str] = None,
    severity: Optional[str] = None,
    safety: Optional[str] = None,
    search: Optional[str] = None,
    incident_id: Optional[str] = None,
    trigger_mode: Optional[str] = None,
    since: Optional[float] = None,
    until: Optional[float] = None,
    limit: int = 100,
    offset: int = 0,
):
    """Filtered history query across all cameras with full-text keyword and metadata search."""
    return storage.query(
        cam=cam,
        since_ts=since,
        until_ts=until,
        severity=severity,
        safety=safety,
        search=search,
        incident_id=incident_id,
        trigger_mode=trigger_mode,
        limit=limit,
        offset=offset,
    )


@app.get("/api/storage/stats")
def api_storage_stats():
    return storage.get_stats()


@app.get("/api/cameras/{name}/stats")
def api_cam_stats(name: str, hours: float = 24):
    return storage.get_camera_summary(name, hours=hours)


# ══════════════════════════════════════════════════════════════════
#  REST — Prompts
# ══════════════════════════════════════════════════════════════════

@app.get("/api/prompts")
def api_get_prompts():
    return {
        "master": prompt_manager.get_master(),
        "followup": prompt_manager._followup,
        "cameras": prompt_manager.get_cam_overrides(),
    }


@app.post("/api/prompts")
async def api_update_prompts(body: PromptBody):
    prompt_manager.save(
        master=body.master,
        followup=body.followup,
        cam_name=body.cam_name,
        cam_prompt=body.cam_prompt,
    )
    await ws_manager.broadcast({
        "type": "prompts",
        "data": {
            "master": prompt_manager.get_master(),
            "followup": prompt_manager._followup,
            "cameras": prompt_manager.get_cam_overrides(),
        },
    })
    return {"status": "ok"}


# ══════════════════════════════════════════════════════════════════
#  REST — Alerts
# ══════════════════════════════════════════════════════════════════

@app.get("/api/alerts")
def api_get_alerts(n: int = 50):
    return alert_engine.get_recent(n)


@app.get("/api/alerts/{alert_id}")
def api_get_single_alert(alert_id: str):
    alert = alert_engine.get_alert(alert_id)
    if alert:
        return alert
    raise HTTPException(status_code=404, detail="Alert not found")


@app.delete("/api/alerts")
async def api_clear_alerts():
    alert_engine.clear()
    return {"status": "ok"}


@app.post("/api/alerts/test")
async def api_trigger_test_alert(cam: Optional[str] = None):
    """Trigger an immediate test alert to verify notification feed and inspector."""
    active = camera_manager.get_active_cameras()
    cam_name = cam if (cam and cam in active) else (active[0] if active else "TEST_CAM")
    snap = frame_store.get_snapshot_b64(
        cam_name, max_w=HIGH_RES_FRAME_WIDTH, quality=HIGH_RES_JPEG_QUALITY
    )
    temporal_snaps = frame_store.get_temporal_snapshots_b64(
        cam_name, count=4, span_sec=10.0, max_w=TEMPORAL_THUMB_WIDTH, quality=TEMPORAL_THUMB_QUALITY
    )
    labels = ["t -10.0s", "t -5.0s", "t -2.0s", "t 0.0s (Trigger)"]
    alert = await alert_engine.process(
        cam_name=cam_name,
        result={
            "severity": "HIGH",
            "safety": "WARNING",
            "activity": "MOTION_TEST",
            "workers": "2",
            "machinery": "None",
            "observation": f"Test Incident: Detected active movement and safety inspection trigger on {cam_name}. Verified alert delivery pipeline.",
            "latency": 1.15,
            "e2e_latency": 1.35,
        },
        thumbnail_b64=snap,
        thumbnails_b64=temporal_snaps if temporal_snaps else ([snap] if snap else []),
        is_incident=True,
        labels=labels,
        drift=0.0482,
        e2e_latency=1.35,
        latency=1.15,
    )
    if alert:
        asyncio.create_task(
            scheduler._schedule_followup(
                cam_name=cam_name,
                incident_id=alert["incident_id"],
                parent_id=alert["id"],
                drift_score=0.0482,
                delay_sec=scheduler.followup_interval_sec,
                prev_observation=alert.get("observation", ""),
                prev_severity=alert.get("severity", "HIGH"),
                cycle=1,
            )
        )
    return {"status": "ok", "alert": alert}


# ══════════════════════════════════════════════════════════════════
#  REST — System Diagnostics & Error Tracking
# ══════════════════════════════════════════════════════════════════

@app.get("/api/errors", response_model=ErrorListResponse)
def api_get_errors(
    limit: int = Query(50, ge=1, le=500),
    component: Optional[str] = None,
    camera: Optional[str] = None,
    severity: Optional[str] = None,
):
    """Retrieve recorded system exceptions and operational impact records."""
    records = error_tracker.get_recent(
        limit=limit,
        component=component,
        camera=camera,
        severity=severity,
    )
    return {"count": len(records), "errors": records}


@app.get("/api/errors/summary", response_model=ErrorSummaryResponse)
def api_get_errors_summary():
    """Retrieve aggregate statistics on system errors grouped by component and effect."""
    return error_tracker.get_summary()


@app.delete("/api/errors", response_model=StandardStatusResponse)
def api_clear_errors():
    """Clear error history from memory and SQLite log."""
    error_tracker.clear()
    return {"status": "ok", "message": "Error log cleared successfully"}


# ══════════════════════════════════════════════════════════════════
#  REST — Metrics & Health
# ══════════════════════════════════════════════════════════════════

@app.get("/api/metrics")
def api_get_metrics():
    m = result_store.get_metrics()
    shards = vlm_pool.get_stats()
    is_mig = any(s.get("is_mig") for s in shards)
    m.update({
        "concurrency": scheduler.concurrency,
        "queue_depth": scheduler.queue_depth,
        "in_flight": scheduler.in_flight_count,
        "vlm_shards": shards,
        "vlm_mode": "mig" if is_mig else "shared",
        "is_mig": is_mig,
    })
    return m


@app.get("/api/health/vlm")
async def api_vlm_health():
    return await vlm_pool.health_check()


# ══════════════════════════════════════════════════════════════
#  REST — RTSP / ONVIF Scanner
# ══════════════════════════════════════════════════════════════

_scan_lock = asyncio.Lock()
_scan_running = False


@app.get("/api/scanner/subnet")
def api_scanner_subnet():
    """Returns the auto-detected local /24 subnet."""
    return {"subnet": RTSPScanner.local_subnet()}


@app.post("/api/scanner/scan")
async def api_scan(body: ScanBody):
    """Run ONVIF WS-Discovery + TCP port 554 scan. May take 3-5 seconds."""
    global _scan_running
    async with _scan_lock:
        if _scan_running:
            raise HTTPException(status_code=409, detail="Scan already running")
        _scan_running = True
    try:
        creds = (body.username, body.password) if body.username else None
        results = await RTSPScanner.full_scan(
            subnet=body.subnet,
            ws_timeout=body.ws_timeout,
            port_timeout=body.port_timeout,
            credentials=creds,
        )
        return {"found": results, "count": len(results)}
    finally:
        async with _scan_lock:
            _scan_running = False



# ══════════════════════════════════════════════════════════════
#  VLM Pool telemetry & Container Inspector
# ══════════════════════════════════════════════════════════════

@app.get("/api/vlm/stats")
async def get_vlm_stats():
    """
    Per-MIG/shared-shard live telemetry:
    queued items, in-flight requests, completed count, error count,
    avg/p95 latency, health flag, weight, active jobs, and load_score.
    """
    shards = vlm_pool.get_stats()
    is_mig = any(s.get("is_mig") for s in shards)
    return {
        "is_mig": is_mig,
        "mode": "mig" if is_mig else "shared",
        "shards": shards,
    }


@app.get("/api/vlm/containers")
async def get_vlm_containers():
    """Detailed live telemetry for VLM docker containers and MIG shards."""
    shards_stats = vlm_pool.get_stats()
    is_mig = vlm_pool.is_mig()
    
    # Fast non-blocking Docker inspection
    docker_status = {}
    try:
        proc = await asyncio.create_subprocess_exec(
            "docker", "ps", "-a", "--filter", "name=rapidalert_vllm", "--format", "{{.Names}}\t{{.Status}}\t{{.Image}}\t{{.Ports}}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=2.0)
        for line in stdout.decode().strip().split("\n"):
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) >= 2:
                name = parts[0].strip()
                docker_status[name] = {
                    "name": name,
                    "status": parts[1].strip(),
                    "image": parts[2].strip() if len(parts) > 2 else "",
                    "ports": parts[3].strip() if len(parts) > 3 else "",
                    "running": parts[1].strip().lower().startswith("up"),
                }
    except Exception:
        pass

    enriched = []
    for s in shards_stats:
        cname = s.get("container_name") or f"rapidalert_vllm_{s.get('port', '')[-1]}"
        dinfo = docker_status.get(cname) or docker_status.get(f"rapidalert_vllm_{0 if s.get('port')=='8000' else 1}") or {}
        enriched.append({
            **s,
            "container": dinfo,
        })

    return {
        "mode": "mig" if is_mig else "shared",
        "is_mig": is_mig,
        "shards": enriched,
        "total_inferences": sum(s.get("completed", 0) for s in shards_stats),
        "total_errors": sum(s.get("errors", 0) for s in shards_stats),
    }


@app.post("/api/vlm/probe")
@app.get("/api/vlm/health")
async def probe_vlm_endpoints():
    """Trigger manual ping, health check and latency probe on all vLLM shards."""
    probe_results = await vlm_pool.probe()
    return {
        "health": {r["url"]: r["healthy"] for r in probe_results},
        "probe": probe_results,
    }


# ══════════════════════════════════════════════════════════════
#  Static files & frontend — mount LAST
# ══════════════════════════════════════════════════════════════

app.mount("/clips", StaticFiles(directory=str(CLIPS_DIR)), name="clips")
app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")

