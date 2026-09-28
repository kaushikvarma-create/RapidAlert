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
import threading
import time
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from pathlib import Path
from typing import List, Optional

import cv2
import numpy as np
import warnings

# Suppress harmless PyTorch/Python 3.12 POSIX semaphore cleanup notice at exit
warnings.filterwarnings("ignore", message=".*resource_tracker.*", category=UserWarning)
os.environ.setdefault("PYTHONWARNINGS", "ignore:resource_tracker:UserWarning")

from pydantic import BaseModel
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query, Request, Depends, Body
from fastapi.responses import Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from starlette.types import ASGIApp, Receive, Scope, Send

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
    LoginBody,
    SetupAdminBody,
    ChangePasswordBody,
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
from backend.services.auth_service import AuthService
from backend.services.reporting_service import ReportingService
from backend.services.rtsp_scanner import RTSPScanner
from backend.services.metrics_monitor import metrics_loop
from backend.services.scene_trigger import SceneTriggerEngine
from backend.services.watchdog import SystemWatchdog
from backend.services.watchdog_emailer import WatchdogEmailer
from backend.services.mediamtx_service import mediamtx_service, get_safe_cam_slug
from backend.core.shutdown_logger import log_system_event


# ══════════════════════════════════════════════════════════════════
#  Singletons Initialization & Security
# ══════════════════════════════════════════════════════════════════

sys_cfg = config_manager.get()

frame_store = FrameStore(sys_cfg.frame_width, sys_cfg.jpeg_quality)
camera_manager = CameraManager(CAMERAS_CONFIG_PATH, frame_store)
vlm_pool = VLMPool([ep.model_dump() for ep in sys_cfg.vllm_endpoints])
result_store = ResultStore()
alert_engine = AlertEngine()
ws_manager = WSManager()
storage = StorageManager(DATABASE_PATH)
auth_service = AuthService(DATABASE_PATH)
reporting_service = ReportingService(DATABASE_PATH, SYSTEM_CONFIG_PATH, vlm_pool, frame_store)
watchdog_emailer = WatchdogEmailer(
    db_path=DATABASE_PATH,
    config_path=SYSTEM_CONFIG_PATH,
    camera_manager=camera_manager,
    frame_store=frame_store,
    vlm_pool=vlm_pool,
)
prompt_manager = PromptManager(
    PROMPTS_CONFIG_PATH,
    cameras_config_provider=camera_manager.get_config,
)

security = HTTPBearer(auto_error=False)


async def require_admin(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    request: Request = None,
) -> str:
    """Security guard verifying valid PBKDF2 authenticated session token."""
    token = None
    if credentials:
        token = credentials.credentials
    elif request and "authorization" in request.headers:
        hdr = request.headers.get("authorization", "")
        if hdr.startswith("Bearer "):
            token = hdr[7:]
    elif request and "x-admin-token" in request.headers:
        token = request.headers.get("x-admin-token")

    username = auth_service.validate_session(token) if token else None
    if not username:
        raise HTTPException(
            status_code=401,
            detail="Admin authentication required to access settings and modify configuration",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return username
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
error_tracker.set_watchdog_emailer(watchdog_emailer)

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
    watchdog_emailer=watchdog_emailer,
    interval_sec=15.0,
    broadcast_fn=ws_manager.broadcast,
)


# ══════════════════════════════════════════════════════════════════
#  Application Lifespan
# ══════════════════════════════════════════════════════════════════

_bg_tasks = []
_is_shutting_down = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _is_shutting_down
    _is_shutting_down = False
    # ── Startup ─────────────────────────────────────────────────
    t_start = time.monotonic()
    # Audit prior crash / unexpected power outage state
    watchdog_emailer.check_and_alert_prior_crash()
    camera_manager.sync()
    await vlm_pool.start()
    await scheduler.start()
    scene_trigger.start()
    watchdog.start()
    reporting_service.start()
    mediamtx_service.start(camera_manager.get_config())
    _bg_tasks.append(asyncio.create_task(prompt_manager.watch_loop()))
    _bg_tasks.append(asyncio.create_task(_config_sync_loop()))
    _bg_tasks.append(asyncio.create_task(metrics_loop(ws_manager.broadcast)))
    
    port = config_manager.get().dashboard_port
    print(f"\n[RapidAlert] ✅ Dashboard → http://localhost:{port}\n")
    yield
    # ── Shutdown ────────────────────────────────────────────────
    _is_shutting_down = True
    watchdog_emailer.mark_clean_shutdown(reason="Application Shutdown Triggered (Ctrl+C / SIGINT / SIGTERM)")
    await ws_manager.close_all()
    uptime = time.monotonic() - t_start
    summary = watchdog.get_summary()
    watchdog.stop()
    reporting_service.stop()
    mediamtx_service.stop()
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





# ══════════════════════════════════════════════════════════════════
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="RapidAlert",
    version="2.0.0",
    description="High-Speed Hybrid VLM Surveillance & Incident Detection Engine",
    lifespan=lifespan,
)

class GracefulShutdownMiddleware:
    """Catches CancelledError when uvicorn cancels streaming connections on graceful shutdown timeout."""
    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        try:
            await self.app(scope, receive, send)
        except (asyncio.CancelledError, GeneratorExit):
            pass

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(GracefulShutdownMiddleware)


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return Response(status_code=204)


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
                "master_scene_context": prompt_manager.get_master_scene_context(),
                "followup": prompt_manager._followup,
                "cameras": prompt_manager.get_cam_overrides(),
            },
            "recent_errors": error_tracker.get_recent(limit=10),
        })

        with _stream_tracker_lock:
            total_active = sum(_active_display_streams.values())
            cams_active = list(_active_display_streams.keys())
            counts_active = dict(_active_display_streams)
        await ws.send_json({
            "type": "active_streams_count",
            "active_streams": total_active,
            "active_cams": cams_active,
            "stream_counts": counts_active,
        })

        # Keep connection alive with periodic pings
        while True:
            await asyncio.sleep(25)
            await ws.send_json({"type": "ping"})
    except (WebSocketDisconnect, RuntimeError):
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
#  REST — Admin Authentication & Security
# ══════════════════════════════════════════════════════════════════

@app.get("/api/auth/status")
def api_auth_status(request: Request, credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)):
    token = None
    if credentials:
        token = credentials.credentials
    elif "authorization" in request.headers:
        hdr = request.headers.get("authorization", "")
        if hdr.startswith("Bearer "):
            token = hdr[7:]
    elif "x-admin-token" in request.headers:
        token = request.headers.get("x-admin-token")

    username = auth_service.validate_session(token) if token else None
    return {
        "configured": auth_service.is_configured(),
        "authenticated": username is not None,
        "username": username,
    }


@app.post("/api/auth/login")
def api_auth_login(body: LoginBody, request: Request):
    client_ip = request.client.host if request.client else "unknown"
    ok, token, err = auth_service.authenticate(body.username, body.password, client_ip=client_ip)
    if not ok:
        raise HTTPException(status_code=401, detail=err or "Invalid credentials")
    return {
        "status": "ok",
        "token": token,
        "username": body.username,
    }


@app.post("/api/auth/setup")
def api_auth_setup(body: SetupAdminBody):
    if auth_service.is_configured():
        raise HTTPException(status_code=400, detail="Admin credentials already configured")
    ok = auth_service.create_or_update_user(body.username, body.password)
    if not ok:
        raise HTTPException(status_code=400, detail="Password must be at least 4 characters")
    ok, token, _ = auth_service.authenticate(body.username, body.password)
    return {
        "status": "ok",
        "token": token,
        "username": body.username,
    }


@app.post("/api/auth/logout")
def api_auth_logout(request: Request, credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)):
    token = None
    if credentials:
        token = credentials.credentials
    elif "authorization" in request.headers:
        hdr = request.headers.get("authorization", "")
        if hdr.startswith("Bearer "):
            token = hdr[7:]
    elif "x-admin-token" in request.headers:
        token = request.headers.get("x-admin-token")
    if token:
        auth_service.revoke_session(token)
    return {"status": "ok"}


@app.post("/api/auth/change-password")
def api_auth_change_password(body: ChangePasswordBody, admin_user: str = Depends(require_admin)):
    ok, err = auth_service.change_password(admin_user, body.old_password, body.new_password)
    if not ok:
        raise HTTPException(status_code=400, detail=err or "Failed to change password")
    return {"status": "ok", "message": "Password successfully updated"}


# ══════════════════════════════════════════════════════════════════
#  REST — Cameras
# ══════════════════════════════════════════════════════════════════

@app.get("/api/cameras")
def api_get_cameras():
    return camera_manager.get_config()


@app.get("/api/drifts")
def api_get_drifts():
    return scene_trigger.latest_drifts


@app.post("/api/cameras", dependencies=[Depends(require_admin)])
async def api_upsert_camera(cam: CameraBody):
    camera_manager.update_camera(cam.model_dump())
    _persist_cameras()
    await ws_manager.broadcast({
        "type": "cameras",
        "data": camera_manager.get_config(),
    })
    return {"status": "ok", "cameras": camera_manager.get_config()}


@app.post("/api/cameras/batch", dependencies=[Depends(require_admin)])
async def api_batch_update_cameras(cams: List[CameraBody]):
    for cam in cams:
        camera_manager.update_camera(cam.model_dump())
    _persist_cameras()
    await ws_manager.broadcast({
        "type": "cameras",
        "data": camera_manager.get_config(),
    })
    return {"status": "ok", "cameras": camera_manager.get_config()}


@app.delete("/api/cameras/{name}", dependencies=[Depends(require_admin)])
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


@app.post("/api/config", dependencies=[Depends(require_admin)])
@app.patch("/api/config", dependencies=[Depends(require_admin)])
@app.post("/api/config/system", dependencies=[Depends(require_admin)])
@app.patch("/api/config/system", dependencies=[Depends(require_admin)])
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


# ── Live Active Stream Tracker ─────────────────────────────────────
_active_display_streams: dict[str, int] = {}
_stream_tracker_lock = threading.Lock()

def _register_stream_start(cam_name: str) -> None:
    with _stream_tracker_lock:
        _active_display_streams[cam_name] = _active_display_streams.get(cam_name, 0) + 1
        total = sum(_active_display_streams.values())
        active_cams = [f"{k}({v})" if v > 1 else k for k, v in sorted(_active_display_streams.items())]
    print(f"\033[1;32m[StreamMgr] 🟢 Stream connected: '{cam_name}' → Actively pulled: {total} feed(s) [{', '.join(active_cams)}]\033[0m")
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_broadcast_active_streams())
    except RuntimeError:
        pass

def _register_stream_stop(cam_name: str) -> None:
    with _stream_tracker_lock:
        if cam_name in _active_display_streams:
            _active_display_streams[cam_name] -= 1
            if _active_display_streams[cam_name] <= 0:
                del _active_display_streams[cam_name]
        total = sum(_active_display_streams.values())
        active_cams = [f"{k}({v})" if v > 1 else k for k, v in sorted(_active_display_streams.items())]
    print(f"\033[1;33m[StreamMgr] 🔴 Stream closed: '{cam_name}' → Actively pulled: {total} feed(s) [{', '.join(active_cams) if active_cams else 'None'}]\033[0m")
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_broadcast_active_streams())
    except RuntimeError:
        pass

async def _broadcast_active_streams() -> None:
    with _stream_tracker_lock:
        total = sum(_active_display_streams.values())
        cams = list(_active_display_streams.keys())
        counts = dict(_active_display_streams)
    await ws_manager.broadcast({
        "type": "active_streams_count",
        "active_streams": total,
        "active_cams": cams,
        "stream_counts": counts,
    })


@app.get("/api/cameras/active-streams")
def api_get_active_streams():
    """Returns exact real-time count and names of video streams actively pulled right now."""
    with _stream_tracker_lock:
        total = sum(_active_display_streams.values())
        cams = list(_active_display_streams.keys())
        counts = dict(_active_display_streams)
    return {
        "active_streams": total,
        "active_cams": cams,
        "stream_counts": counts,
    }


@app.get("/api/cameras/{name}/stream")
async def api_camera_stream(name: str, width: int = 1280, quality: int = 78):
    """Direct zero-latency continuous MJPEG live stream (100% native hardware FPS, zero polling jitter)."""
    async def frame_generator():
        _register_stream_start(name)
        last_seq = -1
        try:
            while not _is_shutting_down:
                frame_data = await asyncio.to_thread(frame_store.get_frame_since, name, last_seq, 1.0, width, quality)
                if frame_data is not None:
                    seq, jpeg_bytes = frame_data
                    last_seq = seq
                    yield (
                        b"--frame\r\n"
                        b"Content-Type: image/jpeg\r\n\r\n" + jpeg_bytes + b"\r\n"
                    )
                else:
                    if _is_shutting_down:
                        break
                    await asyncio.sleep(0.01)
        except (asyncio.CancelledError, GeneratorExit):
            pass
        finally:
            _register_stream_stop(name)

    return StreamingResponse(
        frame_generator(),
        media_type="multipart/x-mixed-replace; boundary=frame",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Connection": "close",
        },
    )


@app.get("/api/cameras/{name}/stream-urls")
def api_camera_stream_urls(name: str, request: Request):
    """Returns direct ultra-low latency WebRTC, WHEP, and LL-HLS stream endpoints."""
    host = request.headers.get("host", "localhost:7000").split(":")[0]
    slug = get_safe_cam_slug(name)
    return {
        "slug": slug,
        "webrtc_player": f"http://{host}:8889/{slug}",
        "hls_player": f"http://{host}:8888/{slug}/",
        "hls_m3u8": f"http://{host}:8888/{slug}/index.m3u8",
        "whep_url": f"http://{host}:8889/{slug}/whep",
        "mjpeg_url": f"/api/cameras/{name}/stream",
    }


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


@app.get("/api/archive/stats")
def api_archive_stats():
    """Archive summary metrics: total stored alert sets, total frames, and timestamp span."""
    return storage.get_archive_stats()


@app.get("/api/incidents/{incident_id}/frames")
def api_get_incident_frames(incident_id: str):
    """Retrieve the sequence of stored frames (base64) for an incident or event ID."""
    frames = storage.get_incident_frames(incident_id)
    if not frames:
        raise HTTPException(status_code=404, detail=f"No stored frames found for incident {incident_id}")
    return {"incident_id": incident_id, "count": len(frames), "frames": frames}


@app.get("/api/incidents/{incident_id}/frame/{frame_idx}")
def api_get_incident_frame_raw(incident_id: str, frame_idx: int = 0):
    """Retrieve a single raw JPEG binary image for an incident frame index (0..3)."""
    raw_jpeg = storage.get_incident_frame_raw(incident_id, frame_idx=frame_idx)
    if not raw_jpeg:
        raise HTTPException(status_code=404, detail=f"Frame #{frame_idx} not found for incident {incident_id}")
    return Response(content=raw_jpeg, media_type="image/jpeg", headers={"Cache-Control": "public, max-age=86400"})


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
        "master_scene_context": prompt_manager.get_master_scene_context(),
        "followup": prompt_manager._followup,
        "cameras": prompt_manager.get_cam_overrides(),
    }


@app.post("/api/prompts", dependencies=[Depends(require_admin)])
async def api_update_prompts(body: PromptBody):
    prompt_manager.save(
        master=body.master,
        master_scene_context=body.master_scene_context,
        followup=body.followup,
        cam_name=body.cam_name,
        cam_prompt=body.cam_prompt,
    )
    await ws_manager.broadcast({
        "type": "prompts",
        "data": {
            "master": prompt_manager.get_master(),
            "master_scene_context": prompt_manager.get_master_scene_context(),
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


@app.delete("/api/alerts", dependencies=[Depends(require_admin)])
async def api_clear_alerts():
    alert_engine.clear()
    return {"status": "ok"}


@app.post("/api/alerts/test", dependencies=[Depends(require_admin)])
async def api_trigger_test_alert(cam: Optional[str] = None, severity: str = "HIGH", followup: bool = True):
    """Trigger an immediate test alert for a specific camera to verify notification feed and inspector."""
    active = camera_manager.get_active_cameras()
    cam_name = cam if (cam and cam in active) else (active[0] if active else "TEST_CAM")
    target_sev = (severity or "HIGH").upper()
    if target_sev not in ("HIGH", "MEDIUM", "LOW"):
        target_sev = "HIGH"

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
            "severity": target_sev,
            "safety": "WARNING" if target_sev == "HIGH" else "CAUTION",
            "activity": "MANUAL_TEST",
            "workers": "2",
            "machinery": "None",
            "observation": f"Manual Test Incident: Detected active movement and safety inspection trigger on {cam_name}. Verified alert delivery pipeline.",
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
    if alert and followup:
        asyncio.create_task(
            scheduler._schedule_followup(
                cam_name=cam_name,
                incident_id=alert["incident_id"],
                parent_id=alert["id"],
                drift_score=0.0482,
                delay_sec=scheduler.followup_interval_sec,
                prev_observation=alert.get("observation", ""),
                prev_severity=alert.get("severity", target_sev),
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


@app.delete("/api/errors", response_model=StandardStatusResponse, dependencies=[Depends(require_admin)])
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


@app.post("/api/scanner/scan", dependencies=[Depends(require_admin)])
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
#  REST — Automated Shift Reporting & PDF Intelligence
# ══════════════════════════════════════════════════════════════

class GenerateReportBody(BaseModel):
    shift_type: str = "AUTO"  # "AUTO", "DAY", "NIGHT", or "CUSTOM"
    send_email: bool = True
    start_ts: Optional[float] = None
    end_ts: Optional[float] = None


@app.get("/api/reports")
def api_get_reports(limit: int = 50):
    """List generated shift reports."""
    return reporting_service.get_reports_list(limit=limit)


@app.get("/api/reports/{report_id}/pdf")
def api_get_report_pdf(report_id: int):
    """Download/view shift report PDF."""
    from fastapi.responses import FileResponse
    conn = reporting_service._conn()
    row = conn.execute("SELECT id, pdf_path FROM reports WHERE id = ?", (report_id,)).fetchone()
    if not row or not row["pdf_path"]:
        raise HTTPException(status_code=404, detail="Report record not found")
    
    pdf_p = Path(row["pdf_path"])
    if not pdf_p.exists():
        raise HTTPException(status_code=404, detail="Report PDF file not found on disk")
    
    return FileResponse(
        str(pdf_p),
        media_type="application/pdf",
        filename=pdf_p.name,
        headers={"Content-Disposition": f'inline; filename="{pdf_p.name}"'}
    )


@app.post("/api/reports/generate")
async def api_generate_report(body: GenerateReportBody = Body(...)):
    """Trigger on-demand shift report generation and email dispatch."""
    loop = asyncio.get_running_loop()
    try:
        result = await loop.run_in_executor(
            None,
            reporting_service.generate_shift_report,
            body.shift_type,
            body.start_ts,
            body.end_ts,
            body.send_email,
        )
        return result
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Report generation failed: {exc}")


@app.get("/api/reports/stats")
def api_get_reporting_stats():
    """Returns reporting schedule status, buffer capacity, recipients, and next scheduled runs."""
    now = datetime.now()
    today_6am = now.replace(hour=6, minute=0, second=0, microsecond=0)
    today_6pm = now.replace(hour=18, minute=0, second=0, microsecond=0)
    tomorrow_6am = (now + timedelta(days=1)).replace(hour=6, minute=0, second=0, microsecond=0)
    
    if now < today_6am:
        next_run = today_6am
        next_shift = "NIGHT"
    elif now < today_6pm:
        next_run = today_6pm
        next_shift = "DAY"
    else:
        next_run = tomorrow_6am
        next_shift = "NIGHT"

    sys_cfg = config_manager.get()
    buf_stats = reporting_service.get_buffer_stats()

    return {
        "enabled": getattr(sys_cfg, "reporting_enabled", True),
        "schedule_hours": [6, 18],
        "next_scheduled_run": next_run.strftime("%b %d, %Y %H:%M:%S IST"),
        "next_shift": next_shift,
        "recipients": getattr(sys_cfg, "report_email_recipients", ["pandalavacarji@gmail.com", "reportsclove@gmail.com"]),
        "sender_email": getattr(sys_cfg, "report_sender_email", "reportsclove@gmail.com"),
        "stored_count": buf_stats.get("stored_count", 0),
        "max_buffer": buf_stats.get("max_buffer", 69),
        "buffer_coverage_days": buf_stats.get("buffer_coverage_days", 34.5),
        "disk_usage_mb": buf_stats.get("disk_usage_mb", 0.0),
        "average_pdf_kb": buf_stats.get("average_pdf_kb", 0.0),
    }


# ══════════════════════════════════════════════════════════════
#  Watchdog Email & Diagnostics Endpoints
# ══════════════════════════════════════════════════════════════

@app.get("/api/watchdog/status")
def api_get_watchdog_status():
    """Returns real-time health watchdog telemetry and 24h health snapshot."""
    summary = watchdog.get_summary()
    daily_snapshot = watchdog_emailer.compile_daily_health_summary()
    sys_cfg = config_manager.get()
    return {
        "watchdog": summary,
        "daily_snapshot": daily_snapshot,
        "watchdog_alerts_enabled": getattr(sys_cfg, "watchdog_alerts_enabled", True),
        "daily_digest_enabled": getattr(sys_cfg, "daily_digest_enabled", True),
        "daily_digest_hour": getattr(sys_cfg, "daily_digest_hour", 8),
        "recipients": watchdog_emailer._get_recipients(),
    }


@app.post("/api/watchdog/test-alert", dependencies=[Depends(require_admin)])
def api_test_watchdog_alert():
    """Triggers an immediate test critical incident email to verified recipients."""
    success = watchdog_emailer.send_critical_alert(
        event_type="TEST_INCIDENT",
        title="Manual Watchdog Alert Diagnostic Test",
        message="This is a test critical watchdog notification initiated by the administrator to verify end-to-end alerting dispatch.",
        details={
            "Initiator": "Admin Dashboard",
            "Hardware Node": "NVIDIA Thor",
            "Pipeline Status": "Operational",
        },
        severity="CRITICAL",
        remediation="No action needed. Alerting pipeline is verified functional.",
        cooldown_sec=0.0,
    )
    return {"status": "ok", "dispatched": success, "message": "Test watchdog alert queued for transmission."}


@app.post("/api/watchdog/send-daily-digest", dependencies=[Depends(require_admin)])
def api_send_daily_digest():
    """Forces immediate compilation and email transmission of the 24-Hour System Health & Diagnostics Digest."""
    success = watchdog_emailer.send_daily_health_digest()
    return {"status": "ok", "dispatched": success, "message": "24-Hour System Health Digest compilation and email queued."}



class NoCacheStaticFiles(StaticFiles):
    async def get_response(self, path: str, scope: Scope) -> Response:
        response = await super().get_response(path, scope)
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response

app.mount("/clips", StaticFiles(directory=str(CLIPS_DIR)), name="clips")
app.mount("/", NoCacheStaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")

