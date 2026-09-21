"""
RapidAlert — FastAPI backend
Serves REST API + WebSocket + static dashboard frontend.
"""
import asyncio
import base64
import json
import os
import socket
import subprocess
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ── Path setup ─────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
CONFIG_DIR = ROOT / "config"
FRONTEND_DIR = ROOT / "frontend"

# All backend modules live alongside main.py
sys.path.insert(0, str(Path(__file__).parent))

from camera_manager import CameraManager
from frame_store import FrameStore
from vlm_client import VLMPool
from prompt_manager import PromptManager
from result_store import ResultStore
from alert_engine import AlertEngine
from scheduler import DeadlineScheduler
from ws_manager import WSManager
from storage import StorageManager
from rtsp_scanner import RTSPScanner
from metrics_monitor import metrics_loop
from scene_trigger import SceneTriggerEngine


# ══════════════════════════════════════════════════════════════════
#  Config loading
# ══════════════════════════════════════════════════════════════════

def _load_system_cfg() -> dict:
    defaults = {
        "vllm_endpoints": [
            {"url": "http://localhost:8000", "model": "Qwen/Qwen3-VL-8B-Instruct"}
        ],
        "auto_start_vllm": True,
        "vllm_model": "Qwen/Qwen3-VL-8B-Instruct",
        "vllm_port": 8000,
        "vllm_max_seqs": 16,
        "vllm_max_model_len": 4096,
        "vllm_gpu_utilization": 0.85,
        "initial_concurrency": 4,
        "max_concurrency": 16,
        "frame_width": 1280,
        "jpeg_quality": 82,
    }
    path = CONFIG_DIR / "system.json"
    if path.exists():
        try:
            with open(path) as f:
                defaults.update(json.load(f))
        except Exception as e:
            print(f"[Config] system.json error: {e}")
    return defaults


# ══════════════════════════════════════════════════════════════════
#  Singletons
# ══════════════════════════════════════════════════════════════════

SYS_CFG = _load_system_cfg()

frame_store      = FrameStore(SYS_CFG["frame_width"], SYS_CFG["jpeg_quality"])
camera_manager   = CameraManager(CONFIG_DIR / "cameras.json", frame_store)
vlm_pool         = VLMPool(SYS_CFG["vllm_endpoints"])
result_store     = ResultStore()
alert_engine     = AlertEngine()
ws_manager       = WSManager()
storage          = StorageManager(ROOT / "data" / "analyses.db")
prompt_manager   = PromptManager(
    CONFIG_DIR / "prompts.json",
    cameras_config_provider=camera_manager.get_config,
)
scheduler = DeadlineScheduler(
    camera_manager    = camera_manager,
    frame_store       = frame_store,
    vlm_pool          = vlm_pool,
    prompt_manager    = prompt_manager,
    result_store      = result_store,
    alert_engine      = alert_engine,
    initial_concurrency = SYS_CFG["initial_concurrency"],
    max_concurrency     = SYS_CFG["max_concurrency"],
    broadcast_fn      = ws_manager.broadcast,
    storage           = storage,
    default_heartbeat_sec = float(SYS_CFG.get("default_heartbeat_sec", 30.0)),
)
alert_engine.set_broadcaster(ws_manager.broadcast)
scene_trigger = SceneTriggerEngine(
    frame_store=frame_store,
    camera_manager=camera_manager,
    on_incident_callback=scheduler.queue_incident,
    broadcast_fn=ws_manager.broadcast,
    alert_engine=alert_engine,
    model_name=SYS_CFG.get("dinov2_model", "facebook/dinov2-small"),
    device="cuda",
    default_threshold=SYS_CFG.get("scene_threshold", 0.033),
    semantic_interval=SYS_CFG.get("semantic_interval", 0.5),
    event_cooldown=SYS_CFG.get("event_cooldown", 15.0),
)


# ══════════════════════════════════════════════════════════════════
#  Lifespan
# ══════════════════════════════════════════════════════════════════

_bg_tasks = []

@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ─────────────────────────────────────────────────
    camera_manager.sync()
    await vlm_pool.start()
    await scheduler.start()
    scene_trigger.start()
    _bg_tasks.append(asyncio.create_task(prompt_manager.watch_loop()))
    _bg_tasks.append(asyncio.create_task(_config_sync_loop()))
    _bg_tasks.append(asyncio.create_task(metrics_loop(ws_manager.broadcast)))
    _bg_tasks.append(asyncio.create_task(_snapshot_stream_loop()))
    print(f"\n[RapidAlert] ✅ Dashboard → http://localhost:{SYS_CFG.get('dashboard_port', 7000)}\n")
    yield
    # ── Shutdown ────────────────────────────────────────────────
    for task in _bg_tasks:
        task.cancel()
    scene_trigger.stop()
    await scheduler.stop()
    await vlm_pool.stop()
    camera_manager.stop_all()


async def _config_sync_loop() -> None:
    """Poll cameras.json and system.json for changes, hot-apply to engines, and notify WS clients."""
    global SYS_CFG
    sys_path = CONFIG_DIR / "system.json"
    last_sys_mtime = 0.0
    try:
        if sys_path.exists():
            last_sys_mtime = os.path.getmtime(sys_path)
    except Exception:
        pass

    while True:
        await asyncio.sleep(2)
        # 1. Camera changes
        if camera_manager.sync():
            await ws_manager.broadcast({
                "type": "cameras",
                "data": camera_manager.get_config(),
            })

        # 2. System config changes
        try:
            if sys_path.exists():
                mtime = os.path.getmtime(sys_path)
                if mtime > last_sys_mtime:
                    last_sys_mtime = mtime
                    with open(sys_path) as f:
                        new_cfg = json.load(f)
                    SYS_CFG.update(new_cfg)
                    if "default_threshold" in new_cfg or "scene_threshold" in new_cfg:
                        t = float(new_cfg.get("default_threshold", new_cfg.get("scene_threshold", 0.033)))
                        scene_trigger.set_default_threshold(t)
                    if "default_heartbeat_sec" in new_cfg:
                        scheduler.default_heartbeat_sec = float(new_cfg["default_heartbeat_sec"])
                    if "event_cooldown" in new_cfg:
                        scene_trigger.event_cooldown = float(new_cfg["event_cooldown"])
                    if "semantic_interval" in new_cfg:
                        scene_trigger.semantic_interval = float(new_cfg["semantic_interval"])
                    if "followup_interval_sec" in new_cfg:
                        scheduler.followup_interval_sec = float(new_cfg["followup_interval_sec"])
                    if "persistent_followup" in new_cfg:
                        scheduler.persistent_followup = bool(new_cfg["persistent_followup"])
                    print(f"[Main] 🔄 Hot-reloaded system.json (default_thresh: {SYS_CFG.get('default_threshold')}, hb: {SYS_CFG.get('default_heartbeat_sec')}s, followup: {scheduler.followup_interval_sec}s, persistent: {scheduler.persistent_followup})")
                    await ws_manager.broadcast({
                        "type": "config_updated",
                        "system": SYS_CFG,
                        "cameras": camera_manager.get_config(),
                    })
        except Exception as e:
            print(f"[Main] Error in system config sync: {e}")


async def _snapshot_stream_loop() -> None:
    """Broadcast live camera snapshots every 2 seconds so the dashboard preview stays live."""
    while True:
        await asyncio.sleep(2.0)
        active = camera_manager.get_active_cameras()
        for cam in active:
            snap = frame_store.get_snapshot_b64(cam, max_w=320, quality=65)
            if snap:
                await ws_manager.broadcast({
                    "type": "camera_frame",
                    "cam": cam,
                    "thumbnail_b64": snap,
                })


# ══════════════════════════════════════════════════════════════════
#  App
# ══════════════════════════════════════════════════════════════════

app = FastAPI(title="RapidAlert", version="1.0.0", lifespan=lifespan)


# ══════════════════════════════════════════════════════════════════
#  WebSocket
# ══════════════════════════════════════════════════════════════════

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws_manager.connect(ws)
    try:
        # Send live snapshots for all active cameras so preview is never blank
        active_cams = camera_manager.get_active_cameras()
        live_thumbs = {}
        for c in active_cams:
            snap = frame_store.get_snapshot_b64(c, max_w=320, quality=65)
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
            "alerts": alert_engine.get_recent(25),
            "system": SYS_CFG,
            "metrics": {
                **result_store.get_metrics(),
                "concurrency": scheduler.concurrency,
                "queue_depth": scheduler.queue_depth,
                "in_flight": scheduler.in_flight_count,
            },
            "prompts": {
                "master": prompt_manager.get_master(),
                "cameras": prompt_manager.get_cam_overrides(),
            },
        })
        # Keep connection alive with periodic pings
        while True:
            await asyncio.sleep(25)
            await ws.send_json({"type": "ping"})
    except WebSocketDisconnect:
        pass
    except asyncio.CancelledError:
        pass
    except Exception:
        pass
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


class CameraBody(BaseModel):
    name: str
    url: str = ""
    enabled: bool = True
    normal_context_day: str = ""
    normal_context_night: str = ""
    priority: str = "normal"
    threshold: Optional[float] = None
    heartbeat_sec: Optional[float] = None


class SystemConfigBody(BaseModel):
    default_threshold: Optional[float] = None
    default_heartbeat_sec: Optional[float] = None
    event_cooldown: Optional[float] = None
    semantic_interval: Optional[float] = None
    followup_interval_sec: Optional[float] = None
    persistent_followup: Optional[bool] = None


@app.get("/api/config")
def api_get_config():
    return {
        "system": SYS_CFG,
        "cameras": camera_manager.get_config(),
    }


@app.post("/api/config")
async def api_update_system_config(body: SystemConfigBody):
    global SYS_CFG
    updates = body.model_dump(exclude_none=True)
    if not updates:
        return {"status": "ok", "system": SYS_CFG}

    SYS_CFG.update(updates)
    if "default_threshold" in updates:
        SYS_CFG["scene_threshold"] = updates["default_threshold"]
        scene_trigger.set_default_threshold(updates["default_threshold"])
    if "default_heartbeat_sec" in updates:
        scheduler.default_heartbeat_sec = float(updates["default_heartbeat_sec"])
    if "event_cooldown" in updates:
        scene_trigger.event_cooldown = float(updates["event_cooldown"])
    if "semantic_interval" in updates:
        scene_trigger.semantic_interval = float(updates["semantic_interval"])
    if "followup_interval_sec" in updates:
        scheduler.followup_interval_sec = float(updates["followup_interval_sec"])
    if "persistent_followup" in updates:
        scheduler.persistent_followup = bool(updates["persistent_followup"])

    # Persist to system.json
    try:
        sys_path = CONFIG_DIR / "system.json"
        with open(sys_path, "w") as f:
            json.dump(SYS_CFG, f, indent=2)
    except Exception as e:
        print(f"[Main] Error saving system.json: {e}")

    await ws_manager.broadcast({
        "type": "config_updated",
        "system": SYS_CFG,
        "cameras": camera_manager.get_config(),
    })
    return {"status": "ok", "system": SYS_CFG}


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
async def api_batch_update_cameras(cams: list[CameraBody]):
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
    path = CONFIG_DIR / "cameras.json"
    with open(path, "w") as f:
        json.dump(camera_manager.get_config(), f, indent=2)


# ══════════════════════════════════════════════════════════════════
#  REST — Frames
# ══════════════════════════════════════════════════════════════════

@app.get("/api/cameras/{name}/frame")
def api_get_frame(name: str, width: int = 640, quality: int = 80):
    b64 = frame_store.get_snapshot_b64(name, max_w=width, quality=quality)
    if b64 is None:
        raise HTTPException(status_code=404, detail="No frame available")
    return Response(
        content=base64.b64decode(b64),
        media_type="image/jpeg",
        headers={"Cache-Control": "no-cache"},
    )


@app.get("/api/cameras/{name}/stream")
async def api_camera_stream(name: str, width: int = 640, quality: int = 70):
    """Continuous MJPEG live video stream (multipart/x-mixed-replace)."""
    async def frame_generator():
        try:
            while True:
                b64 = frame_store.get_snapshot_b64(name, max_w=width, quality=quality)
                if b64:
                    frame_bytes = base64.b64decode(b64)
                    yield (
                        b"--frame\r\n"
                        b"Content-Type: image/jpeg\r\n\r\n" + frame_bytes + b"\r\n"
                    )
                await asyncio.sleep(0.1) # ~10 FPS smooth video
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
    since: Optional[float] = None,
    until: Optional[float] = None,
    limit: int = 100,
    offset: int = 0,
):
    """Filtered history query across all cameras."""
    return storage.query(
        cam=cam,
        since_ts=since,
        until_ts=until,
        severity=severity,
        safety=safety,
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
        "cameras": prompt_manager.get_cam_overrides(),
    }


class PromptBody(BaseModel):
    master: Optional[str] = None
    cam_name: Optional[str] = None
    cam_prompt: Optional[str] = None  # None or "" → clear override


@app.post("/api/prompts")
async def api_update_prompts(body: PromptBody):
    prompt_manager.save(
        master=body.master,
        cam_name=body.cam_name,
        cam_prompt=body.cam_prompt,
    )
    await ws_manager.broadcast({
        "type": "prompts",
        "data": {
            "master": prompt_manager.get_master(),
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


@app.delete("/api/alerts")
async def api_clear_alerts():
    alert_engine.clear()
    return {"status": "ok"}


@app.post("/api/alerts/test")
async def api_trigger_test_alert(cam: Optional[str] = None):
    """Trigger an immediate test alert to verify notification feed and inspector."""
    active = camera_manager.get_active_cameras()
    cam_name = cam if (cam and cam in active) else (active[0] if active else "TEST_CAM")
    snap = frame_store.get_snapshot_b64(cam_name, max_w=960, quality=78)
    temporal_snaps = frame_store.get_temporal_snapshots_b64(cam_name, count=4, span_sec=10.0, max_w=480, quality=68)
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
#  REST — Metrics
# ══════════════════════════════════════════════════════════════════

@app.get("/api/metrics")
def api_get_metrics():
    m = result_store.get_metrics()
    m.update({
        "concurrency": scheduler.concurrency,
        "queue_depth": scheduler.queue_depth,
        "in_flight": scheduler.in_flight_count,
    })
    return m


# ══════════════════════════════════════════════════════════════════
#  REST — vLLM health
# ══════════════════════════════════════════════════════════════════

@app.get("/api/health/vlm")
async def api_vlm_health():
    return await vlm_pool.health_check()


# ══════════════════════════════════════════════════════════════
#  REST — RTSP / ONVIF scanner
# ══════════════════════════════════════════════════════════════

class ScanBody(BaseModel):
    subnet: Optional[str] = None          # e.g. "192.168.1.0/24" or None for auto
    ws_timeout: float = 3.0
    port_timeout: float = 0.4
    username: Optional[str] = None
    password: Optional[str] = None

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
#  Static frontend — mount LAST (catches all remaining routes)
# ══════════════════════════════════════════════════════════════

app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
