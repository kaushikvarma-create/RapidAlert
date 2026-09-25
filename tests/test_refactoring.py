"""
Comprehensive test script for RapidAlert refactoring:
- Tests Centralized Configuration
- Tests Request/Response Schemas
- Tests ErrorTracker exception capturing & effects tracking
- Tests FastAPI application endpoints via TestClient
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "backend"))

def test_config():
    print("\n--- 1. Testing Centralized Configuration ---")
    from backend.core.config import (
        config_manager,
        DEFAULT_DASHBOARD_PORT,
        DEFAULT_VLM_MODEL,
        DEFAULT_SCENE_THRESHOLD,
        DEFAULT_FRAME_WIDTH,
    )
    cfg = config_manager.get()
    print(f"Loaded config: dashboard_port={cfg.dashboard_port}, vllm_model={cfg.vllm_model}")
    assert cfg.dashboard_port == 7000
    assert cfg.scene_threshold > 0
    assert cfg.frame_width == DEFAULT_FRAME_WIDTH
    print("✅ Configuration module verified successfully!")

def test_schemas():
    print("\n--- 2. Testing Request & Response Schemas ---")
    from backend.schemas.requests import CameraBody, SystemConfigBody, PromptBody
    from backend.schemas.responses import ErrorRecordResponse, ErrorListResponse

    # Test CameraBody
    cam = CameraBody(name="Gate_Cam", url="rtsp://192.168.1.100:554/live", threshold=0.04)
    assert cam.name == "Gate_Cam"
    assert cam.threshold == 0.04

    # Test SystemConfigBody
    sys_up = SystemConfigBody(default_threshold=0.038, followup_interval_sec=12.0)
    assert sys_up.default_threshold == 0.038

    # Test ErrorRecordResponse
    err_rec = ErrorRecordResponse(
        id="ERR-TEST-001",
        timestamp="2026-09-21 15:30:00",
        ts=1726911000.0,
        component="Scheduler",
        function="_analyze_job",
        file="backend/scheduler.py:204",
        line=204,
        camera="Front_Gate",
        error_type="TimeoutError",
        message="VLM query timeout after 60s",
        effect="VLM analysis dropped for this cycle; camera queued for retry",
        stack_trace="Traceback...",
        severity="ERROR",
    )
    assert err_rec.id == "ERR-TEST-001"
    assert "dropped for this cycle" in err_rec.effect
    print("✅ Request & Response Schemas verified successfully!")

def test_error_tracker():
    print("\n--- 3. Testing Error Tracker & Effects Tracking ---")
    from backend.core.error_tracker import error_tracker

    error_tracker.clear()
    assert len(error_tracker.get_recent()) == 0

    # Test capturing a controlled exception with location & operational effect
    try:
        raise ConnectionResetError("Remote RTSP peer closed socket prematurely")
    except Exception as exc:
        rec = error_tracker.capture_exception(
            exc,
            component="CameraManager",
            camera="Yard_West",
            effect="RTSP video stream broken; initiated 15s reconnect backoff timer",
            severity="WARNING",
        )

    assert rec.error_type == "ConnectionResetError"
    assert rec.camera == "Yard_West"
    assert rec.component == "CameraManager"
    assert "initiated 15s reconnect backoff" in rec.effect
    assert "test_refactoring.py" in rec.file

    # Verify query
    recent = error_tracker.get_recent(limit=10)
    assert len(recent) == 1
    assert recent[0]["id"] == rec.id

    # Verify summary
    summary = error_tracker.get_summary()
    assert summary["total_errors"] == 1
    assert summary["by_component"]["CameraManager"] == 1
    assert summary["by_severity"]["WARNING"] == 1
    print(f"Recorded error: {rec.id} | Component: {rec.component} | Effect: {rec.effect}")
    print("✅ Error Tracker verified successfully!")

def test_fastapi_endpoints():
    print("\n--- 4. Testing FastAPI App & REST Endpoints ---")
    from fastapi.testclient import TestClient
    from backend.main import app

    client = TestClient(app)

    # Test GET /api/config
    res = client.get("/api/config")
    assert res.status_code == 200
    data = res.json()
    assert "system" in data
    assert "cameras" in data
    print("✅ GET /api/config OK")

    # Test GET /api/errors
    res = client.get("/api/errors")
    assert res.status_code == 200
    data = res.json()
    assert "errors" in data
    assert data["count"] >= 1
    print(f"✅ GET /api/errors OK (returned {data['count']} errors)")

    # Test GET /api/errors/summary
    res = client.get("/api/errors/summary")
    assert res.status_code == 200
    data = res.json()
    assert "total_errors" in data
    print(f"✅ GET /api/errors/summary OK (total: {data['total_errors']})")

    # Test DELETE /api/errors with admin auth
    login_res = client.post("/api/auth/login", json={"username": "admin", "password": "admin123"})
    token = login_res.json().get("token")
    headers = {"Authorization": f"Bearer {token}"} if token else {}

    res = client.delete("/api/errors", headers=headers)
    assert res.status_code == 200
    assert res.json()["status"] == "ok"
    
    # Confirm empty
    res = client.get("/api/errors")
    assert res.json()["count"] == 0
    print("✅ DELETE /api/errors OK")

if __name__ == "__main__":
    test_config()
    test_schemas()
    test_error_tracker()
    test_fastapi_endpoints()
    print("\n🎉 ALL TESTS PASSED SUCCESSFULLY!")
