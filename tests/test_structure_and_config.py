"""
Verification test for backend folder arrangement and config-driven templates.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

def test_backend_folder_structure():
    print("\n--- Testing Backend Folder Structure ---")
    backend_dir = ROOT / "backend"
    py_files = [f.name for f in backend_dir.glob("*.py")]
    print(f"Python files in backend root: {py_files}")
    assert set(py_files) == {"__init__.py", "main.py"}, f"Unexpected loose python files in backend/: {py_files}"
    
    # Check subpackages
    assert (backend_dir / "core").is_dir(), "backend/core missing"
    assert (backend_dir / "schemas").is_dir(), "backend/schemas missing"
    assert (backend_dir / "services").is_dir(), "backend/services missing"

    # Verify all 13 services in services/
    expected_services = {
        "__init__.py",
        "alert_engine.py",
        "camera_manager.py",
        "frame_store.py",
        "metrics_monitor.py",
        "nvidia_ingest.py",
        "prompt_manager.py",
        "result_store.py",
        "rtsp_scanner.py",
        "scene_trigger.py",
        "scheduler.py",
        "storage.py",
        "vlm_client.py",
        "ws_manager.py",
    }
    actual_services = {f.name for f in (backend_dir / "services").glob("*.py")}
    assert expected_services.issubset(actual_services), f"Missing services: {expected_services - actual_services}"
    print("✅ backend/ folder structure cleanly arranged: only main.py, __init__.py and core/, schemas/, services/ packages.")

def test_scanner_config():
    print("\n--- Testing RTSP Scanner Config Isolation ---")
    from backend.services.rtsp_scanner import RTSPScanner
    from backend.core.config import load_scanner_config

    cfg = load_scanner_config()
    assert "rtsp_path_templates" in cfg
    assert len(cfg["rtsp_path_templates"]) >= 13
    assert "ws_discovery_probe" in cfg
    assert "{creds}" in cfg["rtsp_path_templates"][0]
    
    params = RTSPScanner._get_scanner_params()
    assert params["rtsp_path_templates"] == cfg["rtsp_path_templates"]
    print(f"Loaded {len(params['rtsp_path_templates'])} RTSP templates dynamically from config/scanner.json")
    print("✅ RTSP Scanner config test passed!")

def test_prompt_manager_config():
    print("\n--- Testing Prompt Manager Config Isolation ---")
    from backend.services.prompt_manager import PromptManager
    from backend.core.config import load_prompts_config

    prompts_cfg = load_prompts_config()
    pm = PromptManager()
    prompt = pm.get_prompt("TestCam")
    assert prompt is not None and len(prompt) > 50
    assert "surveillance" in prompt.lower() and "OBSERVATION:" in prompt
    print("✅ Prompt Manager dynamic config test passed!")

if __name__ == "__main__":
    test_backend_folder_structure()
    test_scanner_config()
    test_prompt_manager_config()
    print("\n🎉 ALL STRUCTURE & CONFIG CHECKS PASSED!")
