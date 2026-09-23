"""
RapidAlert Services Package
Core business and hardware acceleration services:
- Camera ingestion & hardware decode (NVDEC/DeepStream)
- Frame store & buffer management
- DINOv2 real-time CUDA visual scene drift detection
- Cosmos Reason2 8B VLM pool & inference client
- Deadline-driven prioritized scheduler & follow-up engine
- SQLite persistent analysis storage
- Alert generation & WebSocket dispatch
- RTSP / ONVIF network camera scanner
- System hardware metrics monitor
"""
from backend.services.alert_engine import AlertEngine
from backend.services.camera_manager import CameraManager, CameraThread
from backend.services.clip_recorder import ClipRecorder, clip_recorder
from backend.services.frame_store import FrameStore
from backend.services.metrics_monitor import metrics_loop, get_gpu_util, get_cpu_util, get_ram_util
from backend.services.nvidia_ingest import NvidiaStreamCapture, is_nvidia_available
from backend.services.prompt_manager import PromptManager
from backend.services.result_store import ResultStore
from backend.services.rtsp_scanner import RTSPScanner
from backend.services.scene_trigger import SceneTriggerEngine
from backend.services.scheduler import DeadlineScheduler
from backend.services.storage import StorageManager
from backend.services.vlm_client import VLMPool
from backend.services.ws_manager import WSManager

__all__ = [
    "AlertEngine",
    "CameraManager",
    "CameraThread",
    "ClipRecorder",
    "clip_recorder",
    "FrameStore",
    "metrics_loop",
    "get_gpu_util",
    "get_cpu_util",
    "get_ram_util",
    "NvidiaStreamCapture",
    "is_nvidia_available",
    "PromptManager",
    "ResultStore",
    "RTSPScanner",
    "SceneTriggerEngine",
    "DeadlineScheduler",
    "StorageManager",
    "VLMPool",
    "WSManager",
]
