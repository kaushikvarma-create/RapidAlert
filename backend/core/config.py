"""
RapidAlert Centralized Configuration Module
Single source of truth for all system paths, constants, model names, ports,
thresholds, encoding settings, and runtime configuration.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

# ══════════════════════════════════════════════════════════════════════
#  Filesystem Paths
# ══════════════════════════════════════════════════════════════════════

# ROOT is the RapidAlert project base directory
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
BACKEND_DIR = ROOT_DIR / "backend"
CONFIG_DIR = ROOT_DIR / "config"
DATA_DIR = ROOT_DIR / "data"
FRONTEND_DIR = ROOT_DIR / "frontend"
LOGS_DIR = ROOT_DIR / "logs"

SYSTEM_CONFIG_PATH = CONFIG_DIR / "system.json"
CAMERAS_CONFIG_PATH = CONFIG_DIR / "cameras.json"
PROMPTS_CONFIG_PATH = CONFIG_DIR / "prompts.json"
SCANNER_CONFIG_PATH = CONFIG_DIR / "scanner.json"
DATABASE_PATH = DATA_DIR / "analyses.db"
CLIPS_DIR = DATA_DIR / "clips"

# Ensure runtime directories exist
DATA_DIR.mkdir(parents=True, exist_ok=True)
CLIPS_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)


# ══════════════════════════════════════════════════════════════════════
#  Static Constants & Defaults
# ══════════════════════════════════════════════════════════════════════

# Network & Server
DEFAULT_DASHBOARD_PORT = 7000
DEFAULT_VLLM_PORT = 8000
DEFAULT_VLLM_HOST = "http://localhost:8000"

# AI Models
DEFAULT_VLM_MODEL = "vrfai/Cosmos-Reason2-8B-NVFP4"
DEFAULT_DINOV2_MODEL = "facebook/dinov2-small"

# Video Ingest & Image Resolutions
DEFAULT_FRAME_WIDTH = 1280
DEFAULT_FRAME_HEIGHT = 720
DEFAULT_JPEG_QUALITY = 85

PREVIEW_FRAME_WIDTH = 320
PREVIEW_JPEG_QUALITY = 65

HIGH_RES_FRAME_WIDTH = 960
HIGH_RES_JPEG_QUALITY = 78

TEMPORAL_THUMB_WIDTH = 480
TEMPORAL_THUMB_QUALITY = 68

# DINOv2 Drift Detection & Trigger
DEFAULT_SCENE_THRESHOLD = 0.034
DINO_MAJOR_THRESHOLD = 0.060
DINO_MINOR_THRESHOLD = 0.030
DEFAULT_SEMANTIC_INTERVAL = 0.5    # seconds between DINOv2 embedding checks
DEFAULT_EVENT_COOLDOWN = 15.0      # seconds cooldown per camera between trigger events
DINOV2_INPUT_WIDTH = 448           # resize width for fast inference

# Surveillance Scheduler & Follow-up
DEFAULT_HEARTBEAT_SEC = 35.0       # fallback periodic check interval if camera has none
DEFAULT_FOLLOWUP_INTERVAL = 10.0   # seconds delay before incident follow-up check
DEFAULT_FOLLOWUP_MAX_CYCLES = 6    # max persistent follow-up attempts
DEFAULT_PERSISTENT_FOLLOWUP = True # persist follow-up until severity stabilizes

# Scheduler Concurrency & Workers
DEFAULT_INITIAL_CONCURRENCY = 4
DEFAULT_MAX_CONCURRENCY = 8
QUEUE_CAP_PER_WORKER = 1
FEED_TICK_SEC = 0.005              # 5ms feed loop frequency
IDLE_TICK_SEC = 0.05               # 50ms idle sleep when queue full or no cams
TUNE_INTERVAL_SEC = 15.0           # auto-tune worker check interval
LATENCY_UP_THRESH_SEC = 4.0        # p95 below this -> can scale worker up
LATENCY_DN_THRESH_SEC = 12.0       # p95 above this -> scale worker down

# Camera Management & Hardware Ingestion
CAMERA_RETRY_DELAY_SEC = 5
CAMERA_READ_TIMEOUT_SEC = 1.5
CAMERA_BUFFER_SIZE = 1
DEEPSTREAM_BATCH_TIMEOUT_US = 33000
DEEPSTREAM_SYNC_INPUTS = False
DEEPSTREAM_COMPUTE_HW = 1

# Alert Trigger Classifications
DEFAULT_ALERT_SEVERITIES = ["HIGH", "MEDIUM"]
DEFAULT_ALERT_SAFETIES = ["DANGER", "WARNING"]

# RTSP / ONVIF Scanner Fallbacks
DEFAULT_RTSP_PORT = 554
DEFAULT_SCAN_CONCURRENCY = 128
DEFAULT_PORT_TIMEOUT_SEC = 0.4
DEFAULT_WS_DISCOVERY_TIMEOUT_SEC = 3.0


# ══════════════════════════════════════════════════════════════════════
#  Scanner Configuration Loader
# ══════════════════════════════════════════════════════════════════════

def load_scanner_config() -> Dict[str, Any]:
    """Load RTSP templates and WS-Discovery probe from config/scanner.json."""
    if SCANNER_CONFIG_PATH.exists():
        try:
            with open(SCANNER_CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"[Config] ⚠️ Error loading {SCANNER_CONFIG_PATH}: {e}")
    return {
        "default_port": DEFAULT_RTSP_PORT,
        "scan_concurrency": DEFAULT_SCAN_CONCURRENCY,
        "default_port_timeout": DEFAULT_PORT_TIMEOUT_SEC,
        "default_ws_timeout": DEFAULT_WS_DISCOVERY_TIMEOUT_SEC,
        "rtsp_path_templates": [
            "rtsp://{creds}{ip}:{port}/Streaming/Channels/101",
            "rtsp://{creds}{ip}:{port}/Streaming/Channels/102",
            "rtsp://{creds}{ip}:{port}/cam/realmonitor?channel=1&subtype=0",
            "rtsp://{creds}{ip}:{port}/cam/realmonitor?channel=1&subtype=1",
            "rtsp://{creds}{ip}:{port}/h264Preview_01_main",
            "rtsp://{creds}{ip}:{port}/h264Preview_01_sub",
            "rtsp://{creds}{ip}:{port}/axis-media/media.amp",
            "rtsp://{creds}{ip}:{port}/live/ch0",
            "rtsp://{creds}{ip}:{port}/stream1",
            "rtsp://{creds}{ip}:{port}/stream2",
            "rtsp://{creds}{ip}:{port}/h264",
            "rtsp://{creds}{ip}:{port}/ch0",
            "rtsp://{creds}{ip}:{port}/",
        ],
        "ws_discovery_probe": (
            '<?xml version="1.0" encoding="utf-8"?>\n'
            '<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" '
            'xmlns:wsa="http://schemas.xmlsoap.org/ws/2004/08/addressing" '
            'xmlns:wsd="http://schemas.xmlsoap.org/ws/2005/04/discovery" '
            'xmlns:dn="http://www.onvif.org/ver10/network/wsdl">\n'
            '<soap:Header>\n'
            '<wsa:MessageID>uuid: rapidalert-probe-{}</wsa:MessageID>\n'
            '<wsa:To>urn:schemas-xmlsoap-org:ws:2005:04:discovery</wsa:To>\n'
            '<wsa:Action>http://schemas.xmlsoap.org/ws/2005/04/discovery/Probe</wsa:Action>\n'
            '</soap:Header>\n'
            '<soap:Body>\n'
            '<wsd:Probe>\n'
            '<wsd:Types>dn:NetworkVideoTransmitter</wsd:Types>\n'
            '</wsd:Probe>\n'
            '</soap:Body>\n'
            '</soap:Envelope>'
        ),
    }


def load_prompts_config() -> Dict[str, Any]:
    """Load default prompt templates from config/prompts.json."""
    if PROMPTS_CONFIG_PATH.exists():
        try:
            with open(PROMPTS_CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"[Config] ⚠️ Error loading {PROMPTS_CONFIG_PATH}: {e}")
    return {
        "master": "",
        "followup": "",
        "cameras": {},
    }


# ══════════════════════════════════════════════════════════════════════
#  Dynamic System Configuration Model
# ══════════════════════════════════════════════════════════════════════

class VLMEndpointConfig(BaseModel):
    url: str = DEFAULT_VLLM_HOST
    model: str = DEFAULT_VLM_MODEL
    # MIG slice metadata (informational + used by run.sh)
    mig_uuid: str = ""
    mig_profile: str = ""
    sm_count: int = 0
    # Queueing & dispatch weights — sized to the MIG slice compute ratio
    weight: int = 1            # routing weight; 3 for 12SM shard, 2 for 8SM shard
    max_concurrent: int = 3    # max simultaneous in-flight HTTP requests to this shard
    queue_depth: int = 12      # bounded queue depth before overflow routing kicks in
    # vLLM launch parameter — passed as --gpu-memory-utilization per container
    # 12SM shard: 0.80 (leaves headroom for NVDEC/GUI on the same MIG slice)
    # 8SM shard:  0.95 (dedicated to VLLM only)
    gpu_utilization: float = 0.95


class SystemConfig(BaseModel):
    """Structured representation of config/system.json."""
    vllm_endpoints: List[VLMEndpointConfig] = Field(
        default_factory=lambda: [VLMEndpointConfig(url=DEFAULT_VLLM_HOST, model=DEFAULT_VLM_MODEL)]
    )
    auto_start_vllm: bool = True
    vllm_model: str = DEFAULT_VLM_MODEL
    vllm_port: int = DEFAULT_VLLM_PORT
    vllm_max_seqs: int = 4
    vllm_max_model_len: int = 4096
    vllm_gpu_utilization: float = 0.35
    vllm_instances: int = 1
    primary_model: str = DEFAULT_VLM_MODEL
    vllm_quantization: str = ""
    vllm_port_start: int = DEFAULT_VLLM_PORT
    
    ingest_backend: str = "nvidia"
    trigger_backend: str = "dinov2"
    dinov2_model: str = DEFAULT_DINOV2_MODEL
    
    scene_threshold: float = DEFAULT_SCENE_THRESHOLD
    default_threshold: float = DEFAULT_SCENE_THRESHOLD
    dino_major_threshold: float = DINO_MAJOR_THRESHOLD
    dino_minor_threshold: float = DINO_MINOR_THRESHOLD
    default_heartbeat_sec: float = DEFAULT_HEARTBEAT_SEC
    semantic_interval: float = DEFAULT_SEMANTIC_INTERVAL
    event_cooldown: float = DEFAULT_EVENT_COOLDOWN
    
    followup_interval_sec: float = DEFAULT_FOLLOWUP_INTERVAL
    persistent_followup: bool = DEFAULT_PERSISTENT_FOLLOWUP
    followup_max_cycles: int = DEFAULT_FOLLOWUP_MAX_CYCLES
    
    clip_recording_enabled: bool = True
    clip_rolling_buffer_enabled: bool = True
    clip_retention_hours: float = 24.0

    alert_severity_triggers: List[str] = Field(default_factory=lambda: list(DEFAULT_ALERT_SEVERITIES))
    alert_safety_triggers: List[str] = Field(default_factory=lambda: list(DEFAULT_ALERT_SAFETIES))

    initial_concurrency: int = DEFAULT_INITIAL_CONCURRENCY
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY
    frame_width: int = DEFAULT_FRAME_WIDTH
    jpeg_quality: int = DEFAULT_JPEG_QUALITY
    dashboard_port: int = DEFAULT_DASHBOARD_PORT


# ══════════════════════════════════════════════════════════════════════
#  Configuration Loader & Persistence Manager
# ══════════════════════════════════════════════════════════════════════

class ConfigManager:
    """Manages loading, updating, and saving the system configuration."""

    def __init__(self, config_path: Path = SYSTEM_CONFIG_PATH):
        self.config_path = config_path
        self._cached_config: SystemConfig = self.load()

    def load(self) -> SystemConfig:
        """Load configuration from disk, falling back to defaults for missing fields."""
        data: Dict[str, Any] = {}
        if self.config_path.exists():
            try:
                with open(self.config_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as e:
                print(f"[ConfigManager] ⚠️ Error loading {self.config_path}: {e}")
        
        # Merge with defaults via Pydantic
        try:
            self._cached_config = SystemConfig(**data)
        except Exception as e:
            print(f"[ConfigManager] ⚠️ Validation error in system config, using defaults: {e}")
            self._cached_config = SystemConfig()
            
        return self._cached_config

    def get(self) -> SystemConfig:
        return self._cached_config

    def as_dict(self) -> Dict[str, Any]:
        return self._cached_config.model_dump()

    def update(self, updates: Dict[str, Any]) -> SystemConfig:
        """Apply partial updates, validate, and persist to system.json."""
        current_dict = self._cached_config.model_dump()
        current_dict.update({k: v for k, v in updates.items() if v is not None})
        
        # If threshold updated, keep scene_threshold and default_threshold in sync
        if "default_threshold" in updates:
            current_dict["scene_threshold"] = updates["default_threshold"]
        elif "scene_threshold" in updates:
            current_dict["default_threshold"] = updates["scene_threshold"]

        self._cached_config = SystemConfig(**current_dict)
        self.save()
        return self._cached_config

    def save(self) -> bool:
        """Persist current configuration to system.json."""
        try:
            self.config_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.config_path, "w", encoding="utf-8") as f:
                json.dump(self._cached_config.model_dump(), f, indent=2)
            return True
        except Exception as e:
            print(f"[ConfigManager] ❌ Failed to save {self.config_path}: {e}")
            return False


# Global singleton instance
config_manager = ConfigManager()
