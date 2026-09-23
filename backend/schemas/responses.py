"""
RapidAlert Response Schemas
Pydantic models for REST API response payloads and WebSocket message payloads.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class StandardStatusResponse(BaseModel):
    """Generic status response."""
    status: str = Field("ok", description="Status string ('ok' or 'error')")
    message: Optional[str] = Field(None, description="Optional informative message")
    data: Optional[Any] = Field(None, description="Optional payload")


class AlertResponse(BaseModel):
    """Detailed alert item representation."""
    id: str = Field(..., description="Unique event ID (e.g. EVT-CAM1-20260921-001-TRIGGER)")
    incident_id: Optional[str] = Field(None, description="Unique incident thread identifier")
    parent_id: Optional[str] = Field(None, description="Parent event ID if this is a follow-up")
    followup_id: Optional[str] = Field(None, description="Child follow-up event ID if already linked")
    cam: str = Field(..., description="Camera identifier")
    ts: float = Field(..., description="Unix epoch timestamp")
    timestamp: str = Field(..., description="Human-readable timestamp (YYYY-MM-DD HH:MM:SS)")
    severity: str = Field("LOW", description="Evaluated severity: 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'")
    safety: str = Field("OK", description="Evaluated safety status: 'OK', 'WARNING', 'DANGER', 'UNKNOWN'")
    trigger_mode: str = Field("PERIODIC", description="Mode: 'TRIGGER', 'FOLLOWUP', 'PERIODIC'")
    trigger_badge: str = Field("", description="Visual badge label for dashboard cards")
    observation: str = Field("", description="VLM scene description and safety reasoning")
    activity: str = Field("UNKNOWN", description="Primary activity detected")
    workers: str = Field("0", description="Worker presence or count")
    machinery: str = Field("None", description="Active machinery or equipment detected")
    evolution: str = Field("", description="Evolution comparison to previous scene")
    model: str = Field("", description="Model identifier used for inference")
    is_incident: bool = Field(False, description="True if triggered by DINOv2 scene drift")
    is_followup: bool = Field(False, description="True if scheduled follow-up check")
    is_periodic: bool = Field(True, description="True if regular periodic heartbeat check")
    cycle: int = Field(0, description="Follow-up cycle number (0 for initial/periodic)")
    delay_sec: float = Field(0.0, description="Follow-up delay in seconds")
    drift: Optional[float] = Field(None, description="DINOv2 drift score if triggered")
    latency: Optional[float] = Field(None, description="VLM query inference latency in seconds")
    e2e_latency: Optional[float] = Field(None, description="End-to-end pipeline latency from trigger to post")
    thumbnail_b64: Optional[str] = Field(None, description="High-resolution event frame base64 JPEG")
    thumbnails_b64: List[str] = Field(default_factory=list, description="Multi-frame temporal sequence base64 JPEGs")
    labels: List[str] = Field(default_factory=list, description="Temporal offset labels for each frame")


class SystemConfigResponse(BaseModel):
    """Full system and camera configuration response."""
    system: Dict[str, Any]
    cameras: List[Dict[str, Any]]


class MetricsResponse(BaseModel):
    """Hardware and pipeline real-time telemetry metrics."""
    analyses_per_min: Optional[float] = None
    p50_latency: Optional[float] = None
    p95_latency: Optional[float] = None
    concurrency: int = 0
    queue_depth: int = 0
    in_flight: int = 0
    gpu: Optional[int] = None
    cpu: Optional[float] = None
    ram: Optional[float] = None


class ErrorRecordResponse(BaseModel):
    """Structured error record capturing what happened, where, and its operational impact."""
    id: str = Field(..., description="Unique error identifier (e.g. ERR-20260921-001)")
    timestamp: str = Field(..., description="ISO 8601 formatted timestamp")
    ts: float = Field(..., description="Unix epoch timestamp")
    component: str = Field(..., description="Subsystem component (e.g. 'Scheduler', 'VLMClient')")
    function: str = Field("", description="Function or method where the error occurred")
    file: str = Field("", description="Source filename and line number")
    line: int = Field(0, description="Line number where the error occurred")
    camera: Optional[str] = Field(None, description="Camera name if associated with a stream")
    error_type: str = Field(..., description="Exception class name (e.g. 'TimeoutError', 'ClientError')")
    message: str = Field(..., description="Exact error message")
    effect: str = Field(..., description="Concrete downstream operational impact on the pipeline")
    stack_trace: str = Field("", description="Formatted traceback string for troubleshooting")
    severity: str = Field("ERROR", description="'CRITICAL', 'ERROR', or 'WARNING'")


class ErrorListResponse(BaseModel):
    """List of recent errors with summary stats."""
    count: int
    errors: List[ErrorRecordResponse]


class ErrorSummaryResponse(BaseModel):
    """Aggregated statistics of recorded errors."""
    total_errors: int
    by_component: Dict[str, int]
    by_severity: Dict[str, int]
    by_effect: Dict[str, int]
