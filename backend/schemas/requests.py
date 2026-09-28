"""
RapidAlert Request Schemas
Pydantic models for REST API request payloads.
"""
from __future__ import annotations

from typing import Optional
from pydantic import BaseModel, Field


class CameraBody(BaseModel):
    """Payload for creating or updating a camera stream."""
    name: str = Field(..., description="Unique camera identifier or stream name")
    url: str = Field("", description="RTSP, HTTP, or video file source URL")
    enabled: bool = Field(True, description="Whether this camera feed is actively ingested")
    normal_context_day: str = Field("", description="Expected daytime activity context to reduce false positives")
    normal_context_night: str = Field("", description="Expected nighttime activity context")
    priority: str = Field("normal", description="Priority level: 'normal', 'high', 'critical'")
    threshold: Optional[float] = Field(None, description="Legacy DINOv2 scene drift sensitivity override")
    major_threshold: Optional[float] = Field(None, description="Per-camera DINOv2 Major Shift trigger threshold [T2]")
    minor_threshold: Optional[float] = Field(None, description="Per-camera DINOv2 Minor Shift trigger threshold [T3]")
    heartbeat_sec: Optional[float] = Field(None, description="Per-camera periodic analysis heartbeat interval")


class SystemConfigBody(BaseModel):
    """Payload for updating system surveillance and model configuration."""
    default_threshold: Optional[float] = Field(None, description="Global DINOv2 scene drift trigger threshold")
    dino_major_threshold: Optional[float] = Field(None, description="Global DINOv2 Major Shift threshold [T2]")
    dino_minor_threshold: Optional[float] = Field(None, description="Global DINOv2 Minor Shift threshold [T3]")
    default_heartbeat_sec: Optional[float] = Field(None, description="Global fallback heartbeat analysis interval")
    event_cooldown: Optional[float] = Field(None, description="Cooldown between triggers on the same camera in seconds")
    semantic_interval: Optional[float] = Field(None, description="Interval between DINOv2 frame embedding checks")
    followup_enabled: Optional[bool] = Field(None, description="Master toggle for temporal follow-up analysis")
    followup_interval_sec: Optional[float] = Field(None, description="Delay before follow-up re-analysis in seconds")
    persistent_followup: Optional[bool] = Field(None, description="Whether follow-ups repeat until scene stabilizes")
    followup_max_cycles: Optional[int] = Field(None, description="Maximum number of persistent follow-up cycles")
    clip_recording_enabled: Optional[bool] = Field(None, description="Enable automatic MP4 video clip recording on alerts")
    clip_rolling_buffer_enabled: Optional[bool] = Field(None, description="Enable rolling retention window pruner for clips")
    clip_retention_hours: Optional[float] = Field(None, description="Retention window for video clips in hours")


class PromptBody(BaseModel):
    """Payload for updating master system prompt or follow-up prompt templates."""
    master: Optional[str] = Field(None, description="Master VLM system prompt for initial trigger analysis")
    master_scene_context: Optional[str] = Field(None, description="Global master scene context describing the monitored facility")
    followup: Optional[str] = Field(None, description="Follow-up VLM prompt template for re-check analysis")
    cam_name: Optional[str] = Field(None, description="Target camera name for custom override")
    cam_prompt: Optional[str] = Field(None, description="Per-camera prompt text (empty string removes override)")


class ScanBody(BaseModel):
    """Payload for triggering RTSP / ONVIF network camera scan."""
    subnet: Optional[str] = Field(None, description="Subnet CIDR (e.g. '192.168.1.0/24') or None for auto-detect")
    ws_timeout: float = Field(3.0, description="ONVIF WS-Discovery probe timeout in seconds")
    port_timeout: float = Field(0.4, description="RTSP TCP port 554 connection timeout in seconds")
    username: Optional[str] = Field(None, description="Optional camera RTSP username for auth probe")
    password: Optional[str] = Field(None, description="Optional camera RTSP password for auth probe")


class TestAlertBody(BaseModel):
    """Payload for initiating a synthetic test incident."""
    cam: Optional[str] = Field(None, description="Camera to trigger test on, or first active camera if omitted")
    severity: str = Field("HIGH", description="Target severity for synthetic alert ('HIGH' or 'MEDIUM')")


class LoginBody(BaseModel):
    """Payload for admin authentication."""
    username: str = Field(..., description="Admin username")
    password: str = Field(..., description="Admin password")


class SetupAdminBody(BaseModel):
    """Payload for initializing master admin credentials."""
    username: str = Field("admin", description="Master admin username")
    password: str = Field(..., description="Master admin password (min 4 chars)")


class ChangePasswordBody(BaseModel):
    """Payload for updating admin master password."""
    old_password: str = Field(..., description="Current admin password")
    new_password: str = Field(..., description="New admin password")
