"""
RapidAlert Schemas Package
Exports all request and response models.
"""
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
    AlertResponse,
    SystemConfigResponse,
    MetricsResponse,
    ErrorRecordResponse,
    ErrorListResponse,
    ErrorSummaryResponse,
)

__all__ = [
    "CameraBody",
    "SystemConfigBody",
    "PromptBody",
    "ScanBody",
    "TestAlertBody",
    "LoginBody",
    "SetupAdminBody",
    "ChangePasswordBody",
    "StandardStatusResponse",
    "AlertResponse",
    "SystemConfigResponse",
    "MetricsResponse",
    "ErrorRecordResponse",
    "ErrorListResponse",
    "ErrorSummaryResponse",
]
