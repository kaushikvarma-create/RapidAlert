"""
RapidAlert Backend Package
High-Speed Hybrid VLM Surveillance Engine for NVIDIA Jetson Thor.
"""
from backend.core.config import config_manager, SystemConfig
from backend.core.error_tracker import error_tracker, ErrorRecord

__version__ = "2.0.0"

__all__ = [
    "config_manager",
    "SystemConfig",
    "error_tracker",
    "ErrorRecord",
]
