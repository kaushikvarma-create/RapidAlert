"""
Core package for RapidAlert containing system configuration and error tracking.
"""
from backend.core.config import (
    ROOT_DIR,
    CONFIG_DIR,
    DATA_DIR,
    FRONTEND_DIR,
    LOGS_DIR,
    SYSTEM_CONFIG_PATH,
    CAMERAS_CONFIG_PATH,
    PROMPTS_CONFIG_PATH,
    DATABASE_PATH,
    SystemConfig,
    ConfigManager,
    config_manager,
)

__all__ = [
    "ROOT_DIR",
    "CONFIG_DIR",
    "DATA_DIR",
    "FRONTEND_DIR",
    "LOGS_DIR",
    "SYSTEM_CONFIG_PATH",
    "CAMERAS_CONFIG_PATH",
    "PROMPTS_CONFIG_PATH",
    "DATABASE_PATH",
    "SystemConfig",
    "ConfigManager",
    "config_manager",
]
