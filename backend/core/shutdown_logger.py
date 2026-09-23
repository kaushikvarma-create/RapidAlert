"""
Shutdown & Lifecycle Event Logger for RapidAlert.
Logs process starts, graceful terminations (Ctrl+C / SIGINT / SIGTERM),
and unexpected crashes with actionable error diagnostics and next steps.
"""
from __future__ import annotations

import os
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
LOGS_DIR = ROOT_DIR / "logs"
EVENTS_LOG_PATH = LOGS_DIR / "system_events.log"


def log_system_event(
    event_type: str,
    reason: str,
    uptime_sec: float = 0.0,
    details: dict | None = None,
    action_required: str | None = None,
) -> None:
    """Appends structured event entry to logs/system_events.log and prints console summary."""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Format uptime string
    hours = int(uptime_sec // 3600)
    minutes = int((uptime_sec % 3600) // 60)
    seconds = int(uptime_sec % 60)
    uptime_str = f"{hours}h {minutes}m {seconds}s" if hours > 0 else f"{minutes}m {seconds}s"

    action = action_required or "None. To restart surveillance, run: ./run.sh"

    log_entry = [
        f"[{timestamp}] [SYSTEM_EVENT: {event_type.upper()}]",
        f"  Reason         : {reason}",
        f"  Uptime         : {uptime_str}",
    ]
    if details:
        for k, v in details.items():
            log_entry.append(f"  {k:<15}: {v}")
    log_entry.append(f"  Action Required: {action}\n")

    entry_text = "\n".join(log_entry)

    # 1. Write to persistent log file
    try:
        with open(EVENTS_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(entry_text + "\n")
    except Exception:
        pass

    # 2. Print high-visibility shutdown banner to stderr / stdout
    border = "═" * 70
    sub_border = "─" * 70
    color_code = "\033[93m" if "SHUTDOWN" in event_type.upper() or "INT" in reason else "\033[91m"
    nc = "\033[0m"
    bold = "\033[1m"

    print(f"\n{color_code}{bold}{border}{nc}")
    print(f"{color_code}{bold} 🛑 RAPIDALERT {event_type.upper()}{nc}")
    print(f"{color_code}{sub_border}{nc}")
    print(f" {bold}Trigger Reason{nc}   : {reason}")
    print(f" {bold}Active Uptime{nc}    : {uptime_str}")
    if details:
        for k, v in details.items():
            print(f" {bold}{k:<17}{nc}: {v}")
    print(f" {bold}Action Required{nc}  : {action}")
    print(f"{color_code}{bold}{border}{nc}\n")
