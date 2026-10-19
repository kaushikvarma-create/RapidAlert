"""
Hardware metrics monitor: reads GPU, CPU, and RAM metrics and broadcasts via WebSocket.

MIG mode note: In MIG mode, nvidia-smi returns [N/A] for parent-GPU utilization.
get_gpu_util() tries three methods in order, gracefully degrading.
"""
from __future__ import annotations

import asyncio
import time
from typing import Callable, Optional, Tuple

from backend.core.error_tracker import error_tracker

_LAST_GPU_WARN = 0.0
_LAST_CPU_WARN = 0.0
_LAST_RAM_WARN = 0.0
_NVML_INITED = False
_NVML_DEV = None


def _init_nvml_safe():
    global _NVML_INITED, _NVML_DEV
    if not _NVML_INITED:
        try:
            import pynvml
            pynvml.nvmlInit()
            _NVML_DEV = pynvml.nvmlDeviceGetHandleByIndex(0)
            _NVML_INITED = True
        except Exception:
            _NVML_INITED = False


async def get_gpu_util() -> int:
    """
    Query GPU/MIG compute utilization via direct NVML (sub-millisecond, zero subprocesses).
    """
    global _LAST_GPU_WARN
    _init_nvml_safe()
    if _NVML_INITED and _NVML_DEV is not None:
        try:
            import pynvml
            # 1. Direct utilization rates (when supported)
            try:
                util = pynvml.nvmlDeviceGetUtilizationRates(_NVML_DEV)
                if util and util.gpu is not None:
                    return int(util.gpu)
            except Exception:
                pass

            # 2. Jetson Thor dynamic power draw (18W idle -> 120W TDP)
            pwr_mw = pynvml.nvmlDeviceGetPowerUsage(_NVML_DEV)
            if pwr_mw and pwr_mw > 0:
                pwr_w = pwr_mw / 1000.0
                dynamic_pct = max(0, min(100, int(((pwr_w - 18.0) / (120.0 - 18.0)) * 100)))
                return dynamic_pct
        except Exception:
            pass

    now = time.time()
    if now - _LAST_GPU_WARN > 300:
        _LAST_GPU_WARN = now
        error_tracker.capture_error(
            message="GPU NVML telemetry unavailable; defaulting to 0%",
            component="MetricsMonitor",
            effect="GPU utilization telemetry defaulted to 0%",
            severity="WARNING",
        )
    return 0


def get_cpu_util(prev_idle: float, prev_total: float) -> Tuple[float, float, float]:
    global _LAST_CPU_WARN
    try:
        with open('/proc/stat', 'r', encoding='utf-8') as f:
            lines = f.readlines()
        cpu_line = lines[0].split()
        idle = float(cpu_line[4]) + float(cpu_line[5])
        total = sum(float(x) for x in cpu_line[1:])

        diff_idle = idle - prev_idle
        diff_total = total - prev_total

        util = 100 * (1.0 - diff_idle / diff_total) if diff_total > 0 else 0
        return round(util, 1), idle, total
    except Exception as exc:
        now = time.time()
        if now - _LAST_CPU_WARN > 60:
            _LAST_CPU_WARN = now
            error_tracker.capture_exception(
                exc,
                component="MetricsMonitor",
                effect="Failed to read /proc/stat; CPU telemetry defaulted to 0%",
                severity="WARNING",
            )
        return 0.0, prev_idle, prev_total


def get_ram_util() -> float:
    global _LAST_RAM_WARN
    try:
        with open('/proc/meminfo', 'r', encoding='utf-8') as f:
            lines = f.readlines()
        mem_total = 0
        mem_avail = 0
        for line in lines:
            if line.startswith('MemTotal:'):
                mem_total = int(line.split()[1])
            elif line.startswith('MemAvailable:'):
                mem_avail = int(line.split()[1])
        if mem_total > 0:
            return round(100 * (1.0 - (mem_avail / mem_total)), 1)
        return 0.0
    except Exception as exc:
        now = time.time()
        if now - _LAST_RAM_WARN > 60:
            _LAST_RAM_WARN = now
            error_tracker.capture_exception(
                exc,
                component="MetricsMonitor",
                effect="Failed to read /proc/meminfo; RAM telemetry defaulted to 0%",
                severity="WARNING",
            )
        return 0.0


async def metrics_loop(broadcast_fn: Optional[Callable]) -> None:
    prev_idle, prev_total = 0.0, 0.0
    _, prev_idle, prev_total = get_cpu_util(prev_idle, prev_total)

    while True:
        try:
            await asyncio.sleep(2)
            gpu = await get_gpu_util()
            cpu, prev_idle, prev_total = get_cpu_util(prev_idle, prev_total)
            ram = get_ram_util()

            if broadcast_fn:
                await broadcast_fn({
                    "type": "sys_metrics",
                    "gpu": gpu,
                    "cpu": cpu,
                    "ram": ram,
                })
        except asyncio.CancelledError:
            break
        except Exception as exc:
            error_tracker.capture_exception(
                exc,
                component="MetricsMonitor",
                effect="Error in hardware metrics loop; pausing 2s",
                severity="WARNING",
            )
