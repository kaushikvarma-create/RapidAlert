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


async def get_gpu_util() -> int:
    """
    Query GPU/MIG compute utilization.

    MIG mode causes `utilization.gpu` to return `[N/A]` for the parent GPU.
    Strategy:
      1. Try standard `utilization.gpu` — works on non-MIG systems.
      2. On [N/A]/failure, query per-MIG-instance SM utilization via
         `--query-mig=gpu.utilization` (available on driver 520+).
      3. Final fallback: count active compute processes via nvidia-smi pmon.
    """
    global _LAST_GPU_WARN

    # ── Attempt 1: standard GPU utilization ────────────────────────────────
    try:
        proc = await asyncio.create_subprocess_shell(
            "nvidia-smi --query-gpu=utilization.gpu --format=csv,noheader,nounits",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=3.0)
        if proc.returncode == 0 and stdout:
            val = stdout.decode().strip()
            # MIG mode returns '[N/A]' — fall through to MIG-specific queries
            if val and not val.startswith("["):
                return int(val)
    except Exception:
        pass

    # ── Attempt 2: MIG per-instance utilization (driver 520+) ─────────────
    try:
        proc = await asyncio.create_subprocess_shell(
            "nvidia-smi --query-mig=gpu.utilization --format=csv,noheader,nounits 2>/dev/null",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=3.0)
        if proc.returncode == 0 and stdout:
            lines = [l.strip() for l in stdout.decode().strip().splitlines() if l.strip()]
            values = []
            for line in lines:
                if line and not line.startswith("["):
                    try:
                        values.append(int(line))
                    except ValueError:
                        pass
            if values:
                return max(values)  # report the busiest MIG slice
    except Exception:
        pass

    # ── Attempt 3: count active compute processes via nvidia-smi pmon ──────
    try:
        proc = await asyncio.create_subprocess_shell(
            "nvidia-smi pmon -c 1 -s u 2>/dev/null | awk 'NR>2 && $2 != \"-\" {count++} END {print count+0}'",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=3.0)
        if proc.returncode == 0 and stdout:
            n = int(stdout.decode().strip() or "0")
            return min(99, n * 20)  # each active process implies load; cap at 99
    except Exception:
        pass

    # ── All methods failed ─────────────────────────────────────────────────
    now = time.time()
    if now - _LAST_GPU_WARN > 300:   # warn at most once per 5 minutes
        _LAST_GPU_WARN = now
        error_tracker.capture_error(
            message="All GPU utilization query methods returned [N/A] or failed (MIG mode may restrict access)",
            component="MetricsMonitor",
            effect="GPU utilization telemetry unavailable; dashboard will show 0%",
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
