"""
ContainerManager — Docker container health monitor, crash detector, and self-healing engine.
Monitors vLLM containers on NVIDIA Thor MIG slices, detects abnormal terminations/OOMs,
restarts containers automatically with crash-loop backoff protection, and gathers deep diagnostics.
"""
from __future__ import annotations

import asyncio
import glob
import json
import os
import shutil
import subprocess
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

from backend.core.config import SYSTEM_CONFIG_PATH
from backend.core.error_tracker import error_tracker


class ContainerManager:
    """
    Supervises Docker containers powering the RapidAlert AI surveillance system.
    Provides non-blocking container inspection, log extraction, auto-restart self-healing,
    and structured diagnostic collection for executive watchdog email alerts.
    """

    def __init__(self, config_path: Path = SYSTEM_CONFIG_PATH):
        self.config_path = config_path
        # Restart history per container: {container_name: deque([timestamp1, timestamp2, ...])}
        self._restart_history: dict[str, deque[float]] = {}
        # Cooldown per container to avoid thrashing during warmup: {container_name: last_restart_ts}
        self._last_restart_ts: dict[str, float] = {}
        # Consecutive failed health checks while container is in "running" state
        self._consecutive_unresponsive: dict[str, int] = {}
        # Maximum allowed restarts within the rolling window
        self.max_restarts_in_window = 3
        self.window_seconds = 600.0  # 10 minutes
        self.warmup_grace_seconds = 180.0  # Allow 3 minutes for AWQ model loading before flagging unresponsive

    def _get_sys_config(self) -> dict:
        try:
            if self.config_path.exists():
                with open(self.config_path, "r") as f:
                    return json.load(f)
        except Exception:
            pass
        return {}

    @staticmethod
    def parse_uptime_seconds(started_at_str: str) -> float:
        """Parses Docker ISO 8601 State.StartedAt into elapsed uptime seconds."""
        if not started_at_str or started_at_str.startswith("0001-01-01"):
            return 0.0
        try:
            clean_ts = started_at_str.replace("Z", "+00:00")
            dt = datetime.fromisoformat(clean_ts)
            now_utc = datetime.now(timezone.utc)
            return max(0.0, (now_utc - dt).total_seconds())
        except Exception:
            return 0.0

    async def inspect_container(self, name: str) -> dict:
        """
        Inspects container state via `docker inspect`.
        Returns structured dictionary with Running, Status, ExitCode, OOMKilled, Error, etc.
        """
        try:
            proc = await asyncio.create_subprocess_exec(
                "docker", "inspect", name, "--format", "{{json .State}}",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=10.0)
            if proc.returncode == 0 and stdout:
                state = json.loads(stdout.decode().strip())
                started_at = state.get("StartedAt", "")
                return {
                    "exists": True,
                    "name": name,
                    "running": state.get("Running", False),
                    "status": state.get("Status", "unknown"),
                    "exit_code": state.get("ExitCode", 0),
                    "oom_killed": state.get("OOMKilled", False),
                    "dead": state.get("Dead", False),
                    "error": state.get("Error", ""),
                    "started_at": started_at,
                    "finished_at": state.get("FinishedAt", ""),
                    "pid": state.get("Pid", 0),
                    "uptime_sec": self.parse_uptime_seconds(started_at),
                }
            else:
                return {
                    "exists": False,
                    "name": name,
                    "running": False,
                    "status": "not_found",
                    "exit_code": -1,
                    "oom_killed": False,
                    "dead": False,
                    "error": stderr.decode().strip() or "Container does not exist",
                    "uptime_sec": 0.0,
                }
        except asyncio.TimeoutError:
            # Heavy host load or slow CLI inspect.
            # CRITICAL: Treat as still running so watchdog NEVER restarts a healthy container on CLI lag!
            return {
                "exists": True,
                "name": name,
                "running": True,
                "status": "inspect_timeout",
                "exit_code": 0,
                "oom_killed": False,
                "dead": False,
                "error": "docker inspect timed out after 10.0s (assumed running)",
                "uptime_sec": 0.0,
            }
        except Exception as exc:
            return {
                "exists": False,
                "name": name,
                "running": False,
                "status": "error",
                "exit_code": -1,
                "oom_killed": False,
                "dead": False,
                "error": str(exc),
                "uptime_sec": 0.0,
            }

    async def get_container_logs(self, name: str, lines: int = 40) -> str:
        """Retrieves the last N lines of stdout/stderr from the container."""
        try:
            proc = await asyncio.create_subprocess_exec(
                "docker", "logs", "--tail", str(lines), name,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=4.0)
            text = stdout.decode("utf-8", errors="replace").strip()
            return text if text else "(No recent log lines captured)"
        except Exception as exc:
            return f"(Unable to read docker logs: {exc})"

    def can_restart(self, name: str) -> tuple[bool, str]:
        """
        Enforces crash-loop backoff protection.
        Prevents rapid infinite restart loops if weights are corrupted or GPU is wedged.
        """
        now = time.monotonic()
        history = self._restart_history.setdefault(name, deque())

        # Prune events older than the sliding window
        while history and (now - history[0] > self.window_seconds):
            history.popleft()

        # Check if in immediate restart cooldown (minimum 30s between consecutive restart actions)
        last_restart = self._last_restart_ts.get(name, 0.0)
        if now - last_restart < 30.0:
            remaining = int(30.0 - (now - last_restart))
            return False, f"Throttled: container restarted {int(now - last_restart)}s ago (wait {remaining}s)"

        # Check max restarts within window
        if len(history) >= self.max_restarts_in_window:
            oldest = history[0]
            cooldown_remaining = int(self.window_seconds - (now - oldest))
            return False, (
                f"Crash-loop backoff active ({len(history)} restarts in {int(self.window_seconds // 60)}m). "
                f"Backoff remaining: {cooldown_remaining}s. Manual investigation recommended."
            )

        return True, "OK"

    async def restart_container(self, name: str, timeout_sec: int = 8) -> tuple[bool, str]:
        """
        Restarts an existing container with timeout protection and records history.
        """
        can_res, reason = self.can_restart(name)
        if not can_res:
            return False, reason

        now = time.monotonic()
        self._restart_history.setdefault(name, deque()).append(now)
        self._last_restart_ts[name] = now
        self._consecutive_unresponsive[name] = 0

        print(f"[ContainerManager] 🔄 Initiating automated restart for container '{name}'...")
        try:
            proc = await asyncio.create_subprocess_exec(
                "docker", "restart", "-t", str(timeout_sec), name,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout_sec + 5.0)
            if proc.returncode == 0:
                print(f"[ContainerManager] ✅ Container '{name}' successfully restarted.")
                return True, f"Container '{name}' restarted successfully"
            else:
                err_msg = stderr.decode().strip() or stdout.decode().strip()
                print(f"[ContainerManager] ❌ docker restart failed for '{name}': {err_msg}")
                return False, f"docker restart failed: {err_msg}"
        except asyncio.TimeoutError:
            # Force kill if graceful restart timed out
            try:
                await asyncio.create_subprocess_exec("docker", "kill", name)
                await asyncio.sleep(1.0)
                await asyncio.create_subprocess_exec("docker", "start", name)
                return True, f"Container '{name}' force-cycled after restart timeout"
            except Exception as e:
                return False, f"Force-cycle failed: {e}"
        except Exception as exc:
            return False, f"Error restarting container: {exc}"

    async def start_container(self, name: str) -> tuple[bool, str]:
        """Starts an existing stopped/exited container."""
        can_res, reason = self.can_restart(name)
        if not can_res:
            return False, reason

        now = time.monotonic()
        self._restart_history.setdefault(name, deque()).append(now)
        self._last_restart_ts[name] = now
        self._consecutive_unresponsive[name] = 0

        print(f"[ContainerManager] 🚀 Starting stopped container '{name}'...")
        try:
            proc = await asyncio.create_subprocess_exec(
                "docker", "start", name,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=10.0)
            if proc.returncode == 0:
                print(f"[ContainerManager] ✅ Container '{name}' started.")
                return True, f"Container '{name}' started successfully"
            else:
                err_msg = stderr.decode().strip() or stdout.decode().strip()
                return False, f"docker start failed: {err_msg}"
        except Exception as exc:
            return False, f"Error starting container: {exc}"

    async def launch_container_from_config(self, index: int, endpoint_cfg: dict) -> tuple[bool, str]:
        """
        Recreates and launches a missing or irrecoverable vLLM container
        using official NVIDIA Thor JetPack 7.2 MIG configuration.
        """
        cname = endpoint_cfg.get("container_name") or f"rapidalert_vllm_{index}"
        can_res, reason = self.can_restart(cname)
        if not can_res:
            return False, reason

        sys_cfg = self._get_sys_config()
        port = int(endpoint_cfg.get("url", f":{8000 + index}").split(":")[-1])
        model = endpoint_cfg.get("model", sys_cfg.get("vllm_model", "cyankiwi/Qwen3.5-9B-AWQ-4bit"))
        gpu_util = endpoint_cfg.get("gpu_utilization", sys_cfg.get("vllm_gpu_utilization", 0.2))
        max_model_len = sys_cfg.get("vllm_max_model_len", 4096)
        max_seqs = sys_cfg.get("vllm_max_seqs", 4)
        vllm_image = "ghcr.io/nvidia-ai-iot/vllm:latest-jetson-thor"
        hf_cache = os.path.expanduser("~/huggingface")

        # Clean any old broken container
        try:
            await asyncio.create_subprocess_exec("docker", "rm", "-f", cname)
            await asyncio.sleep(0.5)
        except Exception:
            pass

        # Build capability flags for NVIDIA Thor MIG
        cap_flags = []
        if os.path.isdir("/dev/nvidia-caps"):
            # Root MIG configuration capability — required by all containers
            if os.path.exists("/dev/nvidia-caps/nvidia-cap1"):
                cap_flags.extend(["--device", "/dev/nvidia-caps/nvidia-cap1"])
            gi_id = index + 1
            for cap_file in sorted(glob.glob(f"/dev/nvidia-caps/nvidia-cap{gi_id}*")):
                if cap_file != "/dev/nvidia-caps/nvidia-cap1":
                    cap_flags.extend(["--device", cap_file])

        cmd = [
            "docker", "run", "-d",
            "--name", cname,
            "--runtime", "nvidia",
            "--network", "host",
            "--shm-size=4g",
            "-e", "NVIDIA_VISIBLE_DEVICES=all",
            "-e", "CUDA_VISIBLE_DEVICES=0",
            "-e", "VLLM_HOST_IP=127.0.0.1",
            "-e", "NCCL_SOCKET_IFNAME=lo",
            "-e", "GLOO_SOCKET_IFNAME=lo",
            "-e", "HF_HOME=/data/models/huggingface",
            "-e", f"EP_MODEL={model}",
            "-e", "EP_QUANTIZATION=",
            *cap_flags,
            "-v", f"{hf_cache}:/data/models/huggingface",
            vllm_image,
            "bash", "-c",
            (
                f'vllm serve "{model}" '
                f'--host 0.0.0.0 '
                f'--port {port} '
                f'--max-model-len {max_model_len} '
                f'--gpu-memory-utilization {gpu_util} '
                f'--dtype auto '
                f'--enforce-eager '
                f'--max-num-seqs {max_seqs} '
                f'--trust-remote-code '
                f'--disable-log-stats '
                f'--no-enable-log-requests'
            ),
        ]

        now = time.monotonic()
        self._restart_history.setdefault(cname, deque()).append(now)
        self._last_restart_ts[cname] = now
        self._consecutive_unresponsive[cname] = 0

        print(f"[ContainerManager] 📦 Deploying fresh container '{cname}' on port {port}...")
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=20.0)
            if proc.returncode == 0:
                cid = stdout.decode().strip()[:12]
                print(f"[ContainerManager] ✅ Fresh container '{cname}' launched (CID: {cid}).")
                return True, f"Container '{cname}' launched fresh (ID: {cid})"
            else:
                err_msg = stderr.decode().strip() or stdout.decode().strip()
                print(f"[ContainerManager] ❌ docker run failed for '{cname}': {err_msg}")
                return False, f"docker run failed: {err_msg}"
        except Exception as exc:
            return False, f"Exception deploying container: {exc}"

    def get_hardware_telemetry(self) -> dict:
        """Retrieves current host memory, swap, and GPU telemetry for watchdog reporting."""
        mem_info = {"ram_free_gb": 0.0, "ram_total_gb": 0.0, "swap_free_gb": 0.0}
        try:
            with open("/proc/meminfo", "r") as f:
                lines = f.readlines()
            kvs = {}
            for line in lines:
                parts = line.split(":")
                if len(parts) == 2:
                    k = parts[0].strip()
                    val = parts[1].strip().split()[0]
                    kvs[k] = float(val) / (1024 * 1024)  # GB
            mem_info["ram_total_gb"] = round(kvs.get("MemTotal", 0.0), 1)
            mem_info["ram_free_gb"] = round(kvs.get("MemAvailable", kvs.get("MemFree", 0.0)), 1)
            mem_info["swap_free_gb"] = round(kvs.get("SwapFree", 0.0), 1)
            mem_info["swap_total_gb"] = round(kvs.get("SwapTotal", 0.0), 1)
        except Exception:
            pass

        gpu_summary = "NVIDIA Thor (MIG Enabled)"
        try:
            out = subprocess.check_output(
                ["nvidia-smi", "--query-gpu=name,temperature.gpu,power.draw", "--format=csv,noheader,nounits"],
                text=True, timeout=2.0
            ).strip()
            parts = [p.strip() for p in out.split(",")]
            if len(parts) >= 3:
                gpu_summary = f"{parts[0]} • Temp: {parts[1]}°C • Power: {parts[2]}W"
        except Exception:
            pass

        return {
            "ram_status": f"{mem_info.get('ram_free_gb', 0)} GB free / {mem_info.get('ram_total_gb', 0)} GB total",
            "swap_status": f"{mem_info.get('swap_free_gb', 0)} GB free / {mem_info.get('swap_total_gb', 0)} GB total",
            "gpu_status": gpu_summary,
        }

    def get_solving_tools_guide(self, cname: str, port: int) -> dict:
        """Returns standard copy-paste troubleshooting and remediation tools for operators."""
        return {
            "Container Log Inspection": f"docker logs --tail 100 {cname}",
            "Inspect Thor MIG Slices": "nvidia-smi -L && nvidia-smi mig -lgi",
            "Re-partition MIG Driver": "sudo systemctl restart nvidia-mig",
            "Inspect Host Kernel OOMs": 'dmesg -T | grep -i -E "oom|killed process"',
            "Restart RapidAlert Stack": "./run.sh",
            "Manual Container Bounce": f"docker restart {cname}",
        }

    def record_unresponsive_tick(self, name: str) -> int:
        """Increments consecutive unresponsive tick count for a container."""
        cnt = self._consecutive_unresponsive.get(name, 0) + 1
        self._consecutive_unresponsive[name] = cnt
        return cnt

    def clear_unresponsive_tick(self, name: str) -> None:
        """Resets unresponsive tick count once container is healthy."""
        self._consecutive_unresponsive[name] = 0

    def get_stats(self) -> dict:
        """Returns aggregated telemetry for the dashboard and watchdog status APIs."""
        return {
            "monitored_containers": list(self._restart_history.keys()),
            "total_restarts": {c: len(hist) for c, hist in self._restart_history.items()},
            "last_restarts": {
                c: time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
                for c, ts in self._last_restart_ts.items()
            },
        }


# Global instance
container_manager = ContainerManager()
