"""
Unit and integration tests for ContainerManager, SystemWatchdog container crash auditing,
and WatchdogEmailer container alert generation.
"""
import asyncio
import json
import os
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from backend.services.container_manager import ContainerManager
from backend.services.watchdog import SystemWatchdog
from backend.services.watchdog_emailer import WatchdogEmailer


class TestContainerManager(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.mgr = ContainerManager()
        # Reset internal state
        self.mgr._restart_history.clear()
        self.mgr._last_restart_ts.clear()
        self.mgr._consecutive_unresponsive.clear()

    def test_can_restart_throttling_and_backoff(self):
        cname = "test_vllm_container"
        # 1. First restart allowed
        can_res, reason = self.mgr.can_restart(cname)
        self.assertTrue(can_res)
        self.assertEqual(reason, "OK")

        # Record restart
        t0 = time.monotonic()
        self.mgr._restart_history[cname].append(t0)
        self.mgr._last_restart_ts[cname] = t0

        # 2. Immediate second restart should be throttled (< 30s)
        can_res, reason = self.mgr.can_restart(cname)
        self.assertFalse(can_res)
        self.assertIn("Throttled", reason)

        # Fast forward time past 30s
        self.mgr._last_restart_ts[cname] = t0 - 35.0
        can_res, reason = self.mgr.can_restart(cname)
        self.assertTrue(can_res)

        # 3. Simulate 3 restarts in window -> trigger backoff
        self.mgr._restart_history[cname].append(t0 - 20.0)
        self.mgr._restart_history[cname].append(t0 - 10.0)
        can_res, reason = self.mgr.can_restart(cname)
        self.assertFalse(can_res)
        self.assertIn("Crash-loop backoff active", reason)

    def test_solving_tools_guide(self):
        guide = self.mgr.get_solving_tools_guide("rapidalert_vllm_0", 8000)
        self.assertIn("Container Log Inspection", guide)
        self.assertIn("docker logs --tail 100 rapidalert_vllm_0", guide["Container Log Inspection"])
        self.assertIn("Inspect Thor MIG Slices", guide)
        self.assertIn("nvidia-smi -L", guide["Inspect Thor MIG Slices"])
        self.assertIn("Re-partition MIG Driver", guide)

    @patch("asyncio.create_subprocess_exec")
    async def test_inspect_container_exited(self, mock_exec):
        fake_state = json.dumps({
            "Running": False,
            "Status": "exited",
            "ExitCode": 137,
            "OOMKilled": True,
            "Dead": False,
            "StartedAt": "2026-10-15T00:00:00Z",
            "FinishedAt": "2026-10-15T01:00:00Z",
            "Pid": 0,
        }).encode("utf-8")

        mock_proc = AsyncMock()
        mock_proc.returncode = 0
        mock_proc.communicate.return_value = (fake_state, b"")
        mock_exec.return_value = mock_proc

        cinfo = await self.mgr.inspect_container("rapidalert_vllm_0")
        self.assertTrue(cinfo["exists"])
        self.assertFalse(cinfo["running"])
        self.assertEqual(cinfo["status"], "exited")
        self.assertEqual(cinfo["exit_code"], 137)
        self.assertTrue(cinfo["oom_killed"])

    @patch("asyncio.create_subprocess_exec")
    async def test_restart_container_success(self, mock_exec):
        mock_proc = AsyncMock()
        mock_proc.returncode = 0
        mock_proc.communicate.return_value = (b"", b"")
        mock_exec.return_value = mock_proc

        ok, msg = await self.mgr.restart_container("rapidalert_vllm_0")
        self.assertTrue(ok)
        self.assertIn("restarted successfully", msg)
        self.assertEqual(len(self.mgr._restart_history["rapidalert_vllm_0"]), 1)


class TestWatchdogContainerSelfHealing(unittest.IsolatedAsyncioTestCase):
    async def test_audit_and_heal_crashed_container(self):
        # Mock dependencies
        camera_manager = MagicMock()
        camera_manager.get_active_cameras.return_value = []
        camera_manager.get_config.return_value = {}

        frame_store = MagicMock()
        ws_manager = MagicMock()
        storage = MagicMock()

        vlm_pool = MagicMock()
        vlm_pool.get_stats.return_value = [
            {"url": "http://localhost:8000", "healthy": True},
            {"url": "http://localhost:8001", "healthy": False},
        ]

        emailer = MagicMock()
        emailer.send_container_crash_alert.return_value = True

        container_mgr = MagicMock()
        container_mgr._get_sys_config.return_value = {
            "vllm_endpoints": [
                {"url": "http://localhost:8000", "container_name": "rapidalert_vllm_0", "mig_uuid": "MIG-0", "sm_count": 12},
                {"url": "http://localhost:8001", "container_name": "rapidalert_vllm_1", "mig_uuid": "MIG-1", "sm_count": 6},
            ]
        }
        # rapidalert_vllm_0 is running fine
        # rapidalert_vllm_1 has crashed with OOM
        async def mock_inspect(name):
            if name == "rapidalert_vllm_0":
                return {"exists": True, "running": True, "status": "running", "oom_killed": False, "exit_code": 0}
            else:
                return {"exists": True, "running": False, "status": "exited", "oom_killed": True, "exit_code": 137}

        container_mgr.inspect_container = AsyncMock(side_effect=mock_inspect)
        container_mgr.can_restart.return_value = (True, "OK")
        container_mgr.restart_container = AsyncMock(return_value=(True, "Restarted OK"))
        container_mgr.get_container_logs = AsyncMock(return_value="CUDA out of memory error in vLLM core")
        container_mgr.get_hardware_telemetry.return_value = {"ram_status": "30 GB free", "swap_status": "0 GB", "gpu_status": "Thor 58C"}
        container_mgr.get_solving_tools_guide.return_value = {"Restart": "./run.sh"}
        container_mgr.warmup_grace_seconds = 180.0
        container_mgr.get_stats.return_value = {"total_restarts": {"rapidalert_vllm_1": 1}}

        watchdog = SystemWatchdog(
            camera_manager=camera_manager,
            frame_store=frame_store,
            vlm_pool=vlm_pool,
            ws_manager=ws_manager,
            storage=storage,
            watchdog_emailer=emailer,
            container_mgr=container_mgr,
        )

        await watchdog._audit_and_heal_containers()

        # Check that restart was called for rapidalert_vllm_1
        container_mgr.restart_container.assert_awaited_once_with("rapidalert_vllm_1")
        # Check that container crash email was dispatched
        emailer.send_container_crash_alert.assert_called_once()
        call_kwargs = emailer.send_container_crash_alert.call_args[1]
        self.assertEqual(call_kwargs["container_name"], "rapidalert_vllm_1")
        self.assertEqual(call_kwargs["port"], 8001)
        self.assertIn("OOM Killed", call_kwargs["crash_reason"])
        self.assertEqual(call_kwargs["container_logs"], "CUDA out of memory error in vLLM core")
        self.assertIn("Restart", call_kwargs["solving_tools"])

    async def test_audit_resolves_when_container_recovers(self):
        camera_manager = MagicMock()
        frame_store = MagicMock()
        ws_manager = MagicMock()
        storage = MagicMock()

        vlm_pool = MagicMock()
        # Both healthy now
        vlm_pool.get_stats.return_value = [
            {"url": "http://localhost:8000", "healthy": True},
            {"url": "http://localhost:8001", "healthy": True},
        ]

        emailer = MagicMock()
        container_mgr = MagicMock()
        container_mgr._get_sys_config.return_value = {
            "vllm_endpoints": [
                {"url": "http://localhost:8001", "container_name": "rapidalert_vllm_1", "mig_uuid": "MIG-1", "sm_count": 6},
            ]
        }
        container_mgr.inspect_container = AsyncMock(return_value={
            "exists": True, "running": True, "status": "running", "oom_killed": False, "exit_code": 0
        })
        container_mgr.get_stats.return_value = {}

        watchdog = SystemWatchdog(
            camera_manager=camera_manager,
            frame_store=frame_store,
            vlm_pool=vlm_pool,
            ws_manager=ws_manager,
            storage=storage,
            watchdog_emailer=emailer,
            container_mgr=container_mgr,
        )
        # Mark as previously crashed
        watchdog._container_crashes_active["rapidalert_vllm_1"] = True

        await watchdog._audit_and_heal_containers()

        # Should be resolved
        self.assertFalse(watchdog._container_crashes_active["rapidalert_vllm_1"])
        emailer.send_resolution_alert.assert_called_once()
        self.assertIn("rapidalert_vllm_1", emailer.send_resolution_alert.call_args[1]["title"])

    async def test_watchdog_never_kills_running_or_inspect_timeout_container(self):
        """Validates that running containers and inspect_timeouts are never killed or restarted by watchdog."""
        camera_manager = MagicMock()
        frame_store = MagicMock()
        ws_manager = MagicMock()
        storage = MagicMock()
        vlm_pool = MagicMock()
        # Shard is reported temporarily unhealthy or busy
        vlm_pool.get_stats.return_value = [
            {"url": "http://localhost:8000", "healthy": False},
        ]
        emailer = MagicMock()
        container_mgr = MagicMock()
        container_mgr._get_sys_config.return_value = {
            "vllm_endpoints": [
                {"url": "http://localhost:8000", "container_name": "rapidalert_vllm_0", "mig_uuid": "MIG-0", "sm_count": 12},
            ]
        }
        # Docker says container is running, or inspect_timeout occurred
        container_mgr.inspect_container = AsyncMock(return_value={
            "exists": True, "running": True, "status": "inspect_timeout", "oom_killed": False, "exit_code": 0, "uptime_sec": 3600.0
        })
        container_mgr.restart_container = AsyncMock()

        watchdog = SystemWatchdog(
            camera_manager=camera_manager,
            frame_store=frame_store,
            vlm_pool=vlm_pool,
            ws_manager=ws_manager,
            storage=storage,
            watchdog_emailer=emailer,
            container_mgr=container_mgr,
        )

        await watchdog._audit_and_heal_containers()

        # Under no circumstance should restart_container be called on running/timeout container!
        container_mgr.restart_container.assert_not_called()

    async def test_staggered_6_day_maintenance_cycle(self):
        """Validates 6-day maintenance recycle, single-container-at-a-time, and 1-hour stagger lock."""
        camera_manager = MagicMock()
        frame_store = MagicMock()
        ws_manager = MagicMock()
        storage = MagicMock()
        vlm_pool = MagicMock()
        # Both shards currently healthy
        vlm_pool.get_stats.return_value = [
            {"url": "http://localhost:8000", "healthy": True},
            {"url": "http://localhost:8001", "healthy": True},
        ]
        emailer = MagicMock()
        container_mgr = MagicMock()
        container_mgr._get_sys_config.return_value = {
            "maintenance_restart_enabled": True,
            "maintenance_restart_interval_days": 6,
            "maintenance_stagger_delay_sec": 3600,
            "vllm_endpoints": [
                {"url": "http://localhost:8000", "container_name": "rapidalert_vllm_0", "mig_uuid": "MIG-0", "sm_count": 12},
                {"url": "http://localhost:8001", "container_name": "rapidalert_vllm_1", "mig_uuid": "MIG-1", "sm_count": 6},
            ]
        }
        # Both have 6.1 days of uptime (6.1 * 86400 = 527040s)
        async def mock_inspect(name):
            return {
                "exists": True, "running": True, "status": "running", "oom_killed": False,
                "exit_code": 0, "uptime_sec": 527040.0, "started_at": "2026-10-10T00:00:00Z"
            }

        container_mgr.inspect_container = AsyncMock(side_effect=mock_inspect)
        container_mgr.restart_container = AsyncMock(return_value=(True, "Restarted OK"))

        watchdog = SystemWatchdog(
            camera_manager=camera_manager,
            frame_store=frame_store,
            vlm_pool=vlm_pool,
            ws_manager=ws_manager,
            storage=storage,
            watchdog_emailer=emailer,
            container_mgr=container_mgr,
        )

        # 1. First cycle: Only ONE container (rapidalert_vllm_0) should be restarted for maintenance
        await watchdog._audit_and_heal_containers()
        container_mgr.restart_container.assert_awaited_once_with("rapidalert_vllm_0", timeout_sec=15)
        self.assertEqual(watchdog._maintenance_in_progress, "rapidalert_vllm_0")
        emailer.send_critical_alert.assert_called_once()
        self.assertIn("MAINTENANCE_rapidalert_vllm_0", emailer.send_critical_alert.call_args[1]["event_type"])

        # 2. Second cycle (container 0 recovers and becomes healthy)
        container_mgr.restart_container.reset_mock()
        emailer.send_critical_alert.reset_mock()
        await watchdog._audit_and_heal_containers()

        # Maintenance completed for 0; stagger lock engaged
        self.assertIsNone(watchdog._maintenance_in_progress)
        self.assertIn("rapidalert_vllm_0", watchdog._maintenance_healthy_since)

        # 3. Third cycle (simulating 10 minutes later: container 1 is still locked because < 1 hour has elapsed)
        container_mgr.restart_container.reset_mock()
        await watchdog._audit_and_heal_containers()
        # Container 1 should NOT be restarted yet
        container_mgr.restart_container.assert_not_called()

        # 4. Fourth cycle (fast forward 3605s past the 1-hour stagger delay)
        watchdog._maintenance_healthy_since["rapidalert_vllm_0"] -= 3605.0
        await watchdog._audit_and_heal_containers()
        # Now container 1 can safely undergo maintenance!
        container_mgr.restart_container.assert_awaited_once_with("rapidalert_vllm_1", timeout_sec=15)
        self.assertEqual(watchdog._maintenance_in_progress, "rapidalert_vllm_1")


if __name__ == "__main__":
    unittest.main()
