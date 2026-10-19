"""Queueing/priority behaviour of DeadlineScheduler: trigger reserve + no dropped triggers."""
import asyncio
import time
from unittest.mock import MagicMock

from backend.services.priority_queue import PriorityQueueAdapter
from backend.services.scheduler import DeadlineScheduler


def _make_scheduler(capacity: int = 4, workers: int = 4) -> DeadlineScheduler:
    pool = MagicMock()
    pool.healthy_capacity.return_value = capacity
    sched = DeadlineScheduler(
        camera_manager=MagicMock(), frame_store=MagicMock(), vlm_pool=pool,
        prompt_manager=MagicMock(), result_store=MagicMock(), alert_engine=MagicMock(),
        max_concurrency=workers,
    )
    sched._running = True
    sched._workers = [object()] * workers  # only len() is used by the slot limit
    return sched


async def _start_queue(sched: DeadlineScheduler) -> None:
    sched._queue = PriorityQueueAdapter(redis_url=None)
    await sched._queue.start()


def test_heartbeat_limit_reserves_a_slot_against_healthy_capacity():
    assert _make_scheduler(capacity=4, workers=4)._heartbeat_slot_limit() == 3
    assert _make_scheduler(capacity=2, workers=4)._heartbeat_slot_limit() == 1  # one shard down
    assert _make_scheduler(capacity=1, workers=4)._heartbeat_slot_limit() == 1  # never starve fully
    assert _make_scheduler(capacity=0, workers=4)._heartbeat_slot_limit() == 3  # warmup: fall back to workers


def test_trigger_supersedes_queued_heartbeat_instead_of_being_dropped():
    async def run():
        s = _make_scheduler()
        await _start_queue(s)
        hb = {"cam": "A", "is_heartbeat": True, "tier": 4, "jid": s._next_seq()}
        s._hb["A"] = hb
        s._refresh_in_flight("A")
        await s._queue.put((2, hb["jid"], hb))

        await s.queue_incident({"cam": "A", "tier": 2, "trigger_time": 100.0})

        assert hb["jid"] in s._superseded and "A" not in s._hb
        assert s._inc["A"]["priority"] == 0
        # the next job a worker pops is the trigger (priority 0), not the heartbeat
        prio, _, job = await s._queue.get()
        assert prio == 0 and job["cam"] == "A" and not job.get("is_heartbeat")
    asyncio.run(run())


def test_queued_trigger_superseded_by_fresher_keeps_earliest_time_and_strongest_tier():
    async def run():
        s = _make_scheduler()
        await _start_queue(s)
        await s.queue_incident({"cam": "A", "tier": 3, "trigger_time": 100.0, "frames_b64": ["old"]})
        old_jid = s._inc["A"]["jid"]
        await s.queue_incident({"cam": "A", "tier": 2, "trigger_time": 105.0, "frames_b64": ["new"]})

        cur = s._inc["A"]
        assert old_jid in s._superseded
        assert cur["frames_b64"] == ["new"]
        assert cur["trigger_time"] == 100.0 and cur["tier"] == 2 and cur["priority"] == 0
    asyncio.run(run())


def test_trigger_during_running_incident_is_coalesced_then_promoted():
    async def run():
        s = _make_scheduler()
        await _start_queue(s)
        await s.queue_incident({"cam": "A", "tier": 2, "trigger_time": 100.0})
        running = s._inc["A"]
        s._running_jids.add(running["jid"])  # a worker picked it up

        await s.queue_incident({"cam": "A", "tier": 3, "trigger_time": 110.0, "frames_b64": ["x"]})
        await s.queue_incident({"cam": "A", "tier": 2, "trigger_time": 120.0, "frames_b64": ["y"]})
        assert s._queue.qsize() == 1                       # nothing extra queued while running
        assert s._pending_inc["A"]["trigger_time"] == 110.0  # earliest pending kept
        assert s._pending_inc["A"]["tier"] == 2

        await s._on_job_done(running)
        assert "A" not in s._pending_inc
        assert s._inc["A"]["frames_b64"] == ["y"] and s._inc["A"]["jid"] != running["jid"]
        assert s._queue.qsize() == 2                       # promoted trigger is now queued
    asyncio.run(run())


def test_trigger_while_heartbeat_running_runs_concurrently():
    async def run():
        s = _make_scheduler()
        await _start_queue(s)
        hb = {"cam": "A", "is_heartbeat": True, "tier": 4, "jid": s._next_seq()}
        s._hb["A"] = hb
        s._running_jids.add(hb["jid"])
        await s.queue_incident({"cam": "A", "tier": 2, "trigger_time": time.monotonic()})
        assert hb["jid"] not in s._superseded and "A" in s._hb  # running heartbeat untouched
        assert s._queue.qsize() == 1 and "A" in s._inc
    asyncio.run(run())


def test_worker_skips_superseded_jobs_and_clears_tracking():
    async def run():
        s = _make_scheduler()
        await _start_queue(s)
        ran = []

        async def fake_analyze(job):
            ran.append(job["jid"])
        s._analyze_job = fake_analyze

        await s.queue_incident({"cam": "A", "tier": 3, "trigger_time": 1.0})
        stale = s._inc["A"]["jid"]
        await s.queue_incident({"cam": "A", "tier": 2, "trigger_time": 2.0})
        fresh = s._inc["A"]["jid"]

        w = asyncio.create_task(s._worker(0))
        for _ in range(50):
            await asyncio.sleep(0.02)
            if "A" not in s._inc:
                break
        s._running = False
        await w

        assert ran == [fresh] and stale not in ran
        assert "A" not in s._inc and "A" not in s._in_flight and not s._running_jids
    asyncio.run(run())
