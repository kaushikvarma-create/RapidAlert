"""
MIGAwareVLMPool — one asyncio queue + dedicated workers per VLLM/MIG instance.

Architecture:
  - Each vLLM endpoint (one per MIG slice) gets an _EndpointShard:
      • asyncio.Queue(maxsize=queue_depth)   — bounded, prevents runaway backlog
      • asyncio.Semaphore(max_concurrent)    — hard cap on in-flight HTTP requests
      • N worker coroutines = max_concurrent — one per semaphore slot
  - analyze() selects the shard with the lowest weighted load score:
        load_score = (in_flight + queued) / weight
    This gives the 12SM shard (weight=3) 3x the preference over 8SM (weight=2),
    matching the hardware 60/40 compute split.
  - If every shard's queue is full, we block on the one with the lowest load_score.
  - A background health loop probes /health every 30s and gates routing away
    from unhealthy endpoints automatically.
  - get_stats() returns per-shard telemetry; broadcast via scheduler metrics.
"""
from __future__ import annotations

import asyncio
import time
from collections import deque
from typing import Optional

import aiohttp

from backend.core.error_tracker import error_tracker


# ── Response key mapping ───────────────────────────────────────────────────────
_PARSE_KEYS = {
    "observation": "observation",
    "activity":    "activity",
    "workers":     "workers",
    "machinery":   "machinery",
    "safety":      "safety",
    "severity":    "severity",
    "reasoning":   "reasoning",
    "evolution":   "evolution",
    "procedure_checklist": "procedure_checklist",  # legacy — kept for backward compat
    "priority_flags":      "priority_flags",        # new: maps to severe_incidents
    "routine_flags":       "routine_flags",         # new: maps to low_incidents
}

_HEALTH_INTERVAL_SEC = 30.0     # how often the background health loop pings each shard
_RETRY_COUNT         = 3        # per-request retry attempts before returning fallback


# ══════════════════════════════════════════════════════════════════════════════
#  _EndpointShard — one MIG-pinned vLLM instance
# ══════════════════════════════════════════════════════════════════════════════

class _EndpointShard:
    """
    Encapsulates one vLLM endpoint's queue, semaphore, workers, and stats.

    Each shard owns:
      queue      — asyncio.Queue(maxsize=queue_depth) of (job_dict, Future) tuples
      semaphore  — limits concurrent HTTP requests to max_concurrent
      workers    — max_concurrent coroutine tasks draining the queue
    """

    def __init__(
        self,
        url: str,
        model: str,
        weight: int,
        max_concurrent: int,
        queue_depth: int,
        session: aiohttp.ClientSession,
        mig_uuid: str = "",
        mig_profile: str = "",
        sm_count: int = 0,
        gpu_utilization: float = 0.0,
        container_name: str = "",
    ):
        self.url            = url
        self.model          = model
        self.weight         = max(1, weight)
        self.max_concurrent = max(1, max_concurrent)
        self.queue_depth    = max(1, queue_depth)
        self.mig_uuid       = mig_uuid
        self.mig_profile    = mig_profile
        self.sm_count       = sm_count or (12 if ":8000" in url else 8)
        self.gpu_utilization = gpu_utilization or (0.33 if ":8000" in url else 0.33)
        self.container_name = container_name or f"rapidalert_vllm_{0 if ':8000' in url else 1}"
        self._session       = session

        self._queue: asyncio.PriorityQueue = asyncio.PriorityQueue(maxsize=max(queue_depth, 20))
        self._seq:   int                   = 0
        self._sem:   asyncio.Semaphore     = asyncio.Semaphore(max_concurrent)
        self._workers: list[asyncio.Task]  = []
        self.healthy = False  # Start as False until health probe confirms 200 OK
        self._active_jobs: list[dict]      = []

        # Per-shard telemetry counters
        self.stat_queued:    int   = 0      # total items ever enqueued
        self.stat_inflight:  int   = 0      # currently in HTTP flight
        self.stat_completed: int   = 0      # successfully completed
        self.stat_errors:    int   = 0      # all-retries-failed count
        self._latencies: deque[float] = deque(maxlen=100)

    # ── Load score for routing ─────────────────────────────────────────────

    def load_score(self) -> float:
        """
        Weighted load: lower = prefer this shard.
        Accounts for both in-flight and pending queue depth, normalised by weight.
        """
        if not self.healthy:
            return float("inf")
        return (self.stat_inflight + self._queue.qsize()) / self.weight

    # ── Worker lifecycle ───────────────────────────────────────────────────

    def start_workers(self) -> None:
        for i in range(self.max_concurrent):
            t = asyncio.create_task(
                self._worker(i), name=f"vlm-{self.url.split(':')[-1]}-w{i}"
            )
            self._workers.append(t)

    def stop_workers(self) -> None:
        for w in self._workers:
            w.cancel()
        self._workers.clear()

    async def _worker(self, worker_id: int) -> None:
        """Drain the priority queue, acquire semaphore, execute inference, resolve Future."""
        while True:
            try:
                priority, seq, job, fut = await self._queue.get()
            except asyncio.CancelledError:
                break

            # If shard is not healthy yet (e.g. warming up), wait for it or resolve if cancelled
            while not self.healthy:
                if fut.cancelled():
                    break
                await asyncio.sleep(0.5)

            if fut.cancelled():
                self._queue.task_done()
                continue

            cam_name = job.get("cam", "Unknown")
            try:
                async with self._sem:
                    self.stat_inflight += 1
                    job_record = {"worker_id": worker_id, "cam": cam_name, "t_start": time.monotonic()}
                    self._active_jobs.append(job_record)
                    try:
                        result = await self._infer(job)
                        if not fut.done():
                            fut.set_result(result)
                        self.stat_completed += 1
                    except Exception as exc:
                        self.stat_errors += 1
                        if not fut.done():
                            fut.set_exception(exc)
                        error_tracker.capture_exception(
                            exc,
                            component="VLMClient",
                            camera=cam_name,
                            effect=f"Worker {worker_id} on {self.url} unhandled exception; future resolved with exception",
                            severity="ERROR",
                            )
                    finally:
                        self._active_jobs = [j for j in self._active_jobs if j.get("worker_id") != worker_id]
            except asyncio.CancelledError:
                if not fut.done():
                    fut.cancel()
                break
            finally:
                if self.stat_inflight > 0:
                    self.stat_inflight -= 1
                self._queue.task_done()

    # ── Enqueue ────────────────────────────────────────────────────────────

    def enqueue_nowait(self, job: dict, fut: asyncio.Future, priority: int = 1) -> bool:
        """
        Non-blocking priority enqueue. Returns True if accepted, False if queue full.
        Priority: lower number = higher precedence (-1: compliance/top-priority, 0: incident, 1: minor, 2: heartbeat).
        """
        self._seq += 1
        try:
            self._queue.put_nowait((priority, self._seq, job, fut))
            self.stat_queued += 1
            return True
        except asyncio.QueueFull:
            return False

    async def enqueue_wait(self, job: dict, fut: asyncio.Future, priority: int = 1) -> None:
        """Blocking priority enqueue — used as last-resort when all shards are full."""
        self._seq += 1
        await self._queue.put((priority, self._seq, job, fut))
        self.stat_queued += 1

    # ── Inference (with retries) ───────────────────────────────────────────

    async def _infer(self, job: dict) -> dict:
        cam_name   = job.get("cam", "compliance_check")
        frames_b64 = job.get("frames_b64") or []
        prompt     = job.get("prompt", "")
        labels     = job.get("labels") or []
        url        = f"{self.url}/v1/chat/completions"

        is_text_job = (job.get("type") == "text") or (not frames_b64)

        if is_text_job:
            sys_msg = job.get("system_msg") or "You are an AI assistant."
            payload = {
                "model":       self.model,
                "messages":    [
                    {"role": "system", "content": sys_msg},
                    {"role": "user", "content": prompt}
                ],
                "max_tokens":  job.get("max_tokens", 512),
                "temperature": job.get("temperature", 0.2),
                "chat_template_kwargs": {"enable_thinking": False},
            }
        else:
            content = []
            frames = frames_b64 if isinstance(frames_b64, list) else [frames_b64]
            if len(frames) > 1:
                content.append({
                    "type": "text",
                    "text": (
                        f"CHRONOLOGICAL CCTV FRAME SEQUENCE ({len(frames)} frames ordered from earliest past baseline at Frame 1 "
                        f"progressing chronologically to the latest current moment at Frame {len(frames)}):"
                    ),
                })
                # Interleave explicit frame label markers to anchor multi-frame vision attention
                for idx, b64 in enumerate(frames):
                    raw_label = labels[idx] if idx < len(labels) else f"t -{(len(frames) - 1 - idx) * 3:.1f}s"
                    if raw_label.lower().startswith("frame"):
                        label_txt = raw_label
                    else:
                        label_txt = f"Frame {idx + 1} of {len(frames)} [{raw_label}]"
                    content.append({"type": "text", "text": f"{label_txt}:"})
                    content.append({
                        "type": "image_url",
                        "image_url": {"url": f"data:image/jpeg;base64,{b64}"},
                    })
            elif frames:
                content.append({
                    "type": "image_url",
                    "image_url": {"url": f"data:image/jpeg;base64,{frames[0]}"},
                })
            content.append({"type": "text", "text": prompt})

            sys_msg = (
                "You are an objective, conservative CCTV surveillance AI. "
                "CRITICAL: The checklist is NOT a directive to hunt for violations. Assume the scene is normal unless there is blatant visual proof. "
                "Flag a checklist item or positive threat ONLY IF YOU ARE HIGHLY CONFIDENT based on unmistakable, clearly visible evidence. "
                "If there is any doubt, plausible innocent explanation, or low confidence, you MUST NOT flag it (return [] for priority_flags and safety OK / severity LOW)."
            )

            payload = {
                "model":       self.model,
                "messages":    [
                    {"role": "system", "content": sys_msg},
                    {"role": "user", "content": content}
                ],
                "max_tokens":  256,
                "temperature": 0.5,
                "repetition_penalty": 1.15,
                "chat_template_kwargs": {"enable_thinking": False},
            }

        t0 = time.monotonic()
        for attempt in range(_RETRY_COUNT):
            try:
                async with self._session.post(url, json=payload) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        error_tracker.capture_error(
                            message=f"HTTP {resp.status} from {self.url}: {body[:200]}",
                            component="VLMClient",
                            camera=cam_name,
                            effect=f"vLLM call returned non-200 (attempt {attempt+1}/{_RETRY_COUNT}); retrying",
                            severity="WARNING",
                        )
                        await asyncio.sleep(1)
                        continue
                    data = await resp.json()
                    raw = data["choices"][0]["message"]["content"]
                    usage = data.get("usage") or {}
                    prompt_tokens = usage.get("prompt_tokens", 0)
                    completion_tokens = usage.get("completion_tokens", 0)
                    total_tokens = usage.get("total_tokens", prompt_tokens + completion_tokens)

                    if is_text_job:
                        lat = time.monotonic() - t0
                        self._latencies.append(lat)
                        return {
                            "text": raw,
                            "raw": raw,
                            "latency": lat,
                            "prompt_tokens": prompt_tokens,
                            "completion_tokens": completion_tokens,
                            "total_tokens": total_tokens,
                            "tokens": total_tokens,
                        }
                    result = _parse_response(raw, cam_name)
                    result["latency"] = time.monotonic() - t0
                    result["prompt_tokens"] = prompt_tokens
                    result["completion_tokens"] = completion_tokens
                    result["total_tokens"] = total_tokens
                    result["tokens"] = total_tokens
                    self._latencies.append(result["latency"])
                    return result

            except (aiohttp.ServerDisconnectedError, aiohttp.ClientConnectionError) as exc:
                self.healthy = False
                if attempt == _RETRY_COUNT - 1:
                    error_tracker.capture_exception(
                        exc, component="VLMClient", camera=cam_name,
                        effect=f"Connection failure on {self.url} after {_RETRY_COUNT} attempts; routing suspended",
                        severity="WARNING",
                    )
                await asyncio.sleep(0.1)

            except asyncio.TimeoutError as exc:
                if attempt == _RETRY_COUNT - 1:
                    error_tracker.capture_exception(
                        exc, component="VLMClient", camera=cam_name,
                        effect=f"Timeout on {self.url} after {_RETRY_COUNT} attempts",
                        severity="WARNING",
                    )
                await asyncio.sleep(0.5)

            except Exception as exc:
                if attempt == _RETRY_COUNT - 1:
                    error_tracker.capture_exception(
                        exc, component="VLMClient", camera=cam_name,
                        effect=f"Request error on {self.url} (attempt {attempt+1}/{_RETRY_COUNT}); retrying",
                        severity="WARNING",
                    )
                await asyncio.sleep(0.5)

        # All retries exhausted
        error_tracker.capture_error(
            message=f"All {_RETRY_COUNT} inference attempts failed on {self.url}",
            component="VLMClient",
            camera=cam_name,
            effect=f"Inference exhausted all retries for {cam_name} on {self.url}; returning fallback result",
            severity="WARNING",
        )
        if is_text_job:
            return {"text": "", "raw": "", "latency": time.monotonic() - t0, "error": "Inference failed"}
        return _fallback_result(cam_name, time.monotonic() - t0)

    # ── Stats ──────────────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        lats = list(self._latencies)
        port = self.url.split(":")[-1] if ":" in self.url else self.url
        return {
            "url":              self.url,
            "port":             port,
            "container_name":   self.container_name,
            "model":            self.model,
            "weight":           self.weight,
            "sm_count":         self.sm_count,
            "gpu_utilization":  self.gpu_utilization,
            "max_concurrent":   self.max_concurrent,
            "healthy":          self.healthy,
            "queued":           self._queue.qsize(),
            "inflight":         self.stat_inflight,
            "in_flight":        self.stat_inflight,
            "completed":        self.stat_completed,
            "errors":           self.stat_errors,
            "mig_uuid":         self.mig_uuid,
            "mig_profile":      self.mig_profile,
            "is_mig":           bool(self.mig_uuid),
            "active_jobs":      [{"cam": j.get("cam", ""), "elapsed_s": round(time.monotonic() - j.get("t_start", time.monotonic()), 1)} for j in self._active_jobs],
            "avg_latency_ms":   round(sum(lats) / len(lats) * 1000, 1) if lats else None,
            "p95_latency_ms":   round(sorted(lats)[int(len(lats) * 0.95)] * 1000, 1) if len(lats) >= 5 else None,
            "load_score":       round(self.load_score(), 3) if self.healthy else 999.0,
        }


# ══════════════════════════════════════════════════════════════════════════════
#  MIGAwareVLMPool — public interface (drop-in for VLMPool)
# ══════════════════════════════════════════════════════════════════════════════

class MIGAwareVLMPool:
    """
    Manages N _EndpointShards (one per MIG/vLLM instance).

    Routing:
      analyze()  — weighted-load: routes to shard with min (inflight+queued)/weight.
                   On full queue, overflows to next best shard; if all full, blocks
                   on the shard with lowest load_score.

    Backwards compatibility:
      analyze()           — same signature as old VLMPool.analyze()
      analyze_concurrent()— fires to ALL shards simultaneously (comparator mode)
      endpoints           — list of endpoint dicts (for scheduler print)
    """

    def __init__(self, endpoints: list[dict]):
        if not endpoints:
            raise ValueError("MIGAwareVLMPool requires at least one endpoint")
        self._endpoint_configs = endpoints
        self._shards: list[_EndpointShard] = []
        self._session: Optional[aiohttp.ClientSession] = None
        self._health_task: Optional[asyncio.Task] = None

    # ── Lifecycle ─────────────────────────────────────────────────────────

    async def start(self) -> None:
        connector = aiohttp.TCPConnector(
            limit=128,
            limit_per_host=64,
            keepalive_timeout=15.0,
            enable_cleanup_closed=True,
        )
        timeout = aiohttp.ClientTimeout(total=90, connect=5)
        self._session = aiohttp.ClientSession(connector=connector, timeout=timeout)

        for cfg in self._endpoint_configs:
            shard = _EndpointShard(
                url            = cfg["url"],
                model          = cfg.get("model", ""),
                weight         = cfg.get("weight", 1),
                max_concurrent = cfg.get("max_concurrent", 2),
                queue_depth    = cfg.get("queue_depth", 8),
                mig_uuid       = cfg.get("mig_uuid", ""),
                mig_profile    = cfg.get("mig_profile", ""),
                sm_count       = cfg.get("sm_count", 0),
                gpu_utilization = cfg.get("gpu_utilization", 0.0),
                container_name = cfg.get("container_name", ""),
                session        = self._session,
            )
            self._shards.append(shard)

        # Initial fast health probe
        for shard in self._shards:
            try:
                async with self._session.get(
                    f"{shard.url}/health",
                    timeout=aiohttp.ClientTimeout(total=2)
                ) as r:
                    shard.healthy = (r.status == 200)
                    if shard.healthy:
                        print(f"[VLMPool] ✅ Shard {shard.url} is online and ready")
            except Exception:
                shard.healthy = False

        # Auto-detect models from online endpoints
        for shard in self._shards:
            if shard.healthy:
                try:
                    async with self._session.get(
                        f"{shard.url}/v1/models",
                        timeout=aiohttp.ClientTimeout(total=3)
                    ) as r:
                        if r.status == 200:
                            data   = await r.json()
                            models = data.get("data", [])
                            if models:
                                actual = models[0].get("id")
                                if actual:
                                    shard.model = actual
                                    print(f"[VLMPool] Auto-detected model '{actual}' on {shard.url}")
                except Exception:
                    pass

        # Start per-shard worker coroutines
        for shard in self._shards:
            shard.start_workers()

        # Start background health loop
        self._health_task = asyncio.create_task(
            self._health_loop(), name="vlm-health"
        )

        is_mig = any(bool(s.mig_uuid) for s in self._shards)
        pool_type = "MIG" if is_mig else "Shared"
        print(f"[VLMPool] 🚀 Started — {len(self._shards)} {pool_type} shard(s) registered (warmup monitored)")
        for s in self._shards:
            print(
                f"  → {s.url}  model={s.model}  "
                f"weight={s.weight}  max_concurrent={s.max_concurrent}  "
                f"queue_depth={s.queue_depth}  healthy={s.healthy}"
            )

    async def stop(self) -> None:
        if self._health_task:
            self._health_task.cancel()
        for shard in self._shards:
            shard.stop_workers()
        if self._session:
            await self._session.close()

    # ── Public API ────────────────────────────────────────────────────────

    def has_healthy_shards(self) -> bool:
        """Returns True if at least one shard endpoint is healthy and ready."""
        return any(s.healthy for s in self._shards)

    @property
    def endpoints(self) -> list[dict]:
        """Backwards-compatible property (used by scheduler for logging)."""
        return [{"url": s.url, "model": s.model} for s in self._shards]

    async def health_check(self) -> dict[str, bool]:
        """Manual health check (all shards); updates healthy flags."""
        results = {}
        for shard in self._shards:
            target_url = shard.url.replace("localhost", "127.0.0.1")
            try:
                async with self._session.get(
                    f"{target_url}/health",
                    timeout=aiohttp.ClientTimeout(total=3)
                ) as r:
                    shard.healthy = (r.status == 200)
            except Exception:
                shard.healthy = False
            results[shard.url] = shard.healthy
        return results

    async def probe(self) -> list[dict]:
        """Manual ping, health check and latency probe across all shards."""
        results = []
        for shard in self._shards:
            target_url = shard.url.replace("localhost", "127.0.0.1").rstrip("/")
            t0 = time.monotonic()
            healthy = False
            try:
                async with self._session.get(
                    f"{target_url}/health",
                    timeout=aiohttp.ClientTimeout(total=2.5)
                ) as r:
                    healthy = (r.status == 200)
            except Exception:
                healthy = False
            ping_ms = round((time.monotonic() - t0) * 1000, 1)
            shard.healthy = healthy
            results.append({
                "url": shard.url,
                "port": shard.url.split(":")[-1],
                "healthy": healthy,
                "ping_ms": ping_ms,
                "model": shard.model,
            })
        return results

    def get_stats(self) -> list[dict]:
        """Per-shard telemetry — included in metrics broadcasts."""
        return [s.get_stats() for s in self._shards]

    def is_mig(self) -> bool:
        """Returns True if any shard is configured with a dedicated MIG UUID."""
        return any(bool(s.mig_uuid) for s in self._shards)

    async def analyze(
        self,
        cam_name: str,
        frame_b64: "str | list[str]",
        system_prompt: str,
        labels: Optional[list[str]] = None,
        priority: int = 1,
    ) -> dict:
        """
        Route to the MIG/shared shard with the lowest weighted load score,
        enqueue the job with priority, and await the Future result.
        """
        job = {
            "cam": cam_name,
            "frames_b64": frame_b64,
            "prompt": system_prompt,
            "labels": labels,
        }

        # If no shards are healthy yet (e.g. initial model weight warmup)
        if not self.has_healthy_shards():
            # Quick check if one just booted up
            for s in self._shards:
                try:
                    async with self._session.get(f"{s.url}/health", timeout=aiohttp.ClientTimeout(total=1.0)) as r:
                        if r.status == 200:
                            s.healthy = True
                            print(f"[VLMPool] ✅ Shard {s.url} finished warmup — now healthy")
                            break
                except Exception:
                    pass

            if not self.has_healthy_shards():
                # Clean, non-crashing warmup response without polluting error logs
                return {
                    "observation": "VLM Model Initializing (Warmup in progress)...",
                    "activity": "Model Loading",
                    "workers": "None",
                    "machinery": "None",
                    "safety": "OK",
                    "severity": "LOW",
                    "verdict": "WARMUP",
                    "evolution": "Stable",
                    "latency": 0.0,
                    "model": "warmup",
                    "e2e_latency": 0.0,
                }

        fut: asyncio.Future = asyncio.get_event_loop().create_future()

        # Sort healthy shards by ascending load_score (best first)
        healthy_shards = [s for s in self._shards if s.healthy]
        ranked = sorted(healthy_shards if healthy_shards else self._shards, key=lambda s: s.load_score())

        for shard in ranked:
            if shard.enqueue_nowait(job, fut, priority=priority):
                return await fut

        # All queues full — block on the best shard
        best = ranked[0]
        await best.enqueue_wait(job, fut, priority=priority)
        return await fut

    async def analyze_concurrent(
        self,
        cam_name: str,
        frame_b64: "str | list[str]",
        system_prompt: str,
        labels: Optional[list[str]] = None,
        priority: int = 1,
    ) -> list[dict]:
        """Fire request to ALL shards simultaneously (comparator / ensemble mode)."""
        tasks = [
            self.analyze(cam_name, frame_b64, system_prompt, labels=labels, priority=priority)
            for _ in self._shards
        ]
        return await asyncio.gather(*tasks)

    async def query_text(
        self,
        prompt: str,
        system_msg: str = "You are an AI assistant.",
        priority: int = -1,
        max_tokens: int = 512,
        temperature: float = 0.2,
    ) -> str:
        """
        Execute a text query (compliance check) routed through the shard priority queue.
        Top priority (-1) ensures it immediately jumps to the head of the queue,
        firing before any pending camera frames while strictly obeying concurrency limits.
        """
        job = {
            "type": "text",
            "cam": "compliance_check",
            "prompt": prompt,
            "system_msg": system_msg,
            "frames_b64": [],
            "max_tokens": max_tokens,
            "temperature": temperature,
        }

        # Check health if none marked healthy yet
        if not self.has_healthy_shards():
            for s in self._shards:
                try:
                    async with self._session.get(f"{s.url}/health", timeout=aiohttp.ClientTimeout(total=1.0)) as r:
                        if r.status == 200:
                            s.healthy = True
                            break
                except Exception:
                    pass

        healthy_shards = [s for s in self._shards if s.healthy]
        ranked = sorted(healthy_shards if healthy_shards else self._shards, key=lambda s: s.load_score())
        if not ranked:
            return ""

        fut: asyncio.Future = asyncio.get_event_loop().create_future()

        for shard in ranked:
            if shard.enqueue_nowait(job, fut, priority=priority):
                res = await fut
                return res.get("text", "") if isinstance(res, dict) else str(res)

        # All queues full — wait on the best shard with top priority
        best = ranked[0]
        await best.enqueue_wait(job, fut, priority=priority)
        res = await fut
        return res.get("text", "") if isinstance(res, dict) else str(res)

    async def validate_camera_prompts(
        self,
        cam_name: str,
        severe_text: str | list,
        low_text: str | list,
        normal_context: str,
        night_context: str = "",
        night_context_enabled: bool = False,
    ) -> dict:
        """Audit logical consistency of one proposed camera configuration."""
        import json
        import re

        def _str_fmt(value) -> str:
            if isinstance(value, list):
                return "\n".join(str(item).strip() for item in value if str(item).strip())
            return str(value or "").strip()

        def _items(value) -> list[str]:
            text = _str_fmt(value)
            # Camera configs historically allowed comma-separated strings; treat
            # those as separate rules so an old config cannot hide a conflict.
            parts = []
            for line in text.splitlines():
                parts.extend(line.split(","))
            return [item.strip() for item in parts if item.strip()]

        sev_s = _str_fmt(severe_text)
        low_s = _str_fmt(low_text)
        norm_s = _str_fmt(normal_context)
        night_s = _str_fmt(night_context) if night_context_enabled else ""

        sys_msg = (
            "You are a strict CCTV configuration compliance auditor. "
            "Audit only logical contradictions among the proposed camera rules. "
            "Do not reject entries merely because they share generic words."
        )

        prompt = f"""Audit this proposed CCTV camera configuration for "{cam_name}".

Your ONLY task is to identify logical contradictions among three proposed fields.

FIELD MEANINGS:
A. SEVERE THREAT WATCHLIST: conditions that are never acceptable in this monitored area. If visible, they represent a priority incident.
B. ROUTINE WHITELIST: conditions explicitly declared normal and acceptable. If visible by themselves, they must not be treated as a severe incident.
C. ROUTINE BASELINE CONTEXT: descriptive background about the location and ordinary scene. It is context only; it is not a whitelist and cannot override A or B.

POLARITY MODEL:
- Every condition written in A is being declared malicious, unsafe, unacceptable, or priority-worthy by the camera owner, even if the entry does not use the word "malicious".
- Every condition written in B is being declared benign, safe, acceptable, and normal by the camera owner.
- Every condition written in C is being declared ordinary and expected background behavior.
- These meanings are mutually exclusive for the same condition. A condition must not be treated as malicious in A and acceptable or ordinary in B or C.
- If the same proposition appears in more than one field with opposite meanings, that is a blocking logical contradiction.

A condition cannot be both severe and acceptable. If a condition in A is described as okay, normal, acceptable, routine, benign, safe, or permitted in B or C, that is a blocking contradiction.

PROPOSED SEVERE THREAT WATCHLIST:
{sev_s or "(none)"}

PROPOSED ROUTINE WHITELIST:
{low_s or "(none)"}

PROPOSED ROUTINE BASELINE:
{norm_s or "(none)"}

PROPOSED NIGHT BASELINE:
{night_s or "(none)"}

LOGICAL AUDIT RULES:

1. Evaluate each complete entry as a proposition with a meaning and polarity.
   Report a blocking contradiction when the same proposition is classified as both severe/malicious and routine/benign.
   The contradiction is about opposite classifications, not just matching words.

2. Report a blocking contradiction when a severe condition is described as okay, normal, acceptable, routine, benign, safe, or permitted anywhere in the routine whitelist or baseline. For example:
   - Severe: "physical fighting"
   - Routine: "Fighting is ok"
   This MUST be invalid.
   The words "is okay", "is ok", "normal", "acceptable", "permitted", "routine", "benign", and "safe" reverse the meaning and must not be ignored.
   Do not require the severe entry to repeat the word "malicious": its presence in the severe field already gives it the malicious/unacceptable meaning.

3. Treat equivalent wording as the same condition:
   - "person collapsed on the floor"
   - "collapsed person is okay"
   These conflict because the same condition is both severe and routine.

3. Read complete entries, including qualifiers such as "not", "normal", "okay", "forced", "climbing", "sleeping", and "collapsed".

5. Do not flag a contradiction merely because entries share a generic word. These are distinct:
   - "walking through a turnstile" vs "climbing over a turnstile"
   - "carrying a parcel" vs "using force to enter"
   - "sitting at a desk" vs "sleeping at a desk"
   - "standing near a door" vs "kicking the door"

6. A baseline contradiction exists when the baseline explicitly describes a severe condition as normal, okay, acceptable, permitted, or routine.

7. Empty fields are valid. Do not rewrite or judge the policy.

8. Set valid=false for any real contradiction. Set valid=true only when no severe condition is normalized by the other fields.

Return ONLY valid JSON:
{{
  "valid": true,
  "has_contradictions": false,
  "summary": "One short sentence explaining the result.",
  "conflicts": [
    {{
      "type": "severe_vs_routine | severe_vs_baseline | contradiction",
      "fields": ["severe_incidents", "low_incidents"],
      "severe_item": "Exact severe entry",
      "related_item": "Exact routine or baseline entry",
      "reason": "Why the entries logically conflict.",
      "suggestion": "Specific correction.",
      "blocking": true
    }}
  ]
}}"""
        raw_resp = await self.query_text(prompt, system_msg=sys_msg)

        if raw_resp:
            try:
                clean = raw_resp.strip()
                if clean.startswith("```"):
                    clean = re.sub(r"^```(?:json)?\s*", "", clean)
                    clean = re.sub(r"\s*```$", "", clean)
                data = json.loads(clean)
                if isinstance(data, dict) and ("valid" in data or "conflicts" in data):
                    value = data.get("valid", True)
                    data["valid"] = value is True or (isinstance(value, str) and value.strip().lower() == "true")
                    data["conflicts"] = data.get("conflicts") if isinstance(data.get("conflicts"), list) else []
                    data["warnings"] = data.get("warnings") if isinstance(data.get("warnings"), list) else []
                    has_contradictions = data.get("has_contradictions", False)
                    has_contradictions = has_contradictions is True or (
                        isinstance(has_contradictions, str) and has_contradictions.strip().lower() == "true"
                    )
                    blocking = any(
                        isinstance(conflict, dict) and conflict.get("blocking", True) is not False
                        for conflict in data["conflicts"]
                    )
                    data["has_contradictions"] = has_contradictions or blocking
                    hard_check = self._heuristic_prompt_validation(sev_s, low_s, norm_s, night_s)
                    if not hard_check["valid"]:
                        data["valid"] = False
                        data["has_contradictions"] = True
                        data["conflicts"] = hard_check["conflicts"] + data["conflicts"]
                        data["summary"] = hard_check["summary"]
                    if data["has_contradictions"]:
                        data["valid"] = False
                    data["_raw_vlm_response"] = raw_resp
                    return data
            except Exception:
                pass

        fallback = self._heuristic_prompt_validation(sev_s, low_s, norm_s, night_s)
        fallback["_raw_vlm_response"] = raw_resp or ""
        return fallback

    def _heuristic_prompt_validation(
        self,
        sev_s: str,
        low_s: str,
        norm_s: str,
        night_s: str,
    ) -> dict:
        import re

        def norm(value: str) -> str:
            value = re.sub(r"[^a-z0-9 ]+", " ", value.lower())
            value = re.sub(r"\bpeople\b|\bpersons\b", "person", value)
            value = re.sub(r"\b(an|a|the)\b", " ", value)
            return re.sub(r"\s+", " ", value).strip()
        def items(value: str) -> list[str]:
            parts = []
            for line in value.splitlines():
                parts.extend(line.split(","))
            return [item.strip() for item in parts if item.strip()]

        sev_items, low_items = items(sev_s), items(low_s)
        conflicts = []
        routine_norm = {norm(item): item for item in low_items}
        baseline_norm = norm(norm_s)

        for severe in sev_items:
            severe_n = norm(severe)
            if not severe_n:
                continue
            for routine_n, routine in routine_norm.items():
                if severe_n == routine_n or (len(severe_n) > 18 and (severe_n in routine_n or routine_n in severe_n)):
                    conflicts.append({
                        "type": "direct_overlap",
                        "fields": ["severe_incidents", "low_incidents"],
                        "new_item": severe,
                        "related_item": routine,
                        "reason": "The same visually observable activity appears in both the severe and routine lists.",
                        "suggestion": "Keep the routine activity in the whitelist and rewrite the severe entry as a visually distinct escalation.",
                        "blocking": True,
                    })
            if severe_n and severe_n in baseline_norm and len(severe_n) > 18:
                conflicts.append({
                    "type": "baseline_conflict",
                    "fields": ["severe_incidents", "normal_context"],
                    "new_item": severe,
                    "related_item": norm_s,
                    "reason": "The baseline explicitly contains the same activity as a normal condition.",
                    "suggestion": "Clarify the baseline or describe only the escalated threat in the severe list.",
                    "blocking": True,
                })

        return {
            "valid": not conflicts,
            "summary": "No blocking logical or visual contradictions detected." if not conflicts else f"Detected {len(conflicts)} blocking contradiction(s).",
            "conflicts": conflicts,
            "warnings": [],
        }


    # ── Background health loop ─────────────────────────────────────────────

    async def _health_loop(self) -> None:
        while True:
            try:
                # Fast polling (5s) while any shard is offline, standard interval (15s) when healthy
                interval = 5.0 if not self.has_healthy_shards() else 15.0
                await asyncio.sleep(interval)
                for shard in self._shards:
                    try:
                        async with self._session.get(
                            f"{shard.url}/health",
                            timeout=aiohttp.ClientTimeout(total=3)
                        ) as r:
                            was_healthy = shard.healthy
                            shard.healthy = (r.status == 200)
                            if not was_healthy and shard.healthy:
                                print(f"[VLMPool] ✅ Shard {shard.url} is ONLINE and healthy")
                                # Auto-detect model if missing
                                if not shard.model:
                                    try:
                                        async with self._session.get(f"{shard.url}/v1/models", timeout=aiohttp.ClientTimeout(total=3)) as mr:
                                            if mr.status == 200:
                                                mdata = await mr.json()
                                                models = mdata.get("data", [])
                                                if models and models[0].get("id"):
                                                    shard.model = models[0]["id"]
                                    except Exception:
                                        pass
                    except Exception:
                        if shard.healthy:
                            shard.healthy = False
                            print(f"[VLMPool] ⚠️ Shard {shard.url} unreachable — temporarily suspended")
            except asyncio.CancelledError:
                break
            except Exception as exc:
                error_tracker.capture_exception(
                    exc, component="VLMClient",
                    effect="VLM health loop encountered an unexpected error; continuing",
                    severity="WARNING",
                )


# Backwards-compatible alias — existing imports of VLMPool continue to work
VLMPool = MIGAwareVLMPool


# ══════════════════════════════════════════════════════════════════════════════
#  Helpers
# ══════════════════════════════════════════════════════════════════════════════

import json
import re

_COMMA_HEAL_RE = re.compile(r'("\s*:\s*[^",{}\[\]]+?)(\n\s*")')


def _parse_response(raw: str, cam_name: str) -> dict:
    result: dict = {
        "cam":         cam_name,
        "observation": raw.strip()[:300] if raw else "",
        "activity":    "UNKNOWN",
        "workers":     "0",
        "machinery":   "None",
        "safety":      "UNKNOWN",
        "severity":    "LOW",
        "reasoning":   "",
        "evolution":   "None",
        "procedure_checklist": "[]",
        "priority_flags":      "[]",
        "routine_flags":       "[]",
        "verdict":     "SETTLED",
        "keywords":    "",
    }

    # Strip markdown code fences if present
    clean = raw.strip()
    if clean.startswith("```"):
        clean = re.sub(r"^```[a-zA-Z]*\n?", "", clean)
        clean = re.sub(r"\n?```$", "", clean).strip()

    # Tier 1: Try direct JSON parse
    try:
        data = json.loads(clean)
        if isinstance(data, dict):
            for k, v in data.items():
                k_lower = k.lower()
                for key in _PARSE_KEYS:
                    if key in k_lower:
                        result[key] = str(v).upper() if key not in ("observation", "workers", "machinery", "evolution", "reasoning", "keywords", "procedure_checklist") else str(v)
            if "verdict" in data:
                result["verdict"] = str(data["verdict"]).upper()
            if "keywords" in data:
                result["keywords"] = str(data["keywords"])
            
            return result
    except Exception:
        pass

    # Tier 2: Try comma-healed JSON parse
    try:
        healed = _COMMA_HEAL_RE.sub(r'\1,\2', clean)
        data = json.loads(healed)
        if isinstance(data, dict):
            for k, v in data.items():
                k_lower = k.lower()
                for key in _PARSE_KEYS:
                    if key in k_lower:
                        result[key] = str(v).upper() if key not in ("observation", "workers", "machinery", "evolution", "reasoning", "keywords", "procedure_checklist") else str(v)
            if "verdict" in data:
                result["verdict"] = str(data["verdict"]).upper()
            if "keywords" in data:
                result["keywords"] = str(data["keywords"])

            return result
    except Exception:
        pass

    # Tier 3: Robust regex and line-by-line fallback field extraction
    for line in raw.split("\n"):
        if ":" in line:
            k, _, v = line.partition(":")
            k = k.strip().lower()
            v = v.strip().strip('"\'')
            for key in _PARSE_KEYS:
                if key in k:
                    result[key] = (
                        v.upper()
                        if key not in ("observation", "workers", "machinery", "evolution", "reasoning")
                        else v
                    )
                    break
            if "verdict" in k:
                result["verdict"] = v.upper()
            if "keyword" in k:
                result["keywords"] = v

    # Inline regex sweeps for critical fields if still at default/unknown
    for field in ("safety", "severity", "verdict"):
        if result[field] in ("UNKNOWN", "LOW", "SETTLED"):
            m = re.search(rf"\b{field}\b\s*(?:[:=]|is)?\s*[\"']?([a-zA-Z_]+)", raw, re.IGNORECASE)
            if m:
                extracted = m.group(1).strip().upper()
                if field == "safety" and extracted in ("OK", "WARNING", "DANGER", "CRITICAL", "UNKNOWN"):
                    result["safety"] = extracted
                elif field == "severity" and extracted in ("LOW", "MEDIUM", "HIGH", "EXTREME"):
                    result["severity"] = extracted
                elif field == "verdict" and extracted in ("ALERT", "MONITORING", "SETTLED"):
                    result["verdict"] = extracted

    m_obs = re.search(r"\bobservation\b\s*(?:[:=]|is)?\s*[\"']?([^\"\n\r]+)", raw, re.IGNORECASE)
    if m_obs and (not result["observation"] or result["observation"] == raw[:300]):
        result["observation"] = m_obs.group(1).strip()

    m_reason = re.search(r"\breasoning\b\s*(?:[:=]|is)?\s*[\"']?([^\"\n\r]+)", raw, re.IGNORECASE)
    if m_reason and not result["reasoning"]:
        result["reasoning"] = m_reason.group(1).strip()

    return result



def _fallback_result(cam_name: str, latency: float) -> dict:
    return {
        "cam":         cam_name,
        "observation": "VLM unavailable",
        "activity":    "UNKNOWN",
        "workers":     "0",
        "machinery":   "None",
        "safety":      "UNKNOWN",
        "severity":    "LOW",
        "reasoning":   "None",
        "evolution":   "None",
        "error":       True,
        "latency":     latency,
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
        "tokens": 0,
    }
