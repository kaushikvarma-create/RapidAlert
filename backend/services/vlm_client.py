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
    "evolution":   "evolution",
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
    ):
        self.url            = url
        self.model          = model
        self.weight         = max(1, weight)
        self.max_concurrent = max(1, max_concurrent)
        self.queue_depth    = max(1, queue_depth)
        self.mig_uuid       = mig_uuid
        self.mig_profile    = mig_profile
        self._session       = session

        self._queue: asyncio.Queue       = asyncio.Queue(maxsize=queue_depth)
        self._sem:   asyncio.Semaphore   = asyncio.Semaphore(max_concurrent)
        self._workers: list[asyncio.Task] = []
        self.healthy = True

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
        """Drain the queue, acquire semaphore, execute inference, resolve Future."""
        while True:
            try:
                job, fut = await self._queue.get()
            except asyncio.CancelledError:
                break

            try:
                async with self._sem:
                    self.stat_inflight += 1
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
                            camera=job.get("cam"),
                            effect=f"Worker {worker_id} on {self.url} unhandled exception; future resolved with exception",
                            severity="ERROR",
                        )
            except asyncio.CancelledError:
                if not fut.done():
                    fut.cancel()
                break
            finally:
                if self.stat_inflight > 0:
                    self.stat_inflight -= 1
                self._queue.task_done()

    # ── Enqueue ────────────────────────────────────────────────────────────

    def enqueue_nowait(self, job: dict, fut: asyncio.Future) -> bool:
        """
        Non-blocking enqueue. Returns True if accepted, False if queue full.
        """
        try:
            self._queue.put_nowait((job, fut))
            self.stat_queued += 1
            return True
        except asyncio.QueueFull:
            return False

    async def enqueue_wait(self, job: dict, fut: asyncio.Future) -> None:
        """Blocking enqueue — used as last-resort when all shards are full."""
        await self._queue.put((job, fut))
        self.stat_queued += 1

    # ── Inference (with retries) ───────────────────────────────────────────

    async def _infer(self, job: dict) -> dict:
        cam_name   = job["cam"]
        frames_b64 = job["frames_b64"]
        prompt     = job["prompt"]
        labels     = job.get("labels") or []
        url        = f"{self.url}/v1/chat/completions"

        content = []
        frames = frames_b64 if isinstance(frames_b64, list) else [frames_b64]
        if len(frames) > 1:
            # Interleave explicit frame label markers to anchor multi-frame vision attention
            for idx, b64 in enumerate(frames):
                label_txt = labels[idx] if idx < len(labels) else f"Frame {idx + 1}"
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

        payload = {
            "model":       self.model,
            "messages":    [{"role": "user", "content": content}],
            "max_tokens":  256,
            "temperature": 0.2,
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
                    result = _parse_response(raw, cam_name)
                    result["latency"] = time.monotonic() - t0
                    self._latencies.append(result["latency"])
                    return result

            except asyncio.TimeoutError as exc:
                error_tracker.capture_exception(
                    exc, component="VLMClient", camera=cam_name,
                    effect=f"Timeout on {self.url} (attempt {attempt+1}/{_RETRY_COUNT}); retrying in 1s",
                    severity="WARNING",
                )
                await asyncio.sleep(1)

            except Exception as exc:
                error_tracker.capture_exception(
                    exc, component="VLMClient", camera=cam_name,
                    effect=f"Request error on {self.url} (attempt {attempt+1}/{_RETRY_COUNT}); retrying in 1s",
                    severity="WARNING",
                )
                await asyncio.sleep(1)

        # All retries exhausted
        error_tracker.capture_error(
            message=f"All {_RETRY_COUNT} inference attempts failed on {self.url}",
            component="VLMClient",
            camera=cam_name,
            effect=f"Inference exhausted all retries for {cam_name} on {self.url}; returning fallback result",
            severity="ERROR",
        )
        return _fallback_result(cam_name, time.monotonic() - t0)

    # ── Stats ──────────────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        lats = list(self._latencies)
        port = self.url.split(":")[-1] if ":" in self.url else self.url
        return {
            "url":          self.url,
            "port":         port,
            "model":        self.model,
            "weight":       self.weight,
            "max_concurrent": self.max_concurrent,
            "healthy":      self.healthy,
            "queued":       self._queue.qsize(),
            "inflight":     self.stat_inflight,
            "in_flight":    self.stat_inflight,
            "completed":    self.stat_completed,
            "errors":       self.stat_errors,
            "mig_uuid":     self.mig_uuid,
            "is_mig":       bool(self.mig_uuid),
            "avg_latency_ms": round(sum(lats) / len(lats) * 1000, 1) if lats else None,
            "p95_latency_ms": round(sorted(lats)[int(len(lats) * 0.95)] * 1000, 1) if len(lats) >= 5 else None,
            "load_score":   round(self.load_score(), 3),
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
        connector = aiohttp.TCPConnector(limit=128, limit_per_host=64)
        timeout   = aiohttp.ClientTimeout(total=90, connect=5)
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
                session        = self._session,
            )
            self._shards.append(shard)

        # Auto-detect models from each endpoint's /v1/models
        for shard in self._shards:
            try:
                async with self._session.get(
                    f"{shard.url}/v1/models",
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as r:
                    if r.status == 200:
                        data   = await r.json()
                        models = data.get("data", [])
                        if models:
                            actual = models[0].get("id")
                            if actual:
                                shard.model = actual
                                print(f"[VLMPool] Auto-detected model '{actual}' on {shard.url}")
            except Exception as exc:
                error_tracker.capture_exception(
                    exc, component="VLMClient",
                    effect=f"Failed to auto-detect model on {shard.url}; using config value '{shard.model}'",
                    severity="WARNING",
                )

        # Start per-shard worker coroutines
        for shard in self._shards:
            shard.start_workers()

        # Start background health loop
        self._health_task = asyncio.create_task(
            self._health_loop(), name="vlm-health"
        )

        is_mig = any(bool(s.mig_uuid) for s in self._shards)
        pool_type = "MIG" if is_mig else "Shared"
        print(f"[VLMPool] ✅ Started — {len(self._shards)} {pool_type} shard(s):")
        for s in self._shards:
            print(
                f"  → {s.url}  model={s.model}  "
                f"weight={s.weight}  max_concurrent={s.max_concurrent}  "
                f"queue_depth={s.queue_depth}"
            )

    async def stop(self) -> None:
        if self._health_task:
            self._health_task.cancel()
        for shard in self._shards:
            shard.stop_workers()
        if self._session:
            await self._session.close()

    # ── Public API ────────────────────────────────────────────────────────

    @property
    def endpoints(self) -> list[dict]:
        """Backwards-compatible property (used by scheduler for logging)."""
        return [{"url": s.url, "model": s.model} for s in self._shards]

    async def health_check(self) -> dict[str, bool]:
        """Manual health check (all shards); updates healthy flags."""
        results = {}
        for shard in self._shards:
            try:
                async with self._session.get(
                    f"{shard.url}/health",
                    timeout=aiohttp.ClientTimeout(total=3)
                ) as r:
                    shard.healthy = (r.status == 200)
            except Exception as exc:
                shard.healthy = False
                error_tracker.capture_exception(
                    exc, component="VLMClient",
                    effect=f"Health check failed for {shard.url}; marked unavailable",
                    severity="WARNING",
                )
            results[shard.url] = shard.healthy
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
    ) -> dict:
        """
        Route to the MIG/shared shard with the lowest weighted load score,
        enqueue the job, and await the Future result.
        """
        job = {
            "cam": cam_name,
            "frames_b64": frame_b64,
            "prompt": system_prompt,
            "labels": labels,
        }
        fut: asyncio.Future = asyncio.get_event_loop().create_future()

        # Sort shards by ascending load_score (best first)
        ranked = sorted(self._shards, key=lambda s: s.load_score())

        for shard in ranked:
            if shard.enqueue_nowait(job, fut):
                return await fut

        # All queues full — block on the best shard
        best = ranked[0]
        await best.enqueue_wait(job, fut)
        return await fut

    async def analyze_concurrent(
        self,
        cam_name: str,
        frame_b64: "str | list[str]",
        system_prompt: str,
        labels: Optional[list[str]] = None,
    ) -> list[dict]:
        """Fire request to ALL shards simultaneously (comparator / ensemble mode)."""
        tasks = [
            self.analyze(cam_name, frame_b64, system_prompt, labels=labels)
            for _ in self._shards
        ]
        return await asyncio.gather(*tasks)

    # ── Background health loop ─────────────────────────────────────────────

    async def _health_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(_HEALTH_INTERVAL_SEC)
                for shard in self._shards:
                    try:
                        async with self._session.get(
                            f"{shard.url}/health",
                            timeout=aiohttp.ClientTimeout(total=3)
                        ) as r:
                            was_healthy = shard.healthy
                            shard.healthy = (r.status == 200)
                            if not was_healthy and shard.healthy:
                                print(f"[VLMPool] ✅ Shard {shard.url} recovered — re-routing enabled")
                    except Exception as exc:
                        if shard.healthy:
                            shard.healthy = False
                            error_tracker.capture_exception(
                                exc, component="VLMClient",
                                effect=f"Shard {shard.url} went unhealthy; routing suspended until recovered",
                                severity="ERROR",
                            )
            except asyncio.CancelledError:
                break
            except Exception as exc:
                error_tracker.capture_exception(
                    exc, component="VLMClient",
                    effect="VLM health loop encountered an error; continuing",
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
        "evolution":   "None",
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
                        result[key] = str(v).upper() if key not in ("observation", "workers", "machinery", "evolution", "keywords") else str(v)
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
                        result[key] = str(v).upper() if key not in ("observation", "workers", "machinery", "evolution", "keywords") else str(v)
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
                        if key not in ("observation", "workers", "machinery", "evolution")
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
        "evolution":   "None",
        "error":       True,
        "latency":     latency,
    }
