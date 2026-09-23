"""
PriorityQueueAdapter: Unified Priority Queue for MLFQ Alert Scheduling.

Provides:
  - RedisPriorityQueue: Distributed ZSET-based priority queue with Pub/Sub wake-ups.
  - InMemoryPriorityQueue: Low-overhead asyncio.PriorityQueue implementation.
  - PriorityQueueAdapter: Auto-detects Redis; falls back to in-memory silently.
"""
from __future__ import annotations

import asyncio
import json
import time
from abc import ABC, abstractmethod
from typing import Any, Optional, Tuple


class _QueueBackend(ABC):
    @abstractmethod
    async def put(self, item: Tuple[int, int, dict]) -> None:
        """Put (priority, seq, job_dict) into queue."""
        pass

    @abstractmethod
    async def get(self) -> Tuple[int, int, dict]:
        """Pop the highest priority item (lowest priority number) from queue."""
        pass

    @abstractmethod
    def task_done(self) -> None:
        pass

    @abstractmethod
    def qsize(self) -> int:
        pass

    @abstractmethod
    def empty(self) -> bool:
        pass

    @abstractmethod
    async def stop(self) -> None:
        pass


class InMemoryPriorityQueue(_QueueBackend):
    def __init__(self):
        self._queue: asyncio.PriorityQueue[Tuple[int, int, dict]] = asyncio.PriorityQueue()

    async def put(self, item: Tuple[int, int, dict]) -> None:
        await self._queue.put(item)

    async def get(self) -> Tuple[int, int, dict]:
        return await self._queue.get()

    def task_done(self) -> None:
        self._queue.task_done()

    def qsize(self) -> int:
        return self._queue.qsize()

    def empty(self) -> bool:
        return self._queue.empty()

    async def stop(self) -> None:
        pass


class RedisPriorityQueue(_QueueBackend):
    """
    Redis ZSET-based priority queue.
    Score = priority * 10_000_000_000 + int(time.time() * 1000)
    Uses Redis Pub/Sub for immediate worker wake-up on enqueue.
    """
    def __init__(self, redis_url: str = "redis://localhost:6379/0", queue_key: str = "rapidalert:queue"):
        self.redis_url = redis_url
        self.queue_key = queue_key
        self.channel_key = f"{queue_key}:wake"
        self._client = None
        self._pubsub = None
        self._wake_event = asyncio.Event()
        self._listener_task: Optional[asyncio.Task] = None
        self._running = False

    async def init(self) -> bool:
        try:
            import redis.asyncio as aioredis
            self._client = aioredis.from_url(self.redis_url, decode_responses=True, socket_connect_timeout=1.0)
            await self._client.ping()
            self._pubsub = self._client.pubsub()
            await self._pubsub.subscribe(self.channel_key)
            self._running = True
            self._listener_task = asyncio.create_task(self._listen_wake(), name="redis-queue-wake-listener")
            return True
        except Exception:
            await self.stop()
            return False

    async def _listen_wake(self) -> None:
        try:
            async for _ in self._pubsub.listen():
                if not self._running:
                    break
                self._wake_event.set()
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    async def put(self, item: Tuple[int, int, dict]) -> None:
        priority, seq, job = item
        score = priority * 10_000_000_000 + int(time.time() * 1000)
        payload = json.dumps({"p": priority, "s": seq, "j": job})
        if self._client:
            await self._client.zadd(self.queue_key, {payload: score})
            await self._client.publish(self.channel_key, "1")

    async def get(self) -> Tuple[int, int, dict]:
        while self._running:
            if self._client:
                res = await self._client.zpopmin(self.queue_key, count=1)
                if res:
                    payload_str, _ = res[0]
                    data = json.loads(payload_str)
                    return data["p"], data["s"], data["j"]
            
            # Wait for wake event or timeout
            self._wake_event.clear()
            try:
                await asyncio.wait_for(self._wake_event.wait(), timeout=0.1)
            except asyncio.TimeoutError:
                pass
        raise asyncio.CancelledError("Redis queue stopped")

    def task_done(self) -> None:
        pass

    def qsize(self) -> int:
        return 0  # async ZCARD can be queried via depth()

    async def depth(self) -> int:
        if self._client:
            return await self._client.zcard(self.queue_key)
        return 0

    def empty(self) -> bool:
        return False

    async def stop(self) -> None:
        self._running = False
        if self._listener_task:
            self._listener_task.cancel()
        if self._pubsub:
            try:
                await self._pubsub.unsubscribe(self.channel_key)
                await self._pubsub.close()
            except Exception:
                pass
        if self._client:
            try:
                await self._client.close()
            except Exception:
                pass


class PriorityQueueAdapter:
    """
    Factory / Adapter that selects Redis if available, else InMemory.
    Provides identical async interface for DeadlineScheduler.
    """
    def __init__(self, redis_url: Optional[str] = "redis://localhost:6379/0"):
        self.redis_url = redis_url
        self._backend: Optional[_QueueBackend] = None
        self._is_redis = False

    async def start(self) -> None:
        if self.redis_url:
            rq = RedisPriorityQueue(self.redis_url)
            if await rq.init():
                self._backend = rq
                self._is_redis = True
                print("[PriorityQueue] 🚀 Connected to Redis Priority Queue (ZSET + Pub/Sub)")
                return
        
        self._backend = InMemoryPriorityQueue()
        self._is_redis = False
        print("[PriorityQueue] ⚡ Using In-Memory asyncio Priority Queue (Zero-IPC Latency)")

    @property
    def is_redis(self) -> bool:
        return self._is_redis

    async def put(self, item: Tuple[int, int, dict]) -> None:
        if self._backend:
            await self._backend.put(item)

    async def get(self) -> Tuple[int, int, dict]:
        if self._backend:
            return await self._backend.get()
        raise RuntimeError("PriorityQueue not started")

    def task_done(self) -> None:
        if self._backend:
            self._backend.task_done()

    def qsize(self) -> int:
        return self._backend.qsize() if self._backend else 0

    def empty(self) -> bool:
        return self._backend.empty() if self._backend else True

    async def stop(self) -> None:
        if self._backend:
            await self._backend.stop()
