"""
VLMPool: async aiohttp client to one or more vLLM OpenAI-compatible endpoints.
Routes via least-connections — always picks the endpoint with fewest in-flight
requests, ensuring all N vLLM instances are kept busy in parallel.
"""
import asyncio
import time
from typing import Optional

import aiohttp


_PARSE_KEYS = {
    "observation": "observation",
    "activity": "activity",
    "workers": "workers",
    "machinery": "machinery",
    "safety": "safety",
    "severity": "severity",
}


class VLMPool:
    def __init__(self, endpoints: list[dict]):
        """
        endpoints: [{"url": "http://localhost:8000", "model": "Qwen/..."}]
        """
        if not endpoints:
            raise ValueError("VLMPool requires at least one endpoint")
        self.endpoints = endpoints
        # in-flight counter per endpoint — used for least-connections routing
        self._inflight: list[int] = [0] * len(endpoints)
        self._lock = asyncio.Lock()   # protects _inflight selection
        self._session: Optional[aiohttp.ClientSession] = None

    async def start(self) -> None:
        connector = aiohttp.TCPConnector(limit=64, limit_per_host=32)
        timeout = aiohttp.ClientTimeout(total=60, connect=5)
        self._session = aiohttp.ClientSession(connector=connector, timeout=timeout)
        print(f"[VLMPool] Started — {len(self.endpoints)} endpoint(s):")

        # Auto-discover models from endpoints to prevent 404s
        for ep in self.endpoints:
            try:
                async with self._session.get(f"{ep['url']}/v1/models", timeout=3) as r:
                    if r.status == 200:
                        data = await r.json()
                        models = data.get("data", [])
                        if models:
                            actual_model = models[0].get("id")
                            if actual_model:
                                print(f"[VLMPool] Auto-detected model '{actual_model}' on {ep['url']}")
                                ep["model"] = actual_model
            except Exception as e:
                print(f"[VLMPool] Failed to auto-detect model on {ep['url']}: {e}")

        for ep in self.endpoints:
            print(f"  → {ep['url']}  model={ep['model']}")

    async def stop(self) -> None:
        if self._session:
            await self._session.close()

    async def health_check(self) -> dict[str, bool]:
        """Check which endpoints are reachable."""
        results = {}
        for ep in self.endpoints:
            try:
                async with self._session.get(
                    f"{ep['url']}/health", timeout=aiohttp.ClientTimeout(total=3)
                ) as r:
                    results[ep["url"]] = r.status == 200
            except Exception:
                results[ep["url"]] = False
        return results

    async def analyze(
        self, cam_name: str, frame_b64: str, system_prompt: str
    ) -> dict:
        # Least-connections: pick the endpoint with fewest in-flight requests
        async with self._lock:
            idx = self._inflight.index(min(self._inflight))
            self._inflight[idx] += 1
        ep = self.endpoints[idx]
        url = f"{ep['url']}/v1/chat/completions"
        model = ep["model"]

        content = [
            {"type": "text", "text": system_prompt},
            {
                "type": "image_url",
                "image_url": {"url": f"data:image/jpeg;base64,{frame_b64}"},
            },
        ]
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": content}],
            "temperature": 0.25,
            "max_tokens": 220,
            "stop": ["<think>", "</think>"]
        }

        try:
            for attempt in range(3):
                try:
                    async with self._session.post(url, json=payload) as resp:
                        if resp.status != 200:
                            body = await resp.text()
                            print(
                                f"[VLMPool] HTTP {resp.status} for {cam_name} "
                                f"(attempt {attempt+1}): {body[:200]}"
                            )
                            await asyncio.sleep(1)
                            continue
                        data = await resp.json()
                        raw = data["choices"][0]["message"]["content"]
                        return self._parse(raw, cam_name)
                except asyncio.TimeoutError:
                    print(f"[VLMPool] Timeout — {cam_name} (attempt {attempt+1})")
                    await asyncio.sleep(1)
                except Exception as exc:
                    print(f"[VLMPool] Error — {cam_name} (attempt {attempt+1}): {exc}")
                    await asyncio.sleep(1)

            return {
                "cam": cam_name,
                "observation": "VLM unavailable",
                "activity": "UNKNOWN",
                "workers": "0",
                "machinery": "None",
                "safety": "UNKNOWN",
                "severity": "LOW",
                "error": True,
            }
        finally:
            self._inflight[idx] -= 1

    def _parse(self, raw: str, cam_name: str) -> dict:
        result: dict = {
            "cam": cam_name,
            "observation": raw.strip()[:300] if raw else "",
            "activity": "UNKNOWN",
            "workers": "0",
            "machinery": "None",
            "safety": "UNKNOWN",
            "severity": "LOW",
        }
        for line in raw.split("\n"):
            if ":" not in line:
                continue
            k, _, v = line.partition(":")
            k = k.strip().lower()
            v = v.strip()
            for key in _PARSE_KEYS:
                if key in k:
                    result[key] = v.upper() if key not in ("observation", "workers", "machinery") else v
                    break
        return result
