"""
WSManager: manages all active WebSocket connections.
broadcast() fans out to every connected client, auto-prunes dead connections.
"""
import asyncio
from typing import Any

from fastapi import WebSocket


class WSManager:
    def __init__(self):
        self._connections: list[WebSocket] = []
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        async with self._lock:
            self._connections.append(ws)
        print(f"[WS] Client connected  — total: {len(self._connections)}")

    async def disconnect(self, ws: WebSocket) -> None:
        async with self._lock:
            if ws in self._connections:
                self._connections.remove(ws)
        print(f"[WS] Client disconnected — total: {len(self._connections)}")

    async def broadcast(self, data: Any) -> None:
        if not self._connections:
            return
        dead: list[WebSocket] = []
        async with self._lock:
            targets = list(self._connections)
        for ws in targets:
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        if dead:
            async with self._lock:
                for ws in dead:
                    if ws in self._connections:
                        self._connections.remove(ws)
