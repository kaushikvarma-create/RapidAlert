"""
WSManager: manages all active WebSocket connections.
broadcast() fans out to every connected client, auto-prunes dead connections.
"""
from __future__ import annotations

import asyncio
from typing import Any
from fastapi import WebSocket

from backend.core.error_tracker import error_tracker


class WSManager:
    def __init__(self):
        self._connections: list[WebSocket] = []
        self._lock = asyncio.Lock()

    @property
    def has_clients(self) -> bool:
        return bool(self._connections)

    @property
    def client_count(self) -> int:
        return len(self._connections)

    @property
    def _clients(self) -> list[WebSocket]:
        """Backward compatibility alias for _connections."""
        return self._connections

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
        async with self._lock:
            targets = list(self._connections)
        if not targets:
            return

        async def _send(ws: WebSocket):
            try:
                await asyncio.wait_for(ws.send_json(data), timeout=1.5)
                return None
            except Exception:
                return ws

        results = await asyncio.gather(*[_send(ws) for ws in targets], return_exceptions=True)
        dead = [ws for ws in results if isinstance(ws, WebSocket)]
        if dead:
            async with self._lock:
                for ws in dead:
                    if ws in self._connections:
                        self._connections.remove(ws)

    async def close_all(self) -> None:
        """Closes all active WebSockets immediately on server shutdown."""
        async with self._lock:
            clients = list(self._connections)
            self._connections.clear()
        for ws in clients:
            try:
                await ws.close(code=1000)
            except Exception:
                pass
