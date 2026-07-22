from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from dataclasses import dataclass


@dataclass(slots=True)
class HealthState:
    config_valid: bool = False
    preflight_passed: bool = False
    listener_running: bool = False


class HealthServer:
    def __init__(
        self,
        host: str,
        port: int,
        state: HealthState,
        is_connected: Callable[[], bool],
    ) -> None:
        self._host = host
        self._port = port
        self._state = state
        self._is_connected = is_connected
        self._server: asyncio.Server | None = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, self._host, self._port)

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    def ready(self) -> bool:
        return (
            self._state.config_valid
            and self._state.preflight_passed
            and self._state.listener_running
            and self._is_connected()
        )

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        status = 400
        payload = {"status": "bad_request"}
        try:
            request_line = await asyncio.wait_for(reader.readline(), timeout=2)
            parts = request_line.decode("ascii", errors="replace").strip().split()
            if len(parts) == 3 and parts[0] == "GET":
                if parts[1] == "/health/live":
                    status, payload = 200, {"status": "live"}
                elif parts[1] == "/health/ready":
                    ready = self.ready()
                    status = 200 if ready else 503
                    payload = {"status": "ready" if ready else "not_ready"}
                else:
                    status, payload = 404, {"status": "not_found"}
        except (TimeoutError, UnicodeError):
            pass
        body = json.dumps(payload, separators=(",", ":")).encode("ascii")
        reason = {200: "OK", 400: "Bad Request", 404: "Not Found", 503: "Service Unavailable"}[
            status
        ]
        writer.write(
            f"HTTP/1.1 {status} {reason}\r\n".encode()
            + b"Content-Type: application/json\r\n"
            + f"Content-Length: {len(body)}\r\n".encode()
            + b"Connection: close\r\n\r\n"
            + body
        )
        await writer.drain()
        writer.close()
        await writer.wait_closed()
