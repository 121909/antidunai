from __future__ import annotations

import asyncio

import pytest

from burst_guard.health import HealthServer, HealthState


async def get(port: int, path: str) -> bytes:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(f"GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode())
    await writer.drain()
    response = await reader.read()
    writer.close()
    await writer.wait_closed()
    return response


@pytest.mark.asyncio
async def test_live_and_ready_endpoints() -> None:
    connected = True
    state = HealthState(config_valid=True)
    server = HealthServer("127.0.0.1", 0, state, lambda: connected)
    await server.start()
    assert server._server is not None
    port = int(server._server.sockets[0].getsockname()[1])
    try:
        live = await get(port, "/health/live")
        not_ready = await get(port, "/health/ready")
        state.preflight_passed = True
        state.listener_running = True
        ready = await get(port, "/health/ready")
        missing = await get(port, "/missing")
    finally:
        await server.close()

    assert b"200 OK" in live and b'"live"' in live
    assert b"503 Service Unavailable" in not_ready
    assert b"200 OK" in ready and b'"ready"' in ready
    assert b"404 Not Found" in missing
