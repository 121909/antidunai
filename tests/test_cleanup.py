from __future__ import annotations

import logging
from collections.abc import Sequence

import pytest
from telethon.errors import (
    FloodWaitError,
    MessageDeleteForbiddenError,
    MessageIdInvalidError,
)

from burst_guard.cleanup import CleanupService
from burst_guard.metrics import Metrics
from burst_guard.models import CleanupRequest


class FakeClient:
    def __init__(self, responses: list[BaseException | None] | None = None) -> None:
        self.responses = list(responses or [])
        self.calls: list[tuple[int, tuple[int, ...]]] = []

    async def delete_messages(self, entity: int, message_ids: Sequence[int]) -> object:
        self.calls.append((entity, tuple(message_ids)))
        if self.responses:
            response = self.responses.pop(0)
            if response is not None:
                raise response
        return object()


def service(
    client: FakeClient,
    *,
    dry_run: bool = False,
    batch_size: int = 100,
    retry_limit: int = 3,
    metrics: Metrics | None = None,
    sleep=None,
) -> CleanupService:
    options = {}
    if sleep is not None:
        options["sleep"] = sleep
    return CleanupService(
        client,
        batch_size=batch_size,
        retry_limit=retry_limit,
        dry_run=dry_run,
        metrics=metrics or Metrics(),
        logger=logging.getLogger("test-cleanup"),
        **options,
    )


def request(*message_ids: int) -> CleanupRequest:
    return CleanupRequest(-1001, tuple(message_ids), "test", "run-1")


@pytest.mark.asyncio
async def test_dry_run_never_calls_telegram() -> None:
    client = FakeClient()
    metrics = Metrics()

    report = await service(client, dry_run=True, metrics=metrics).execute(request(1, 2))

    assert client.calls == []
    assert report.planned == 2 and report.deleted == 0
    assert metrics.get("deletions_planned") == 2


@pytest.mark.asyncio
async def test_batches_are_limited_to_configured_size() -> None:
    client = FakeClient()

    report = await service(client, batch_size=2).execute(request(1, 2, 3, 4, 5))

    assert client.calls == [(-1001, (1, 2)), (-1001, (3, 4)), (-1001, (5,))]
    assert report.deleted == 5 and report.failed == 0


@pytest.mark.asyncio
async def test_flood_wait_uses_server_delay_then_retries() -> None:
    client = FakeClient([FloodWaitError(None, capture=7), None])
    waits: list[float] = []

    async def sleep(seconds: float) -> None:
        waits.append(seconds)

    report = await service(client, sleep=sleep).execute(request(1, 2))

    assert waits == [7]
    assert len(client.calls) == 2
    assert report.deleted == 2 and report.retries == 1


@pytest.mark.asyncio
async def test_flood_wait_stops_at_retry_limit() -> None:
    client = FakeClient([FloodWaitError(None, capture=1), FloodWaitError(None, capture=2)])

    async def sleep(_: float) -> None:
        return None

    report = await service(client, retry_limit=1, sleep=sleep).execute(request(1))

    assert len(client.calls) == 2
    assert report.failed == 1 and report.retries == 1


@pytest.mark.asyncio
async def test_invalid_message_ids_are_reported_separately() -> None:
    client = FakeClient([MessageIdInvalidError(None)])

    report = await service(client).execute(request(1, 2))

    assert report.deleted == 0 and report.failed == 2


@pytest.mark.asyncio
async def test_already_deleted_messages_are_idempotent_success() -> None:
    client = FakeClient([None])

    report = await service(client).execute(request(1, 2))

    assert report.deleted == 2 and report.failed == 0


@pytest.mark.asyncio
async def test_failed_batch_does_not_stop_later_batches() -> None:
    client = FakeClient([MessageDeleteForbiddenError(None), None])

    report = await service(client, batch_size=2).execute(request(1, 2, 3))

    assert len(client.calls) == 2
    assert report.failed == 2 and report.deleted == 1
