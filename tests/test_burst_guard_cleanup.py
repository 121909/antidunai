import asyncio
from collections.abc import Sequence
from typing import cast

import pytest
from pyrogram import Client
from pyrogram.errors import FloodWait, MessageDeleteForbidden, MessageIdInvalid

from plugins import burst_guard as plugin
from services.burst_guard import BurstCleanup, BurstGuardService


class FixedRandom:
    def __init__(self, value: int) -> None:
        self.value = value

    def randrange(self, stop: int) -> int:
        assert self.value < stop
        return self.value


class RecordingClient:
    def __init__(self) -> None:
        self.calls: list[tuple[int, tuple[int, ...]]] = []

    async def delete_messages(self, chat_id: int, message_ids: Sequence[int]) -> int:
        self.calls.append((chat_id, tuple(message_ids)))
        return len(message_ids)


def as_client(client: object) -> Client:
    return cast(Client, client)


@pytest.mark.asyncio
async def test_delete_messages_uses_telegram_sized_batches() -> None:
    client = RecordingClient()

    await plugin.delete_burst_messages(as_client(client), -100, list(range(1, 206)))

    assert [len(message_ids) for _, message_ids in client.calls] == [100, 100, 5]


@pytest.mark.asyncio
async def test_delete_messages_waits_and_retries_flood_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    class FloodClient(RecordingClient):
        async def delete_messages(self, chat_id: int, message_ids: Sequence[int]) -> int:
            self.calls.append((chat_id, tuple(message_ids)))
            if len(self.calls) == 1:
                raise FloodWait(2)
            return len(message_ids)

    sleeps: list[int] = []

    async def fake_sleep(delay: int) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(plugin.asyncio, "sleep", fake_sleep)
    client = FloodClient()

    await plugin.delete_burst_messages(as_client(client), -100, [1, 2])

    assert sleeps == [2]
    assert client.calls == [(-100, (1, 2)), (-100, (1, 2))]


@pytest.mark.asyncio
async def test_invalid_batch_falls_back_to_individual_deletes() -> None:
    class InvalidClient(RecordingClient):
        async def delete_messages(self, chat_id: int, message_ids: Sequence[int]) -> int:
            ids = tuple(message_ids)
            self.calls.append((chat_id, ids))
            if len(ids) > 1 or ids == (2,):
                raise MessageIdInvalid()
            return len(ids)

    client = InvalidClient()

    await plugin.delete_burst_messages(as_client(client), -100, [1, 2, 3])

    assert client.calls == [(-100, (1, 2, 3)), (-100, (1,)), (-100, (2,)), (-100, (3,))]


@pytest.mark.asyncio
async def test_permission_error_does_not_block_other_cleanup(monkeypatch: pytest.MonkeyPatch) -> None:
    class PermissionClient(RecordingClient):
        async def delete_messages(self, chat_id: int, message_ids: Sequence[int]) -> int:
            ids = tuple(message_ids)
            self.calls.append((chat_id, ids))
            if ids == (1,):
                raise MessageDeleteForbidden()
            return len(ids)

    service = BurstGuardService()
    monkeypatch.setattr(plugin, "burst_guard", service)
    client = PermissionClient()
    cleanups = [
        BurstCleanup(chat_id=-100, source_message_id=1, message_ids=(1,), tasks=()),
        BurstCleanup(chat_id=-100, source_message_id=2, message_ids=(2,), tasks=()),
    ]

    await plugin.cleanup_evictions(as_client(client), cleanups)

    assert client.calls == [(-100, (1,)), (-100, (2,))]


@pytest.mark.asyncio
async def test_cleanup_cancels_tasks_and_deletes_source_and_outputs(monkeypatch: pytest.MonkeyPatch) -> None:
    service = BurstGuardService(rng=FixedRandom(0))
    monkeypatch.setattr(plugin, "burst_guard", service)
    await service.register_candidate(-100, 10, 1)
    await service.register_candidate(-100, 10, 2)
    await service.record_output(-100, 2, [20, 21])

    async def wait_forever() -> None:
        await asyncio.Event().wait()

    task = asyncio.create_task(wait_forever())
    await service.attach_task(-100, 2, task)
    third = await service.register_candidate(-100, 10, 3)
    cleanup = next(item for item in third.evictions if item.source_message_id == 2)
    client = RecordingClient()

    await plugin.cleanup_evictions(as_client(client), [cleanup])

    with pytest.raises(asyncio.CancelledError):
        await task
    await service.detach_task(-100, 2, task)
    await service.finish_processing(-100, 2)
    after_cleanup = await service.record_output(-100, 2, [22])
    assert client.calls == [(-100, (2, 20, 21))]
    assert after_cleanup.tracked is False
