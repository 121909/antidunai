import asyncio
from collections import deque

import pytest

from services.burst_guard import BurstGuardService, BurstItemStatus


class FixedRandom:
    def __init__(self, *values: int) -> None:
        self.values = deque(values)

    def randrange(self, stop: int) -> int:
        value = self.values.popleft()
        assert 0 <= value < stop
        return value


@pytest.mark.asyncio
async def test_third_candidate_keeps_exactly_one() -> None:
    service = BurstGuardService(rng=FixedRandom(1))

    first = await service.register_candidate(-100, 10, 1)
    second = await service.register_candidate(-100, 10, 2)
    third = await service.register_candidate(-100, 10, 3)

    assert first.evictions == ()
    assert second.evictions == ()
    assert {cleanup.source_message_id for cleanup in third.evictions} == {1, 3}
    run = await service.get_run(-100)
    assert run is not None
    assert run.current_retained_source_message_id == 2
    assert run.item_statuses == (
        (1, BurstItemStatus.EVICTED),
        (2, BurstItemStatus.RETAINED),
        (3, BurstItemStatus.EVICTED),
    )


@pytest.mark.asyncio
async def test_later_candidate_can_replace_retained_item() -> None:
    service = BurstGuardService(rng=FixedRandom(0, 0))
    for message_id in range(1, 4):
        await service.register_candidate(-100, 10, message_id)

    fourth = await service.register_candidate(-100, 10, 4)

    assert [cleanup.source_message_id for cleanup in fourth.evictions] == [1]
    assert fourth.status is BurstItemStatus.RETAINED
    run = await service.get_run(-100)
    assert run is not None
    assert run.current_retained_source_message_id == 4


@pytest.mark.asyncio
async def test_later_candidate_can_leave_retained_item_unchanged() -> None:
    service = BurstGuardService(rng=FixedRandom(0, 3))
    for message_id in range(1, 4):
        await service.register_candidate(-100, 10, message_id)

    fourth = await service.register_candidate(-100, 10, 4)

    assert [cleanup.source_message_id for cleanup in fourth.evictions] == [4]
    assert fourth.status is BurstItemStatus.EVICTED
    run = await service.get_run(-100)
    assert run is not None
    assert run.current_retained_source_message_id == 1


@pytest.mark.asyncio
async def test_duplicate_candidate_is_not_counted_twice() -> None:
    service = BurstGuardService()

    first = await service.register_candidate(-100, 10, 1)
    duplicate = await service.register_candidate(-100, 10, 1)

    assert first.is_new is True
    assert duplicate.is_new is False
    assert duplicate.candidate_count == 1


@pytest.mark.asyncio
async def test_groups_and_target_users_have_isolated_runs() -> None:
    service = BurstGuardService()

    group_a = await service.register_candidate(-100, 10, 1)
    group_b = await service.register_candidate(-200, 10, 1)
    other_target = await service.register_candidate(-100, 20, 2)

    assert group_a.run_id == 1
    assert group_b.run_id == 1
    assert other_target.run_id == 2
    run_a = await service.get_run(-100)
    run_b = await service.get_run(-200)
    assert run_a is not None and run_a.user_id == 20 and run_a.candidate_count == 1
    assert run_b is not None and run_b.user_id == 10 and run_b.candidate_count == 1


@pytest.mark.asyncio
async def test_close_run_starts_a_new_segment() -> None:
    service = BurstGuardService()
    await service.register_candidate(-100, 10, 1)

    closed = await service.close_run(-100)
    new_run = await service.register_candidate(-100, 10, 2)

    assert closed is not None and closed.closed is True
    assert new_run.run_id == 2
    assert new_run.candidate_count == 1


@pytest.mark.asyncio
async def test_output_tracker_deletes_late_output_without_holding_group_lock() -> None:
    service = BurstGuardService(rng=FixedRandom(0))
    for message_id in range(1, 4):
        await service.register_candidate(-100, 10, message_id)

    delete_started = asyncio.Event()
    allow_delete = asyncio.Event()
    deleted: list[tuple[int, tuple[int, ...]]] = []

    async def delete_messages(chat_id: int, message_ids: tuple[int, ...]) -> None:
        deleted.append((chat_id, message_ids))
        delete_started.set()
        await allow_delete.wait()

    tracker = service.output_tracker(-100, 3, delete_messages)
    track_task = asyncio.create_task(tracker.track([30, 31]))
    await delete_started.wait()

    run = await asyncio.wait_for(service.get_run(-100), timeout=0.1)
    assert run is not None
    allow_delete.set()
    registration = await track_task

    assert registration.delete_immediately is True
    assert deleted == [(-100, (30, 31))]


@pytest.mark.asyncio
async def test_task_attached_after_eviction_is_cancelled() -> None:
    service = BurstGuardService(rng=FixedRandom(0))
    for message_id in range(1, 4):
        await service.register_candidate(-100, 10, message_id)

    async def wait_forever() -> None:
        await asyncio.Event().wait()

    task = asyncio.create_task(wait_forever())
    should_continue = await service.attach_task(-100, 3, task)

    assert should_continue is False
    with pytest.raises(asyncio.CancelledError):
        await task
    await service.detach_task(-100, 3, task)


@pytest.mark.asyncio
async def test_completed_evicted_item_is_released() -> None:
    service = BurstGuardService(rng=FixedRandom(0))
    for message_id in range(1, 4):
        await service.register_candidate(-100, 10, message_id)

    await service.finish_processing(-100, 3)
    registration = await service.record_output(-100, 3, [30])

    assert registration.tracked is False
