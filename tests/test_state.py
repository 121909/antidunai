from __future__ import annotations

from collections.abc import Iterator

import pytest

from burst_guard.state import BurstStateService


class FixedRandom:
    def __init__(self, values: list[int]) -> None:
        self._values: Iterator[int] = iter(values)
        self.stops: list[int] = []

    def randrange(self, stop: int) -> int:
        self.stops.append(stop)
        value = next(self._values)
        assert 0 <= value < stop
        return value


async def send(
    service: BurstStateService,
    message_id: int,
    *,
    chat_id: int = -1,
    target: str | None = "id:10",
    candidate: bool = True,
):
    return await service.process(
        chat_id=chat_id,
        message_id=message_id,
        target_key=target,
        is_candidate=candidate,
    )


@pytest.mark.asyncio
async def test_threshold_keeps_exactly_one_candidate() -> None:
    random = FixedRandom([1])
    service = BurstStateService(3, 60, random_source=random, run_id_factory=lambda: "run")

    assert (await send(service, 1)).cleanup is None
    assert (await send(service, 2)).cleanup is None
    third = await send(service, 3)

    assert third.retained_message_id == 2
    assert third.cleanup is not None
    assert third.cleanup.message_ids == (1, 3)
    assert random.stops == [3]
    state = service.chat_state(-1)
    assert state.active_run is not None
    assert state.active_run.warmup_message_ids == []


@pytest.mark.asyncio
async def test_reservoir_rejects_or_replaces_new_candidates() -> None:
    random = FixedRandom([0, 3, 0])
    service = BurstStateService(3, 60, random_source=random)
    await send(service, 1)
    await send(service, 2)
    third = await send(service, 3)
    fourth = await send(service, 4)
    fifth = await send(service, 5)

    assert third.retained_message_id == 1
    assert fourth.cleanup is not None and fourth.cleanup.message_ids == (4,)
    assert fourth.retained_message_id == 1
    assert fifth.cleanup is not None and fifth.cleanup.message_ids == (1,)
    assert fifth.retained_message_id == 5
    assert random.stops == [3, 4, 5]


@pytest.mark.asyncio
async def test_interruption_and_target_switch_rules() -> None:
    service = BurstStateService(3, 60, random_source=FixedRandom([]))
    await send(service, 1)

    same_target_text = await send(service, 2, candidate=False)
    assert not same_target_text.run_closed
    assert service.chat_state(-1).active_run is not None

    other_target_text = await send(service, 3, target="id:20", candidate=False)
    assert other_target_text.run_closed
    assert service.chat_state(-1).active_run is None

    await send(service, 4)
    human = await send(service, 5, target=None, candidate=False)
    assert human.run_closed
    assert service.chat_state(-1).active_run is None


@pytest.mark.asyncio
async def test_groups_are_isolated_and_duplicates_are_idempotent() -> None:
    service = BurstStateService(3, 60, random_source=FixedRandom([]))
    first = await send(service, 1, chat_id=-1)
    await send(service, 1, chat_id=-2)
    duplicate = await send(service, 1, chat_id=-1)

    assert first.run_started
    assert duplicate.duplicate
    assert service.chat_state(-1).active_run is not None
    assert service.chat_state(-2).active_run is not None
    assert service.chat_state(-1).active_run is not service.chat_state(-2).active_run


@pytest.mark.asyncio
async def test_seen_ids_expire_and_idle_state_is_removed() -> None:
    now = 100.0

    def clock() -> float:
        return now

    service = BurstStateService(3, 10, random_source=FixedRandom([]), clock=clock)
    await send(service, 1, target=None, candidate=False)
    assert (await send(service, 1, target=None, candidate=False)).duplicate

    now = 111.0
    assert await service.cleanup_expired() == 1
    assert not (await send(service, 1, target=None, candidate=False)).duplicate
