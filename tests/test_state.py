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
    urls: frozenset[str] = frozenset(),
):
    return await service.process(
        chat_id=chat_id,
        message_id=message_id,
        target_key=target,
        is_candidate=candidate,
        candidate_url_keys=urls,
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
    assert state.active_run is None
    assert third.run_closed
    assert third.message_count == 3


@pytest.mark.asyncio
async def test_group_waits_for_x_messages_and_selects_from_all_candidates() -> None:
    random = FixedRandom([1])
    service = BurstStateService(2, 60, group_size=5, random_source=random)

    await send(service, 1, urls=frozenset({"youtube.com/1"}))
    threshold_reached = await send(service, 2, urls=frozenset({"youtube.com/2"}))
    await send(service, 3, target=None, candidate=False)
    await send(service, 4, urls=frozenset({"youtube.com/4"}))
    group_end = await send(service, 5, target=None, candidate=False)

    assert threshold_reached.cleanup is None
    assert not threshold_reached.run_closed
    assert group_end.cleanup is not None
    assert group_end.cleanup.message_ids == (1, 4)
    assert group_end.retained_message_id == 2
    assert random.stops == [3]


@pytest.mark.asyncio
async def test_group_below_y_keeps_every_candidate() -> None:
    service = BurstStateService(3, 60, group_size=4, random_source=FixedRandom([]))

    await send(service, 1, urls=frozenset({"youtube.com/1"}))
    await send(service, 2, target=None, candidate=False)
    await send(service, 3, urls=frozenset({"youtube.com/3"}))
    group_end = await send(service, 4, target=None, candidate=False)

    assert group_end.run_closed
    assert group_end.candidate_count == 2
    assert group_end.cleanup is None


@pytest.mark.asyncio
async def test_y_may_be_one() -> None:
    service = BurstStateService(1, 60, group_size=3, random_source=FixedRandom([1]))

    await send(service, 1, urls=frozenset({"youtube.com/1"}))
    await send(service, 2, urls=frozenset({"youtube.com/2"}))
    group_end = await send(service, 3, target=None, candidate=False)

    assert group_end.cleanup is not None
    assert group_end.cleanup.message_ids == (1,)
    assert group_end.retained_message_id == 2


@pytest.mark.asyncio
async def test_target_switch_and_other_humans_do_not_reset_fixed_group() -> None:
    service = BurstStateService(2, 60, group_size=4, random_source=FixedRandom([0]))
    await send(service, 1, urls=frozenset({"youtube.com/1"}))
    same_target_text = await send(service, 2, candidate=False)
    other_target = await send(
        service,
        3,
        target="id:20",
        urls=frozenset({"youtube.com/3"}),
    )
    human = await send(service, 4, target=None, candidate=False)

    assert not same_target_text.run_closed
    assert not other_target.run_closed
    assert human.run_closed
    assert human.cleanup is not None and human.cleanup.message_ids == (3,)
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

    service = BurstStateService(1, 10, group_size=1, random_source=FixedRandom([]), clock=clock)
    await send(service, 1, target=None, candidate=False)
    assert (await send(service, 1, target=None, candidate=False)).duplicate

    now = 111.0
    assert await service.cleanup_expired() == 1
    assert not (await send(service, 1, target=None, candidate=False)).duplicate


@pytest.mark.asyncio
async def test_parser_outputs_before_and_after_eviction_are_deleted() -> None:
    service = BurstStateService(3, 60, random_source=FixedRandom([0]))
    link_a = frozenset({"youtube.com/watch?v=a"})
    link_b = frozenset({"youtube.com/watch?v=b"})
    link_c = frozenset({"youtube.com/watch?v=c"})

    await send(service, 1, urls=link_a)
    retained_output = await service.process_parser_output(
        chat_id=-1, message_id=101, url_keys=link_a
    )
    await send(service, 2, urls=link_b)
    await service.process_parser_output(chat_id=-1, message_id=102, url_keys=link_b)
    third = await send(service, 3, urls=link_c)

    assert retained_output.cleanup is None
    assert third.cleanup is not None
    assert third.cleanup.message_ids == (2, 3, 102)

    late_discarded = await service.process_parser_output(
        chat_id=-1, message_id=103, url_keys=link_c
    )
    late_retained = await service.process_parser_output(chat_id=-1, message_id=104, url_keys=link_a)
    assert late_discarded.cleanup is not None
    assert late_discarded.cleanup.message_ids == (103,)
    assert late_retained.cleanup is None
    assert service.chat_state(-1).active_run is None


@pytest.mark.asyncio
async def test_shared_url_with_retained_candidate_is_never_link_deleted() -> None:
    service = BurstStateService(3, 60, random_source=FixedRandom([0]))
    shared = frozenset({"youtube.com/watch?v=same"})

    await send(service, 1, urls=shared)
    await service.process_parser_output(chat_id=-1, message_id=101, url_keys=shared)
    await send(service, 2, urls=shared)
    third = await send(service, 3, urls=shared)
    late = await service.process_parser_output(chat_id=-1, message_id=102, url_keys=shared)

    assert third.cleanup is not None
    assert third.cleanup.message_ids == (2, 3)
    assert late.cleanup is None


@pytest.mark.asyncio
async def test_unrelated_parser_url_is_not_remembered_or_deleted() -> None:
    service = BurstStateService(3, 60, random_source=FixedRandom([0]))
    active = frozenset({"youtube.com/watch?v=active"})
    unrelated = frozenset({"youtube.com/watch?v=other"})
    await send(service, 1, urls=active)

    output = await service.process_parser_output(chat_id=-1, message_id=101, url_keys=unrelated)

    assert output.cleanup is None
    assert service.chat_state(-1).pending_parser_messages == {}


@pytest.mark.asyncio
async def test_parser_message_with_retained_and_discarded_links_is_protected() -> None:
    service = BurstStateService(3, 60, random_source=FixedRandom([0]))
    link_a = frozenset({"youtube.com/watch?v=a"})
    link_b = frozenset({"youtube.com/watch?v=b"})

    await send(service, 1, urls=link_a)
    await send(service, 2, urls=link_b)
    combined = await service.process_parser_output(
        chat_id=-1, message_id=101, url_keys=link_a | link_b
    )
    third = await send(service, 3, urls=frozenset({"youtube.com/watch?v=c"}))
    late_combined = await service.process_parser_output(
        chat_id=-1, message_id=102, url_keys=link_a | link_b
    )

    assert combined.cleanup is None
    assert third.cleanup is not None
    assert third.cleanup.message_ids == (2, 3)
    assert late_combined.cleanup is None


@pytest.mark.asyncio
async def test_previous_group_retained_link_protects_ambiguous_output() -> None:
    service = BurstStateService(3, 60, random_source=FixedRandom([0, 1]))
    link_a = frozenset({"youtube.com/watch?v=a"})
    await send(service, 1, urls=link_a)
    await service.process_parser_output(chat_id=-1, message_id=101, url_keys=link_a)
    await send(service, 2, urls=frozenset({"youtube.com/watch?v=b"}))
    await send(service, 3, urls=frozenset({"youtube.com/watch?v=c"}))

    await send(service, 4, urls=link_a)
    await send(service, 5, urls=frozenset({"youtube.com/watch?v=d"}))
    second_group = await send(service, 6, urls=frozenset({"youtube.com/watch?v=e"}))
    ambiguous_output = await service.process_parser_output(
        chat_id=-1, message_id=102, url_keys=link_a
    )

    assert second_group.cleanup is not None
    assert second_group.cleanup.message_ids == (4, 6)
    assert ambiguous_output.cleanup is None
