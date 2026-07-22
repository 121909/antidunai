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
async def test_y_candidates_trigger_inside_latest_x_window() -> None:
    random = FixedRandom([1])
    service = BurstStateService(3, 60, group_size=5, random_source=random)

    first = await send(service, 1, urls=frozenset({"youtube.com/1"}))
    second = await send(service, 2, urls=frozenset({"youtube.com/2"}))
    await send(service, 3, candidate=False)
    triggered = await send(service, 4, urls=frozenset({"youtube.com/4"}))

    assert first.cleanup is None
    assert second.cleanup is None
    assert triggered.message_count == 4
    assert triggered.candidate_count == 3
    assert triggered.retained_message_id == 2
    assert triggered.cleanup is not None
    assert triggered.cleanup.message_ids == (1, 4)
    assert random.stops == [3]


@pytest.mark.asyncio
async def test_window_only_keeps_latest_x_messages_for_one_target() -> None:
    random = FixedRandom([0])
    service = BurstStateService(2, 60, group_size=3, random_source=random)

    await send(service, 1, urls=frozenset({"youtube.com/1"}))
    await send(service, 2, candidate=False)
    await send(service, 3, candidate=False)
    not_triggered = await send(service, 4, urls=frozenset({"youtube.com/4"}))
    triggered = await send(service, 5, urls=frozenset({"youtube.com/5"}))

    assert not_triggered.cleanup is None
    assert not_triggered.candidate_count == 1
    assert triggered.message_count == 3
    assert triggered.cleanup is not None
    assert triggered.cleanup.message_ids == (5,)
    assert triggered.retained_message_id == 4


@pytest.mark.asyncio
async def test_each_target_user_has_an_independent_window() -> None:
    random = FixedRandom([0])
    service = BurstStateService(2, 60, group_size=3, random_source=random)

    await send(service, 1, target="id:10", urls=frozenset({"youtube.com/1"}))
    other = await send(service, 2, target="id:20", urls=frozenset({"youtube.com/2"}))
    target = await send(service, 3, target="id:10", urls=frozenset({"youtube.com/3"}))

    assert other.cleanup is None
    assert target.cleanup is not None
    assert target.cleanup.message_ids == (3,)
    assert set(service.chat_state(-1).target_windows) == {"id:10", "id:20"}


@pytest.mark.asyncio
async def test_retained_candidate_participates_in_later_window_selection() -> None:
    service = BurstStateService(3, 60, group_size=5, random_source=FixedRandom([0, 2]))
    link_1 = frozenset({"youtube.com/1"})

    await send(service, 1, urls=link_1)
    await send(service, 2, urls=frozenset({"youtube.com/2"}))
    first = await send(service, 3, urls=frozenset({"youtube.com/3"}))
    await service.process_parser_output(chat_id=-1, message_id=101, url_keys=link_1)
    await send(service, 4, urls=frozenset({"youtube.com/4"}))
    second = await send(service, 5, urls=frozenset({"youtube.com/5"}))

    assert first.cleanup is not None
    assert first.cleanup.message_ids == (2, 3)
    assert second.cleanup is not None
    assert second.cleanup.message_ids == (1, 4, 101)
    assert second.retained_message_id == 5


@pytest.mark.asyncio
async def test_non_target_messages_do_not_enter_target_window() -> None:
    service = BurstStateService(2, 60, group_size=3, random_source=FixedRandom([0]))

    await send(service, 1, urls=frozenset({"youtube.com/1"}))
    await send(service, 2, target=None, candidate=False)
    await send(service, 3, target=None, candidate=False)
    result = await send(service, 4, candidate=False)
    candidate = await send(service, 5, urls=frozenset({"youtube.com/5"}))

    assert result.message_count == 2
    assert candidate.message_count == 3
    assert candidate.cleanup is not None
    assert candidate.cleanup.message_ids == (5,)


@pytest.mark.asyncio
async def test_duplicate_updates_are_idempotent() -> None:
    service = BurstStateService(2, 60, group_size=3, random_source=FixedRandom([0]))
    first = await send(service, 1, urls=frozenset({"youtube.com/1"}))
    duplicate = await send(service, 1, urls=frozenset({"youtube.com/1"}))

    assert first.run_started
    assert duplicate.duplicate
    assert service.chat_state(-1).target_windows["id:10"].message_count == 1


@pytest.mark.asyncio
async def test_y_may_be_one_and_keeps_one_of_multiple_candidates() -> None:
    service = BurstStateService(1, 60, group_size=5, random_source=FixedRandom([0, 1]))

    await send(service, 1, urls=frozenset({"youtube.com/1"}))
    result = await send(service, 2, urls=frozenset({"youtube.com/2"}))

    assert result.cleanup is not None
    assert result.cleanup.message_ids == (1,)
    assert result.retained_message_id == 2


@pytest.mark.asyncio
async def test_parser_outputs_before_and_after_eviction_are_deleted() -> None:
    service = BurstStateService(3, 60, group_size=5, random_source=FixedRandom([0]))
    link_a = frozenset({"youtube.com/watch?v=a"})
    link_b = frozenset({"youtube.com/watch?v=b"})
    link_c = frozenset({"youtube.com/watch?v=c"})

    await send(service, 1, urls=link_a)
    retained_output = await service.process_parser_output(
        chat_id=-1, message_id=101, url_keys=link_a
    )
    await send(service, 2, urls=link_b)
    await service.process_parser_output(chat_id=-1, message_id=102, url_keys=link_b)
    triggered = await send(service, 3, urls=link_c)

    assert retained_output.cleanup is None
    assert triggered.cleanup is not None
    assert triggered.cleanup.message_ids == (2, 3, 102)

    late_discarded = await service.process_parser_output(
        chat_id=-1, message_id=103, url_keys=link_c
    )
    late_retained = await service.process_parser_output(chat_id=-1, message_id=104, url_keys=link_a)
    assert late_discarded.cleanup is not None
    assert late_discarded.cleanup.message_ids == (103,)
    assert late_retained.cleanup is None


@pytest.mark.asyncio
async def test_shared_url_with_retained_candidate_is_never_link_deleted() -> None:
    service = BurstStateService(3, 60, group_size=5, random_source=FixedRandom([0]))
    shared = frozenset({"youtube.com/watch?v=same"})

    await send(service, 1, urls=shared)
    await service.process_parser_output(chat_id=-1, message_id=101, url_keys=shared)
    await send(service, 2, urls=shared)
    triggered = await send(service, 3, urls=shared)
    late = await service.process_parser_output(chat_id=-1, message_id=102, url_keys=shared)

    assert triggered.cleanup is not None
    assert triggered.cleanup.message_ids == (2, 3)
    assert late.cleanup is None


@pytest.mark.asyncio
async def test_unrelated_parser_url_is_not_remembered_or_deleted() -> None:
    service = BurstStateService(3, 60, group_size=5, random_source=FixedRandom([0]))
    active = frozenset({"youtube.com/watch?v=active"})
    unrelated = frozenset({"youtube.com/watch?v=other"})
    await send(service, 1, urls=active)

    output = await service.process_parser_output(chat_id=-1, message_id=101, url_keys=unrelated)

    assert output.cleanup is None
    assert service.chat_state(-1).pending_parser_messages == {}


@pytest.mark.asyncio
async def test_parser_message_with_retained_and_discarded_links_is_protected() -> None:
    service = BurstStateService(3, 60, group_size=5, random_source=FixedRandom([0]))
    link_a = frozenset({"youtube.com/watch?v=a"})
    link_b = frozenset({"youtube.com/watch?v=b"})

    await send(service, 1, urls=link_a)
    await send(service, 2, urls=link_b)
    combined = await service.process_parser_output(
        chat_id=-1, message_id=101, url_keys=link_a | link_b
    )
    triggered = await send(service, 3, urls=frozenset({"youtube.com/watch?v=c"}))
    late_combined = await service.process_parser_output(
        chat_id=-1, message_id=102, url_keys=link_a | link_b
    )

    assert combined.cleanup is None
    assert triggered.cleanup is not None
    assert triggered.cleanup.message_ids == (2, 3)
    assert late_combined.cleanup is None


@pytest.mark.asyncio
async def test_old_candidate_falling_out_of_window_is_protected() -> None:
    service = BurstStateService(2, 60, group_size=2, random_source=FixedRandom([0]))
    link_a = frozenset({"youtube.com/watch?v=a"})

    await send(service, 1, urls=link_a)
    await service.process_parser_output(chat_id=-1, message_id=101, url_keys=link_a)
    await send(service, 2, candidate=False)
    await send(service, 3, candidate=False)
    late = await service.process_parser_output(chat_id=-1, message_id=102, url_keys=link_a)

    assert late.cleanup is None
