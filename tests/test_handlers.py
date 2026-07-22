from __future__ import annotations

import logging
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import pytest

from burst_guard.config import Settings
from burst_guard.handlers import TelegramUpdateHandler, to_incoming_message
from burst_guard.metrics import Metrics
from burst_guard.models import CleanupReport, CleanupRequest
from burst_guard.state import BurstStateService


class ZeroRandom:
    def randrange(self, stop: int) -> int:
        return 0


class FakeMessage:
    def __init__(
        self,
        message_id: int,
        *,
        video: bool = False,
        text: str = "",
        out: bool = False,
        action: object | None = None,
        hidden_url: str | None = None,
    ) -> None:
        self.id = message_id
        self.video = object() if video else None
        self.video_note = None
        self.document = None
        self.message = text
        self.out = out
        self.action = action
        self.entities = [SimpleNamespace(url=hidden_url)] if hidden_url else []

    def get_entities_text(self) -> list[tuple[object, str]]:
        return []


class FakeEvent:
    def __init__(
        self,
        message_id: int,
        sender_id: int | None,
        *,
        chat_id: int = -1001,
        username: str | None = None,
        bot: bool = False,
        is_group: bool = True,
        **message_options: Any,
    ) -> None:
        self.chat_id = chat_id
        self.sender_id = sender_id
        self.is_group = is_group
        self.message = FakeMessage(message_id, **message_options)
        self._sender = SimpleNamespace(username=username, bot=bot)

    async def get_sender(self) -> object:
        return self._sender


class FakeCleanup:
    def __init__(self, state: BurstStateService) -> None:
        self.state = state
        self.requests: list[CleanupRequest] = []

    async def execute(self, request: CleanupRequest) -> CleanupReport:
        assert not self.state.chat_state(request.chat_id).lock.locked()
        self.requests.append(request)
        return CleanupReport(planned=len(request.message_ids))


def make_handler(
    settings: Settings,
) -> tuple[TelegramUpdateHandler, BurstStateService, FakeCleanup, Metrics]:
    state = BurstStateService(
        settings.threshold,
        60,
        group_size=settings.group_size,
        random_source=ZeroRandom(),
    )
    cleanup = FakeCleanup(state)
    metrics = Metrics()
    handler = TelegramUpdateHandler(
        settings,
        state,
        cleanup,
        metrics,
        logging.getLogger("test-handler"),  # type: ignore[arg-type]
    )
    return handler, state, cleanup, metrics


@pytest.mark.asyncio
async def test_fixed_group_link_cleanup_occurs_outside_state_lock(
    settings_factory: Callable[..., Settings],
) -> None:
    handler, state, cleanup, metrics = make_handler(settings_factory(group_size=3))

    await handler(FakeEvent(1, 101, text="https://youtube.com/1"))
    await handler(FakeEvent(2, 101, text="https://youtube.com/2"))
    await handler(FakeEvent(9, 999, video=True, bot=True))
    await handler(FakeEvent(3, 101, text="https://youtube.com/3"))

    assert len(cleanup.requests) == 1
    assert cleanup.requests[0].message_ids == (2, 3)
    assert metrics.get("candidate_messages") == 3
    assert metrics.get("groups_started") == 1
    assert state.chat_state(-1001).active_run is None


@pytest.mark.asyncio
async def test_target_video_file_counts_as_message_but_not_link_candidate(
    settings_factory: Callable[..., Settings],
) -> None:
    handler, state, cleanup, metrics = make_handler(settings_factory(group_size=2, threshold=1))

    await handler(FakeEvent(1, 101, video=True))
    await handler(FakeEvent(2, 999, text="ordinary"))

    assert cleanup.requests == []
    assert metrics.get("candidate_messages") == 0
    assert state.chat_state(-1001).active_run is None


@pytest.mark.asyncio
async def test_non_target_link_counts_as_message_but_not_candidate(
    settings_factory: Callable[..., Settings],
) -> None:
    handler, _, cleanup, metrics = make_handler(settings_factory(group_size=3, threshold=2))

    await handler(FakeEvent(1, 101, text="https://youtube.com/1"))
    await handler(FakeEvent(2, 999, text="https://youtube.com/not-target"))
    await handler(FakeEvent(3, 101, text="https://youtube.com/3"))

    assert [request.message_ids for request in cleanup.requests] == [(3,)]
    assert metrics.get("candidate_messages") == 2


@pytest.mark.asyncio
async def test_every_non_bot_message_occupies_one_group_position(
    settings_factory: Callable[..., Settings],
) -> None:
    handler, state, _, _ = make_handler(settings_factory(group_size=3))

    await handler(FakeEvent(1, 101, text="https://youtube.com/1"))
    await handler(FakeEvent(2, 101, text="ordinary"))
    assert state.chat_state(-1001).active_run is not None

    await handler(FakeEvent(3, None, text="anonymous admin"))
    assert state.chat_state(-1001).active_run is None


@pytest.mark.asyncio
async def test_filters_disabled_wrong_chat_private_service_own_and_bot(
    settings_factory: Callable[..., Settings],
) -> None:
    handler, state, _, metrics = make_handler(settings_factory())

    await handler(FakeEvent(1, 101, chat_id=-9999, video=True))
    await handler(FakeEvent(2, 101, is_group=False, video=True))
    await handler(FakeEvent(3, 101, action=object(), video=True))
    await handler(FakeEvent(4, 101, out=True, video=True))
    await handler(FakeEvent(5, 101, bot=True, video=True))

    assert metrics.snapshot() == {}
    assert state.chat_state(-1001).active_run is None

    disabled, _, _, disabled_metrics = make_handler(settings_factory(enabled=False))
    await disabled(FakeEvent(6, 101, video=True))
    assert disabled_metrics.snapshot() == {}


@pytest.mark.asyncio
async def test_duplicate_update_is_not_counted_twice(
    settings_factory: Callable[..., Settings],
) -> None:
    handler, _, _, metrics = make_handler(settings_factory())
    event = FakeEvent(1, 101, text="https://youtube.com/1")

    await handler(event)
    await handler(event)

    assert metrics.get("candidate_messages") == 1
    assert metrics.get("duplicate_updates") == 1


def test_hidden_entity_url_is_adapted() -> None:
    event = FakeEvent(1, 101, hidden_url="https://youtube.com/hidden")

    incoming = to_incoming_message(event, SimpleNamespace(username="name"))

    assert incoming.entity_urls == ("https://youtube.com/hidden",)


@pytest.mark.asyncio
async def test_late_parser_video_with_discarded_original_url_is_deleted(
    settings_factory: Callable[..., Settings],
) -> None:
    handler, _, cleanup, metrics = make_handler(settings_factory(group_size=3))

    await handler(FakeEvent(1, 101, text="https://youtube.com/watch?v=1"))
    await handler(FakeEvent(2, 101, text="https://youtube.com/watch?v=2"))
    await handler(FakeEvent(3, 101, text="https://youtube.com/watch?v=3"))
    await handler(
        FakeEvent(
            20,
            500,
            bot=True,
            video=True,
            text="原链接: HTTP://YOUTUBE.COM/watch?v=2#result",
        )
    )
    await handler(
        FakeEvent(
            21,
            500,
            bot=True,
            video=True,
            text="原链接: https://youtube.com/watch?v=1",
        )
    )

    assert [request.message_ids for request in cleanup.requests] == [(2, 3), (20,)]
    assert metrics.get("parser_outputs_received") == 2
    assert metrics.get("parser_outputs_matched") == 1


@pytest.mark.asyncio
async def test_parser_video_arriving_before_threshold_is_deleted_with_source(
    settings_factory: Callable[..., Settings],
) -> None:
    handler, state, cleanup, _ = make_handler(settings_factory(group_size=3))

    await handler(FakeEvent(1, 101, text="https://youtube.com/watch?v=1"))
    await handler(FakeEvent(2, 101, text="https://youtube.com/watch?v=2"))
    await handler(
        FakeEvent(
            20,
            500,
            bot=True,
            video=True,
            text="https://youtube.com/watch?v=2",
        )
    )
    assert state.chat_state(-1001).active_run is not None
    await handler(FakeEvent(3, 101, text="https://youtube.com/watch?v=3"))

    assert [request.message_ids for request in cleanup.requests] == [(2, 3, 20)]


@pytest.mark.asyncio
async def test_bot_text_without_video_is_not_deleted(
    settings_factory: Callable[..., Settings],
) -> None:
    handler, _, cleanup, metrics = make_handler(settings_factory(group_size=3))
    await handler(FakeEvent(1, 101, text="https://youtube.com/watch?v=1"))
    await handler(FakeEvent(2, 101, text="https://youtube.com/watch?v=2"))
    await handler(FakeEvent(3, 101, text="https://youtube.com/watch?v=3"))

    await handler(FakeEvent(20, 500, bot=True, text="https://youtube.com/watch?v=2"))

    assert [request.message_ids for request in cleanup.requests] == [(2, 3)]
    assert metrics.get("parser_outputs_received") == 0
