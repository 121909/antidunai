from __future__ import annotations

import logging
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import pytest

from burst_guard.config import Settings
from burst_guard.handlers import TelegramUpdateHandler, to_incoming_message
from burst_guard.interaction import INFO_REPLY_TEXT, InfoReplyService
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
        button_url: str | None = None,
        preview_url: str | None = None,
        reply_to_message_id: int | None = None,
        gif: bool = False,
        sticker: bool = False,
        document_mime_type: str | None = None,
        mention: str | None = None,
    ) -> None:
        self.id = message_id
        self.video = object() if video else None
        self.video_note = None
        self.gif = object() if gif else None
        self.sticker = object() if sticker else None
        self.document = (
            SimpleNamespace(mime_type=document_mime_type) if document_mime_type else None
        )
        self.message = text
        self.out = out
        self.action = action
        self._mention_entity = None
        if mention is not None:
            self._mention_entity = type("MessageEntityMention", (), {})()
        self.entities = [SimpleNamespace(url=hidden_url)] if hidden_url else []
        if self._mention_entity is not None:
            self.entities.append(self._mention_entity)
        self._mention = mention
        self.reply_markup = (
            SimpleNamespace(rows=[SimpleNamespace(buttons=[SimpleNamespace(url=button_url)])])
            if button_url
            else None
        )
        self.web_preview = (
            SimpleNamespace(url=preview_url, display_url=None) if preview_url else None
        )
        self.reply_to_msg_id = reply_to_message_id

    def get_entities_text(self) -> list[tuple[object, str]]:
        if self._mention_entity is not None and self._mention is not None:
            return [(self._mention_entity, self._mention)]
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


class SuccessfulCleanup(FakeCleanup):
    async def execute(self, request: CleanupRequest) -> CleanupReport:
        await super().execute(request)
        return CleanupReport(
            planned=len(request.message_ids),
            deleted=len(request.message_ids),
        )


class FakeNotifier:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def notify(self, **values: object) -> bool:
        self.calls.append(values)
        return True


class FakeReplySender:
    def __init__(self) -> None:
        self.calls: list[tuple[int, str, int]] = []

    async def __call__(self, chat_id: int, text: str, reply_to: int) -> object:
        self.calls.append((chat_id, text, reply_to))
        return SimpleNamespace(id=800 + len(self.calls))


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
async def test_rolling_window_cleanup_occurs_outside_state_lock(
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
    assert metrics.get("target_windows_started") == 1
    assert "id:101" in state.chat_state(-1001).target_windows


@pytest.mark.asyncio
async def test_target_video_file_counts_as_candidate(
    settings_factory: Callable[..., Settings],
) -> None:
    handler, state, cleanup, metrics = make_handler(settings_factory(group_size=3, threshold=2))

    await handler(FakeEvent(1, 101, video=True))
    await handler(FakeEvent(2, 999, text="ordinary"))
    await handler(FakeEvent(3, 101, text="https://youtube.com/3"))

    assert [request.message_ids for request in cleanup.requests] == [(3,)]
    assert metrics.get("candidate_messages") == 2
    assert "id:101" in state.chat_state(-1001).target_windows


@pytest.mark.asyncio
async def test_non_target_link_does_not_enter_target_window(
    settings_factory: Callable[..., Settings],
) -> None:
    handler, _, cleanup, metrics = make_handler(settings_factory(group_size=3, threshold=2))

    await handler(FakeEvent(1, 101, text="https://youtube.com/1"))
    await handler(FakeEvent(2, 999, text="https://youtube.com/not-target"))
    await handler(FakeEvent(3, 101, text="https://youtube.com/3"))

    assert [request.message_ids for request in cleanup.requests] == [(3,)]
    assert metrics.get("candidate_messages") == 2


@pytest.mark.asyncio
async def test_only_target_messages_enter_target_window(
    settings_factory: Callable[..., Settings],
) -> None:
    handler, state, _, _ = make_handler(settings_factory(group_size=3))

    await handler(FakeEvent(1, 101, text="https://youtube.com/1"))
    await handler(FakeEvent(2, 101, text="ordinary"))
    await handler(FakeEvent(3, None, text="anonymous admin"))

    window = state.chat_state(-1001).target_windows["id:101"]
    assert window.window_message_ids == [1, 2]


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
    assert state.chat_state(-1001).target_windows == {}

    disabled, _, _, disabled_metrics = make_handler(settings_factory(enabled=False))
    await disabled(FakeEvent(6, 101, video=True))
    assert disabled_metrics.snapshot() == {}


@pytest.mark.asyncio
async def test_handler_replies_with_fixed_text_when_account_is_mentioned(
    settings_factory: Callable[..., Settings],
) -> None:
    settings = settings_factory()
    state = BurstStateService(settings.threshold, 60, group_size=settings.group_size)
    cleanup = FakeCleanup(state)
    metrics = Metrics()
    reply_sender = FakeReplySender()
    handler = TelegramUpdateHandler(
        settings,
        state,
        cleanup,
        metrics,
        logging.getLogger("test-handler"),
        info_reply=InfoReplyService(9001, "guard_user", metrics, logging.getLogger("test-info")),
        reply_sender=reply_sender,
    )

    await handler(FakeEvent(7, 999, text="@guard_user", mention="@guard_user"))

    assert reply_sender.calls == [(-1001, INFO_REPLY_TEXT, 7)]
    assert metrics.get("info_reply_sent") == 1


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


def test_button_and_preview_urls_are_adapted() -> None:
    event = FakeEvent(
        1,
        101,
        button_url="https://youtube.com/button",
        preview_url="https://youtube.com/preview",
    )

    incoming = to_incoming_message(event, SimpleNamespace(username="name"))

    assert incoming.entity_urls == (
        "https://youtube.com/button",
        "https://youtube.com/preview",
    )


def test_reply_message_id_is_adapted() -> None:
    event = FakeEvent(1, 101, reply_to_message_id=42)

    incoming = to_incoming_message(event, SimpleNamespace(username="name"))

    assert incoming.reply_to_message_id == 42


def test_animation_and_sticker_attributes_are_adapted() -> None:
    gif_event = FakeEvent(1, 101, video=True, gif=True, document_mime_type="video/mp4")
    sticker_event = FakeEvent(2, 101, sticker=True, document_mime_type="video/webm")

    gif = to_incoming_message(gif_event, SimpleNamespace(username="name"))
    sticker = to_incoming_message(sticker_event, SimpleNamespace(username="name"))

    assert gif.has_video is True
    assert gif.is_animated is True
    assert sticker.is_sticker is True


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
async def test_non_bot_parser_account_with_source_hyperlink_is_deleted(
    settings_factory: Callable[..., Settings],
) -> None:
    handler, _, cleanup, metrics = make_handler(settings_factory(group_size=5))

    await handler(FakeEvent(1, 101, text="https://youtube.com/watch?v=1"))
    await handler(FakeEvent(2, 101, text="https://youtube.com/watch?v=2"))
    await handler(FakeEvent(3, 101, text="https://youtube.com/watch?v=3"))
    await handler(
        FakeEvent(
            20,
            500,
            video=True,
            text="Source",
            hidden_url="https://youtube.com/watch?v=2",
        )
    )

    assert [request.message_ids for request in cleanup.requests] == [(2, 3), (20,)]
    assert metrics.get("parser_outputs_received") == 1
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
    assert "id:101" in state.chat_state(-1001).target_windows
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


@pytest.mark.asyncio
async def test_configured_parser_sender_videos_follow_link_order(
    settings_factory: Callable[..., Settings],
) -> None:
    handler, _, cleanup, metrics = make_handler(
        settings_factory(group_size=5, parser_sender_ids="7947627028")
    )

    await handler(FakeEvent(1, 101, text="https://youtube.com/watch?v=1"))
    await handler(FakeEvent(2, 101, text="https://youtube.com/watch?v=2"))
    await handler(FakeEvent(3, 101, text="https://youtube.com/watch?v=3"))
    await handler(FakeEvent(20, 7947627028, bot=True, video=True))
    await handler(FakeEvent(21, 7947627028, bot=True, video=True))
    await handler(FakeEvent(22, 7947627028, bot=True, video=True))

    assert [request.message_ids for request in cleanup.requests] == [(2, 3), (21,), (22,)]
    assert metrics.get("parser_outputs_received") == 3
    assert metrics.get("parser_outputs_matched") == 2


@pytest.mark.asyncio
async def test_parser_reply_takes_priority_over_sender_order(
    settings_factory: Callable[..., Settings],
) -> None:
    handler, _, cleanup, _ = make_handler(
        settings_factory(group_size=5, parser_sender_ids="7947627028")
    )

    await handler(FakeEvent(1, 101, text="https://youtube.com/watch?v=1"))
    await handler(FakeEvent(2, 101, text="https://youtube.com/watch?v=2"))
    await handler(FakeEvent(3, 101, text="https://youtube.com/watch?v=3"))
    await handler(FakeEvent(20, 7947627028, video=True, reply_to_message_id=2))
    await handler(FakeEvent(21, 7947627028, video=True))

    assert [request.message_ids for request in cleanup.requests] == [(2, 3), (20,)]


@pytest.mark.asyncio
async def test_parser_source_link_takes_priority_and_normalizes_share_parameters(
    settings_factory: Callable[..., Settings],
) -> None:
    handler, _, cleanup, _ = make_handler(
        settings_factory(group_size=5, parser_sender_ids="7947627028")
    )

    await handler(
        FakeEvent(
            1,
            101,
            text="https://www.bilibili.com/video/BV1abc?spm_id_from=333&vd_source=x",
        )
    )
    await handler(FakeEvent(2, 101, text="https://youtube.com/watch?v=2"))
    await handler(FakeEvent(3, 101, text="https://youtube.com/watch?v=3"))
    await handler(
        FakeEvent(
            20,
            7947627028,
            video=True,
            text="Source",
            hidden_url="https://m.bilibili.com/video/BV1abc?p=1",
        )
    )
    await handler(FakeEvent(21, 7947627028, video=True))

    assert [request.message_ids for request in cleanup.requests] == [(2, 3), (21,)]


@pytest.mark.asyncio
async def test_successful_threshold_cleanup_notifies_once_but_parser_cleanup_does_not(
    settings_factory: Callable[..., Settings],
) -> None:
    settings = settings_factory(group_size=5)
    state = BurstStateService(
        settings.threshold,
        60,
        group_size=settings.group_size,
        random_source=ZeroRandom(),
    )
    cleanup = SuccessfulCleanup(state)
    notifier = FakeNotifier()
    handler = TelegramUpdateHandler(
        settings,
        state,
        cleanup,
        Metrics(),
        logging.getLogger("test-handler"),
        notifier=notifier,  # type: ignore[arg-type]
    )

    await handler(FakeEvent(1, 101, username="video_user", text="https://youtube.com/watch?v=1"))
    await handler(FakeEvent(2, 101, username="video_user", text="https://youtube.com/watch?v=2"))
    await handler(FakeEvent(3, 101, username="video_user", text="https://youtube.com/watch?v=3"))
    await handler(
        FakeEvent(
            20,
            500,
            bot=True,
            video=True,
            text="https://youtube.com/watch?v=2",
        )
    )

    assert notifier.calls == [
        {
            "chat_id": -1001,
            "target_key": "id:101",
            "sender_id": 101,
            "sender_username": "video_user",
        }
    ]
    assert [request.message_ids for request in cleanup.requests] == [(2, 3), (20,)]


@pytest.mark.asyncio
async def test_target_gif_and_video_sticker_do_not_count_as_candidates(
    settings_factory: Callable[..., Settings],
) -> None:
    handler, state, cleanup, metrics = make_handler(settings_factory(group_size=4, threshold=2))

    await handler(FakeEvent(1, 101, video=True, gif=True, document_mime_type="video/mp4"))
    await handler(FakeEvent(2, 101, sticker=True, document_mime_type="video/webm"))
    await handler(FakeEvent(3, 101, text="https://youtube.com/watch?v=3"))
    await handler(FakeEvent(4, 101, video=True))

    window = state.chat_state(-1001).target_windows["id:101"]
    assert [request.message_ids for request in cleanup.requests] == [(4,)]
    assert window.candidate_message_ids == [3]
    assert metrics.get("candidate_messages") == 2


@pytest.mark.asyncio
async def test_parser_gif_does_not_consume_next_link_source(
    settings_factory: Callable[..., Settings],
) -> None:
    handler, _, cleanup, metrics = make_handler(
        settings_factory(group_size=5, parser_sender_ids="7947627028")
    )

    await handler(FakeEvent(1, 101, text="https://youtube.com/watch?v=1"))
    await handler(FakeEvent(2, 101, text="https://youtube.com/watch?v=2"))
    await handler(FakeEvent(3, 101, text="https://youtube.com/watch?v=3"))
    await handler(
        FakeEvent(
            20,
            7947627028,
            bot=True,
            video=True,
            gif=True,
            document_mime_type="video/mp4",
        )
    )
    await handler(FakeEvent(21, 7947627028, bot=True, video=True))
    await handler(FakeEvent(22, 7947627028, bot=True, video=True))

    assert [request.message_ids for request in cleanup.requests] == [(2, 3), (22,)]
    assert metrics.get("parser_outputs_received") == 2
