from types import SimpleNamespace
from typing import Any, cast

import pytest
from pyrogram import enums
from pyrogram.types import Message

from plugins import burst_guard as plugin
from services.burst_guard import BurstGuardService


class FakeSettings:
    burst_guard_enabled = True
    burst_guard_threshold = 3
    burst_guard_video_platforms = frozenset({"youtube", "douyin"})

    def __init__(self, *targets: tuple[int, str | None]) -> None:
        self.targets = set(targets)

    def is_burst_guard_target(self, user_id: int, username: str | None) -> bool:
        return (user_id, username) in self.targets


def make_message(
    message_id: int,
    *,
    chat_id: int = -100,
    chat_type: enums.ChatType = enums.ChatType.SUPERGROUP,
    user_id: int | None = 10,
    username: str | None = "target",
    is_bot: bool = False,
    text: str | None = None,
    caption: str | None = None,
    video: object | None = None,
    video_note: object | None = None,
    document_mime: str | None = None,
    sender_chat: object | None = None,
    service: object | None = None,
) -> Message:
    from_user = None
    if user_id is not None:
        from_user = SimpleNamespace(id=user_id, username=username, is_bot=is_bot)
    document = SimpleNamespace(mime_type=document_mime) if document_mime else None
    return cast(
        Message,
        SimpleNamespace(
            id=message_id,
            chat=SimpleNamespace(id=chat_id, type=chat_type),
            from_user=from_user,
            sender_chat=sender_chat,
            service=service,
            text=text,
            caption=caption,
            video=video,
            video_note=video_note,
            document=document,
        ),
    )


def configure(monkeypatch: pytest.MonkeyPatch, *targets: tuple[int, str | None]) -> BurstGuardService:
    service = BurstGuardService()
    monkeypatch.setattr(plugin, "bs", FakeSettings(*targets))
    monkeypatch.setattr(plugin, "burst_guard", service)
    return service


@pytest.mark.parametrize(
    ("attributes", "expected"),
    [
        ({"video": object()}, True),
        ({"video_note": object()}, True),
        ({"document_mime": "video/mp4"}, True),
        ({"document_mime": "VIDEO/QUICKTIME"}, True),
        ({"document_mime": "application/zip"}, False),
        ({}, False),
    ],
)
def test_video_file_recognition(attributes: dict[str, Any], expected: bool) -> None:
    assert plugin.is_video_file(make_message(1, **attributes)) is expected


def test_link_candidate_uses_platform_allowlist(monkeypatch: pytest.MonkeyPatch) -> None:
    def platform_id(url: str) -> str | None:
        if "youtu.be" in url:
            return "youtube"
        if "example.com" in url:
            return "article"
        return None

    monkeypatch.setattr(plugin, "_platform_id", platform_id)
    message = make_message(
        1,
        text="two links https://youtu.be/first and https://youtu.be/second, plus https://example.com/post",
    )

    assert plugin.video_platform_ids(message) == frozenset({"youtube", "article"})
    assert plugin.is_video_candidate(message, frozenset({"youtube"})) is True
    assert plugin.is_video_candidate(message, frozenset({"bilibili"})) is False


@pytest.mark.asyncio
async def test_multiple_links_in_one_message_count_once(monkeypatch: pytest.MonkeyPatch) -> None:
    service = configure(monkeypatch, (10, "target"))
    monkeypatch.setattr(plugin, "_platform_id", lambda _: "youtube")
    message = make_message(1, text="https://youtu.be/first https://youtu.be/second")

    await plugin.handle_burst_guard_message(cast(Any, None), message)

    run = await service.get_run(-100)
    assert run is not None and run.candidate_count == 1


@pytest.mark.asyncio
async def test_same_target_text_and_bot_messages_do_not_break_run(monkeypatch: pytest.MonkeyPatch) -> None:
    service = configure(monkeypatch, (10, "target"))
    await plugin.handle_burst_guard_message(cast(Any, None), make_message(1, video=object()))

    await plugin.handle_burst_guard_message(cast(Any, None), make_message(2, text="ordinary text"))
    await plugin.handle_burst_guard_message(
        cast(Any, None),
        make_message(3, user_id=99, username="parser", is_bot=True, text="progress"),
    )
    await plugin.handle_burst_guard_message(
        cast(Any, None),
        make_message(4, user_id=99, username="member", service=object()),
    )

    run = await service.get_run(-100)
    assert run is not None and run.run_id == 1 and run.candidate_count == 1


@pytest.mark.asyncio
async def test_other_human_breaks_run(monkeypatch: pytest.MonkeyPatch) -> None:
    service = configure(monkeypatch, (10, "target"))
    await plugin.handle_burst_guard_message(cast(Any, None), make_message(1, video=object()))

    await plugin.handle_burst_guard_message(
        cast(Any, None),
        make_message(2, user_id=99, username="member", text="interrupt"),
    )

    assert await service.get_run(-100) is None


@pytest.mark.asyncio
async def test_anonymous_sender_breaks_run(monkeypatch: pytest.MonkeyPatch) -> None:
    service = configure(monkeypatch, (10, "target"))
    await plugin.handle_burst_guard_message(cast(Any, None), make_message(1, video=object()))

    await plugin.handle_burst_guard_message(
        cast(Any, None),
        make_message(2, user_id=None, sender_chat=SimpleNamespace(id=-100), text="anonymous"),
    )

    assert await service.get_run(-100) is None


@pytest.mark.asyncio
async def test_other_target_starts_its_own_run_only_for_candidate(monkeypatch: pytest.MonkeyPatch) -> None:
    service = configure(monkeypatch, (10, "first"), (20, "second"))
    await plugin.handle_burst_guard_message(
        cast(Any, None),
        make_message(1, user_id=10, username="first", video=object()),
    )

    await plugin.handle_burst_guard_message(
        cast(Any, None),
        make_message(2, user_id=20, username="second", text="ordinary"),
    )
    assert await service.get_run(-100) is None

    await plugin.handle_burst_guard_message(
        cast(Any, None),
        make_message(3, user_id=20, username="second", video=object()),
    )
    run = await service.get_run(-100)
    assert run is not None and run.user_id == 20 and run.run_id == 2


@pytest.mark.asyncio
async def test_private_chat_and_disabled_feature_are_noops(monkeypatch: pytest.MonkeyPatch) -> None:
    service = configure(monkeypatch, (10, "target"))

    await plugin.handle_burst_guard_message(
        cast(Any, None),
        make_message(1, chat_type=enums.ChatType.PRIVATE, video=object()),
    )
    cast(FakeSettings, plugin.bs).burst_guard_enabled = False
    await plugin.handle_burst_guard_message(cast(Any, None), make_message(2, video=object()))

    assert await service.get_run(-100) is None


@pytest.mark.asyncio
async def test_group_state_is_isolated(monkeypatch: pytest.MonkeyPatch) -> None:
    service = configure(monkeypatch, (10, "target"))
    await plugin.handle_burst_guard_message(cast(Any, None), make_message(1, chat_id=-100, video=object()))
    await plugin.handle_burst_guard_message(cast(Any, None), make_message(1, chat_id=-200, video=object()))

    await plugin.handle_burst_guard_message(
        cast(Any, None),
        make_message(2, chat_id=-100, user_id=99, username="member", text="interrupt"),
    )

    assert await service.get_run(-100) is None
    other_run = await service.get_run(-200)
    assert other_run is not None and other_run.candidate_count == 1
