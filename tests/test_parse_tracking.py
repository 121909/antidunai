import ast
import asyncio
from collections.abc import Sequence
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from pyrogram import Client, enums
from pyrogram.types import Message

from plugins import burst_guard as burst_plugin
from plugins import parse
from services.burst_guard import BurstGuardService
from services.cache import CacheEntry, CacheMedia, CacheMediaType, CacheParseResult


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


def test_every_parse_reply_is_wrapped_by_output_tracking() -> None:
    source = Path(parse.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    parents: dict[ast.AST, ast.AST] = {}
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            parents[child] = parent

    untracked: list[int] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        if not node.func.attr.startswith("reply_") or node.func.attr == "reply_chat_action":
            continue
        ancestor = parents.get(node)
        tracked = False
        while ancestor is not None:
            if isinstance(ancestor, ast.Call) and isinstance(ancestor.func, ast.Name):
                if ancestor.func.id in {"_track_output", "_send_tracked"}:
                    tracked = True
                    break
            ancestor = parents.get(ancestor)
        if not tracked:
            untracked.append(node.lineno)

    assert untracked == []


@pytest.mark.asyncio
async def test_output_context_tracks_single_and_media_group_results() -> None:
    service = BurstGuardService(rng=FixedRandom(0))
    for message_id in range(1, 4):
        await service.register_candidate(-100, 10, message_id)
    deleted: list[tuple[int, tuple[int, ...]]] = []

    async def delete_messages(chat_id: int, message_ids: tuple[int, ...]) -> None:
        deleted.append((chat_id, message_ids))

    tracker = service.output_tracker(-100, 2, delete_messages)
    token = parse._output_tracker.set(tracker)
    try:
        single = SimpleNamespace(id=20)
        media_group = [SimpleNamespace(id=21), SimpleNamespace(id=22)]
        assert await parse._track_output(single) is single
        assert await parse._track_output(media_group) is media_group
    finally:
        parse._output_tracker.reset(token)

    assert deleted == [(-100, (20,)), (-100, (21, 22))]


@pytest.mark.asyncio
async def test_cached_text_single_media_and_media_group_outputs_are_tracked() -> None:
    class ReplyMessage:
        def __init__(self) -> None:
            self.next_id = 100

        def sent(self) -> SimpleNamespace:
            message = SimpleNamespace(id=self.next_id)
            self.next_id += 1
            return message

        async def reply_chat_action(self, _: object) -> None:
            return

        async def reply_text(self, *_: Any, **__: Any) -> SimpleNamespace:
            return self.sent()

        async def reply_photo(self, *_: Any, **__: Any) -> SimpleNamespace:
            return self.sent()

        async def reply_video(self, *_: Any, **__: Any) -> SimpleNamespace:
            return self.sent()

        async def reply_animation(self, *_: Any, **__: Any) -> SimpleNamespace:
            return self.sent()

        async def reply_document(self, *_: Any, **__: Any) -> SimpleNamespace:
            return self.sent()

        async def reply_media_group(self, media: Sequence[object]) -> list[SimpleNamespace]:
            return [self.sent() for _ in media]

    service = BurstGuardService(rng=FixedRandom(0))
    for message_id in range(1, 4):
        await service.register_candidate(-100, 10, message_id)
    deleted: list[tuple[int, tuple[int, ...]]] = []

    async def delete_messages(chat_id: int, message_ids: tuple[int, ...]) -> None:
        deleted.append((chat_id, message_ids))

    token = parse._output_tracker.set(service.output_tracker(-100, 2, delete_messages))
    message = cast(Message, ReplyMessage())
    cache_result = CacheParseResult(title="title", content="content")
    try:
        await parse._send_cached(
            message,
            CacheEntry(parse_result=cache_result, telegraph_url="https://telegra.ph/page"),
            "https://source",
            user_config=parse.UserConfig(),
        )
        await parse._send_cached(
            message,
            CacheEntry(parse_result=cache_result),
            "https://source",
            user_config=parse.UserConfig(),
        )
        for media_type in CacheMediaType:
            await parse._send_cached(
                message,
                CacheEntry(
                    parse_result=cache_result,
                    media=[CacheMedia(type=media_type, file_id=f"{media_type}-file")],
                ),
                "https://source",
                user_config=parse.UserConfig(),
            )
        await parse._send_cached(
            message,
            CacheEntry(
                parse_result=cache_result,
                media=[
                    CacheMedia(type=CacheMediaType.ANIMATION, file_id="animation"),
                    CacheMedia(type=CacheMediaType.PHOTO, file_id="photo"),
                    CacheMedia(type=CacheMediaType.DOCUMENT, file_id="document"),
                ],
            ),
            "https://source",
            user_config=parse.UserConfig(),
        )
    finally:
        parse._output_tracker.reset(token)

    deleted_ids = [message_id for _, message_ids in deleted for message_id in message_ids]
    assert deleted_ids == list(range(100, 109))


@pytest.mark.asyncio
async def test_progress_error_raw_and_zip_outputs_are_tracked(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    class SentMessage:
        def __init__(self, message_id: int) -> None:
            self.id = message_id
            self.text: str | None = None

        async def delete(self) -> None:
            return

        async def edit_text(self, text: str, **_: Any) -> None:
            self.text = text

    class ReplyMessage:
        def __init__(self) -> None:
            self.next_id = 200

        async def reply_chat_action(self, _: object) -> None:
            return

        async def reply_text(self, *_: Any, **__: Any) -> SentMessage:
            sent = SentMessage(self.next_id)
            self.next_id += 1
            return sent

        async def reply_document(self, *_: Any, **__: Any) -> SentMessage:
            sent = SentMessage(self.next_id)
            self.next_id += 1
            return sent

    class Reporter:
        async def report(self, _: str) -> None:
            return

        async def report_error(self, _: str, __: Exception) -> None:
            return

        async def dismiss(self) -> None:
            return

    class Result:
        def __init__(self, output_dir: Path, processed_list: list[object]) -> None:
            self.parse_result = SimpleNamespace(title="title", content="content", raw_url="https://source", media=[])
            self.output_dir = output_dir
            self.processed_list = processed_list
            self.cleanup_count = 0

        def cleanup(self) -> None:
            self.cleanup_count += 1

    service = BurstGuardService(rng=FixedRandom(0))
    for message_id in range(1, 4):
        await service.register_candidate(-100, 10, message_id)
    deleted: list[tuple[int, tuple[int, ...]]] = []

    async def delete_messages(chat_id: int, message_ids: tuple[int, ...]) -> None:
        deleted.append((chat_id, message_ids))

    token = parse._output_tracker.set(service.output_tracker(-100, 2, delete_messages))
    message = cast(Message, ReplyMessage())
    user_config = parse.UserConfig(keep_error_log=True)
    translator = cast(Any, lambda text: text)
    source_file = tmp_path / "source.mp4"
    source_file.write_bytes(b"video")
    raw_result = Result(tmp_path, [SimpleNamespace(output_paths=None, source=SimpleNamespace(path=source_file))])
    archive = tmp_path / "archive.tar.gz"
    archive.write_bytes(b"archive")
    zip_result = Result(tmp_path, [])
    monkeypatch.setattr(parse, "pack_dir_to_tar_gz", lambda _: archive)
    try:
        status_reporter = parse.MessageStatusReporter(message, _t=translator, user_config=user_config)
        await status_reporter.report_error("parse", RuntimeError("failed"))
        await parse._send_raw(
            message,
            cast(Any, raw_result),
            cast(Any, Reporter()),
            _t=translator,
            user_config=user_config,
        )
        await parse._send_zip(
            message,
            cast(Any, zip_result),
            cast(Any, Reporter()),
            _t=translator,
            user_config=user_config,
        )
    finally:
        parse._output_tracker.reset(token)

    deleted_ids = [message_id for _, message_ids in deleted for message_id in message_ids]
    assert deleted_ids == [200, 201, 202]
    assert raw_result.cleanup_count == 1
    assert zip_result.cleanup_count == 1
    assert archive.exists() is False


@pytest.mark.asyncio
async def test_disabled_feature_does_not_create_output_tracker(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(parse.bs, "burst_guard_enabled", False)
    observed: list[object] = []

    async def fake_process(_: Client, __: Message) -> None:
        observed.append(parse._output_tracker.get())

    monkeypatch.setattr(parse, "_process_message", fake_process)
    message = cast(
        Message,
        SimpleNamespace(
            id=1,
            chat=SimpleNamespace(id=-100, type=enums.ChatType.SUPERGROUP),
        ),
    )

    await parse.jx(as_client(RecordingClient()), message)

    assert observed == [None]


@pytest.mark.asyncio
async def test_external_handler_cancellation_is_not_swallowed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(parse.bs, "burst_guard_enabled", False)
    started = asyncio.Event()

    async def fake_process(_: Client, __: Message) -> None:
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(parse, "_process_message", fake_process)
    message = cast(
        Message,
        SimpleNamespace(
            id=1,
            chat=SimpleNamespace(id=-100, type=enums.ChatType.SUPERGROUP),
        ),
    )
    task = asyncio.create_task(parse.jx(as_client(RecordingClient()), message))
    await started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_eviction_cancels_parse_and_deletes_output_sent_during_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = BurstGuardService(rng=FixedRandom(1))
    await service.register_candidate(-100, 10, 1)
    monkeypatch.setattr(parse, "burst_guard", service)
    monkeypatch.setattr(burst_plugin, "burst_guard", service)
    monkeypatch.setattr(parse.bs, "burst_guard_enabled", True)

    class FakeParseService:
        parser = SimpleNamespace(get_platform=lambda _: SimpleNamespace(id="youtube"))

    monkeypatch.setattr(parse, "ParseService", FakeParseService)
    all_started = asyncio.Event()
    started_count = 0
    cancelled_urls: set[str] = set()

    async def fake_parse_request(*_: Any, **kwargs: Any) -> None:
        nonlocal started_count
        url = cast(str, kwargs["url"])
        started_count += 1
        if started_count == 2:
            all_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled_urls.add(url)
            output_id = 101 if url.endswith("first") else 102
            await parse._track_output(SimpleNamespace(id=output_id))

    monkeypatch.setattr(parse, "_handle_parse_request", fake_parse_request)
    message = cast(
        Message,
        SimpleNamespace(
            id=1,
            chat=SimpleNamespace(id=-100, type=enums.ChatType.SUPERGROUP),
            from_user=None,
            command=None,
            reply_to_message=None,
            text="https://youtu.be/first https://youtu.be/second",
            caption=None,
        ),
    )
    client = RecordingClient()
    handler_task = asyncio.create_task(parse.jx(as_client(client), message))
    await all_started.wait()
    await asyncio.sleep(0)

    await service.register_candidate(-100, 10, 2)
    third = await service.register_candidate(-100, 10, 3)
    await burst_plugin.cleanup_evictions(as_client(client), third.evictions)
    await handler_task

    assert cancelled_urls == {"https://youtu.be/first", "https://youtu.be/second"}
    deleted_ids = {message_id for _, message_ids in client.calls for message_id in message_ids}
    assert {1, 3, 101, 102} <= deleted_ids
    after_completion = await service.record_output(-100, 1, [102])
    assert after_completion.tracked is False
