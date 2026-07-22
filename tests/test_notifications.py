from __future__ import annotations

import logging
from datetime import UTC, datetime

import pytest

from burst_guard.metrics import Metrics
from burst_guard.notifications import SanctionNotifier


class FakeMessageClient:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.messages: list[tuple[int, str, object | None]] = []

    async def send_message(
        self,
        entity: int,
        message: str,
        *,
        parse_mode: object | None = None,
    ) -> object:
        if self.error is not None:
            raise self.error
        self.messages.append((entity, message, parse_mode))
        return object()


@pytest.mark.asyncio
async def test_notifications_count_per_chat_and_target_for_utc_day() -> None:
    now = datetime(2026, 7, 22, 23, 59, tzinfo=UTC)
    client = FakeMessageClient()
    metrics = Metrics()
    notifier = SanctionNotifier(
        client,
        metrics,
        logging.getLogger("test-notifications"),
        clock=lambda: now,
    )

    await notifier.notify(
        chat_id=-1,
        target_key="id:101",
        sender_id=101,
        sender_username="video_user",
    )
    await notifier.notify(
        chat_id=-1,
        target_key="id:101",
        sender_id=101,
        sender_username="video_user",
    )
    await notifier.notify(
        chat_id=-1,
        target_key="id:202",
        sender_id=202,
        sender_username=None,
    )

    assert client.messages == [
        (-1, "检测到 @video_user 正在发送垃圾视频，已制裁，今日共制裁1次", None),  # noqa: RUF001
        (-1, "检测到 @video_user 正在发送垃圾视频，已制裁，今日共制裁2次", None),  # noqa: RUF001
        (-1, "检测到 用户 202 正在发送垃圾视频，已制裁，今日共制裁1次", None),  # noqa: RUF001
    ]
    assert metrics.get("sanction_notifications_sent") == 3

    now = datetime(2026, 7, 23, 0, 0, tzinfo=UTC)
    await notifier.notify(
        chat_id=-1,
        target_key="id:101",
        sender_id=101,
        sender_username="video_user",
    )

    assert client.messages[-1][1].endswith("今日共制裁1次")


@pytest.mark.asyncio
async def test_notification_failure_is_reported_without_raising() -> None:
    client = FakeMessageClient(RuntimeError("send failed"))
    metrics = Metrics()
    notifier = SanctionNotifier(client, metrics, logging.getLogger("test-notifications"))

    sent = await notifier.notify(
        chat_id=-1,
        target_key="id:101",
        sender_id=101,
        sender_username="video_user",
    )

    assert sent is False
    assert metrics.get("sanction_notifications_failed") == 1
