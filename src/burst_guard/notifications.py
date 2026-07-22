from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import UTC, date, datetime
from typing import Protocol

from burst_guard.logging import log_event
from burst_guard.metrics import Metrics


class MessageClient(Protocol):
    async def send_message(
        self,
        entity: int,
        message: str,
        *,
        parse_mode: object | None = None,
    ) -> object: ...


class SanctionNotifier:
    def __init__(
        self,
        client: MessageClient,
        metrics: Metrics,
        logger: logging.Logger,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._client = client
        self._metrics = metrics
        self._logger = logger
        self._clock = clock
        self._counts: dict[tuple[date, int, str], int] = {}
        self._lock = asyncio.Lock()

    async def notify(
        self,
        *,
        chat_id: int,
        target_key: str,
        sender_id: int | None,
        sender_username: str | None,
    ) -> bool:
        async with self._lock:
            today = self._utc_day()
            self._discard_previous_days(today)
            key = (today, chat_id, target_key)
            count = self._counts.get(key, 0) + 1
            self._counts[key] = count
            target = self._target_label(sender_id, sender_username)
            message = f"检测到 {target} 正在发送垃圾视频，已制裁，今日共制裁{count}次"  # noqa: RUF001
            try:
                await self._client.send_message(chat_id, message, parse_mode=None)
            except Exception as exc:
                self._metrics.increment("sanction_notifications_failed")
                log_event(
                    self._logger,
                    logging.ERROR,
                    "sanction_notification_failed",
                    chat_id=chat_id,
                    target_key=target_key,
                    sanction_count=count,
                    error_type=type(exc).__name__,
                )
                return False

            self._metrics.increment("sanction_notifications_sent")
            log_event(
                self._logger,
                logging.INFO,
                "sanction_notification_sent",
                chat_id=chat_id,
                target_key=target_key,
                sanction_count=count,
            )
            return True

    def _utc_day(self) -> date:
        now = self._clock()
        if now.tzinfo is None:
            return now.date()
        return now.astimezone(UTC).date()

    def _discard_previous_days(self, today: date) -> None:
        for key in tuple(self._counts):
            if key[0] != today:
                del self._counts[key]

    @staticmethod
    def _target_label(sender_id: int | None, sender_username: str | None) -> str:
        if sender_username:
            return f"@{sender_username.removeprefix('@')}"
        if sender_id is not None:
            return f"用户 {sender_id}"
        return "指定用户"
