from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from collections.abc import Awaitable, Callable
from typing import Any

from burst_guard.logging import log_event
from burst_guard.metrics import Metrics
from burst_guard.models import IncomingMessage

INFO_REPLY_TEXT = "本机器人仅治理 @Dunai_233 滥发视频现象，无其他任何功能"  # noqa: RUF001

ReplySender = Callable[[int, str, int], Awaitable[object]]
Clock = Callable[[], float]


def _entity_text(message: Any, entity: Any) -> str | None:
    getter = getattr(message, "get_entities_text", None)
    if callable(getter):
        try:
            for candidate, text in getter():
                if candidate is entity:
                    return str(text)
        except Exception:
            return None
    return None


def _mentions_account(message: Any, account_id: int, username: str | None) -> bool:
    normalized_username = username.removeprefix("@").casefold() if username else None
    for entity in getattr(message, "entities", None) or ():
        entity_name = type(entity).__name__
        if entity_name == "MessageEntityMentionName":
            try:
                if int(entity.user_id) == account_id:
                    return True
            except (TypeError, ValueError):
                continue
        if entity_name != "MessageEntityMention" or normalized_username is None:
            continue
        text = _entity_text(message, entity)
        if text and text.removeprefix("@").casefold() == normalized_username:
            return True
    return False


def _sender_id(message: Any) -> int | None:
    sender_id = getattr(message, "sender_id", None)
    if isinstance(sender_id, int) and sender_id > 0:
        return sender_id
    sender = getattr(message, "from_id", None)
    sender_id = getattr(sender, "user_id", None)
    return sender_id if isinstance(sender_id, int) and sender_id > 0 else None


class InfoReplyService:
    """Reply to direct mentions/replies without exposing a command surface."""

    def __init__(
        self,
        account_id: int,
        account_username: str | None,
        metrics: Metrics,
        logger: logging.Logger,
        *,
        enabled: bool = True,
        cooldown_seconds: float = 60.0,
        clock: Clock = time.monotonic,
    ) -> None:
        self._account_id = account_id
        self._account_username = account_username
        self._metrics = metrics
        self._logger = logger
        self._enabled = enabled
        self._cooldown_seconds = cooldown_seconds
        self._clock = clock
        self._next_allowed_at: dict[int, float] = {}
        self._own_message_ids: dict[int, deque[int]] = {}
        self._lock = asyncio.Lock()

    async def maybe_reply(
        self,
        event: Any,
        incoming: IncomingMessage,
        send_reply: ReplySender,
    ) -> bool:
        if not self._enabled or incoming.sender_id is None:
            return False
        if not await self._is_trigger(event, incoming):
            return False

        reservation = await self._reserve(incoming.chat_id)
        if reservation is None:
            self._metrics.increment("info_reply_rate_limited")
            return False

        try:
            sent = await send_reply(incoming.chat_id, INFO_REPLY_TEXT, incoming.message_id)
        except Exception as exc:
            await self._release(incoming.chat_id, reservation)
            self._metrics.increment("info_reply_failed")
            log_event(
                self._logger,
                logging.WARNING,
                "info_reply_failed",
                chat_id=incoming.chat_id,
                message_id=incoming.message_id,
                error_type=type(exc).__name__,
            )
            return False

        sent_id = getattr(sent, "id", None)
        if isinstance(sent_id, int) and sent_id > 0:
            async with self._lock:
                own_messages = self._own_message_ids.setdefault(incoming.chat_id, deque(maxlen=256))
                own_messages.append(sent_id)
        self._metrics.increment("info_reply_sent")
        log_event(
            self._logger,
            logging.INFO,
            "info_reply_sent",
            chat_id=incoming.chat_id,
            message_id=incoming.message_id,
        )
        return True

    async def _is_trigger(self, event: Any, incoming: IncomingMessage) -> bool:
        if _mentions_account(event.message, self._account_id, self._account_username):
            return True
        reply_to = incoming.reply_to_message_id
        if reply_to is None:
            return False
        async with self._lock:
            if reply_to in self._own_message_ids.get(incoming.chat_id, set()):
                return True
        get_reply_message = getattr(event, "get_reply_message", None)
        if not callable(get_reply_message):
            return False
        try:
            reply_message = await get_reply_message()
        except Exception:
            return False
        return _sender_id(reply_message) == self._account_id

    async def _reserve(self, chat_id: int) -> float | None:
        now = self._clock()
        async with self._lock:
            next_allowed_at = self._next_allowed_at.get(chat_id, 0.0)
            if next_allowed_at > now:
                return None
            reservation = now + self._cooldown_seconds
            self._next_allowed_at[chat_id] = reservation
            return reservation

    async def _release(self, chat_id: int, reservation: float) -> None:
        async with self._lock:
            if self._next_allowed_at.get(chat_id) == reservation:
                self._next_allowed_at.pop(chat_id, None)
