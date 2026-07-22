from __future__ import annotations

import logging
from typing import Any

from burst_guard.classifier import is_video_candidate
from burst_guard.cleanup import CleanupService
from burst_guard.config import Settings
from burst_guard.logging import log_event
from burst_guard.metrics import Metrics
from burst_guard.models import IncomingMessage
from burst_guard.state import BurstStateService


def _entity_urls(message: Any) -> tuple[str, ...]:
    urls: list[str] = []
    for entity in getattr(message, "entities", None) or ():
        hidden_url = getattr(entity, "url", None)
        if hidden_url:
            urls.append(str(hidden_url))
    get_entities_text = getattr(message, "get_entities_text", None)
    if callable(get_entities_text):
        for entity, text in get_entities_text():
            if type(entity).__name__ == "MessageEntityUrl" and text:
                urls.append(str(text))
    return tuple(urls)


def to_incoming_message(event: Any, sender: Any) -> IncomingMessage:
    message = event.message
    document = getattr(message, "document", None)
    sender_id = getattr(event, "sender_id", None)
    if not isinstance(sender_id, int) or sender_id <= 0:
        sender_id = None
    username = getattr(sender, "username", None)
    return IncomingMessage(
        chat_id=int(event.chat_id),
        message_id=int(message.id),
        sender_id=sender_id,
        sender_username=str(username) if username else None,
        text=getattr(message, "message", None),
        entity_urls=_entity_urls(message),
        has_video=getattr(message, "video", None) is not None,
        has_video_note=getattr(message, "video_note", None) is not None,
        document_mime_type=getattr(document, "mime_type", None),
    )


class TelegramUpdateHandler:
    def __init__(
        self,
        settings: Settings,
        state: BurstStateService,
        cleanup: CleanupService,
        metrics: Metrics,
        logger: logging.Logger,
    ) -> None:
        self._settings = settings
        self._state = state
        self._cleanup = cleanup
        self._metrics = metrics
        self._logger = logger

    async def __call__(self, event: Any) -> None:
        if not self._settings.enabled:
            return
        chat_id = getattr(event, "chat_id", None)
        if chat_id not in self._settings.chat_ids:
            return
        if not getattr(event, "is_group", False):
            return
        message = getattr(event, "message", None)
        if message is None or getattr(message, "action", None) is not None:
            return
        if getattr(message, "out", False):
            return

        sender = await event.get_sender()
        if getattr(sender, "bot", False):
            return

        incoming = to_incoming_message(event, sender)
        if incoming.sender_id == self._settings.telegram_expected_user_id:
            return
        self._metrics.increment("group_messages_received")

        target_key = self._settings.target_key(incoming.sender_id, incoming.sender_username)
        candidate = bool(target_key and is_video_candidate(incoming, self._settings.video_domains))
        result = await self._state.process(
            chat_id=incoming.chat_id,
            message_id=incoming.message_id,
            target_key=target_key,
            is_candidate=candidate,
        )
        if result.duplicate:
            self._metrics.increment("duplicate_updates")
            return
        if candidate:
            self._metrics.increment("candidate_messages")
        if result.run_started:
            self._metrics.increment("bursts_started")

        log_event(
            self._logger,
            logging.INFO,
            "message_processed",
            chat_id=incoming.chat_id,
            run_id=result.run_id,
            sender_id=incoming.sender_id,
            message_id=incoming.message_id,
            candidate_count=result.candidate_count or None,
            retained_message_id=result.retained_message_id,
            candidate=candidate,
            run_closed=result.run_closed,
        )
        if result.cleanup is not None:
            await self._cleanup.execute(result.cleanup)
