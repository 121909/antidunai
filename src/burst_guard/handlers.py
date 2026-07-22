from __future__ import annotations

import logging
from typing import Any

from burst_guard.classifier import candidate_url_keys, has_video_media
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
        incoming = to_incoming_message(event, sender)
        if incoming.sender_id == self._settings.telegram_expected_user_id:
            return
        if getattr(sender, "bot", False):
            await self._handle_parser_output(incoming)
            return
        self._metrics.increment("group_messages_received")

        target_key = self._settings.target_key(incoming.sender_id, incoming.sender_username)
        url_keys = candidate_url_keys(incoming, self._settings.video_domains)
        candidate = bool(target_key and (url_keys or has_video_media(incoming)))
        result = await self._state.process(
            chat_id=incoming.chat_id,
            message_id=incoming.message_id,
            target_key=target_key,
            is_candidate=candidate,
            candidate_url_keys=url_keys,
        )
        if result.duplicate:
            self._metrics.increment("duplicate_updates")
            return
        if candidate:
            self._metrics.increment("candidate_messages")
        if result.run_started:
            self._metrics.increment("groups_started")

        log_event(
            self._logger,
            logging.INFO,
            "message_processed",
            chat_id=incoming.chat_id,
            run_id=result.run_id,
            sender_id=incoming.sender_id,
            message_id=incoming.message_id,
            message_count=result.message_count or None,
            candidate_count=result.candidate_count or None,
            retained_message_id=result.retained_message_id,
            candidate=candidate,
            run_closed=result.run_closed,
        )
        if result.cleanup is not None:
            await self._cleanup.execute(result.cleanup)

    async def _handle_parser_output(self, incoming: IncomingMessage) -> None:
        if not has_video_media(incoming):
            return
        url_keys = candidate_url_keys(incoming, self._settings.video_domains)
        if not url_keys:
            return
        self._metrics.increment("parser_outputs_received")
        result = await self._state.process_parser_output(
            chat_id=incoming.chat_id,
            message_id=incoming.message_id,
            url_keys=url_keys,
        )
        if result.duplicate:
            self._metrics.increment("duplicate_updates")
            return
        matched = result.cleanup is not None
        log_event(
            self._logger,
            logging.INFO,
            "parser_output_processed",
            chat_id=incoming.chat_id,
            run_id=result.run_id,
            sender_id=incoming.sender_id,
            message_id=incoming.message_id,
            matched_discarded_candidate=matched,
        )
        if result.cleanup is not None:
            self._metrics.increment("parser_outputs_matched")
            await self._cleanup.execute(result.cleanup)
