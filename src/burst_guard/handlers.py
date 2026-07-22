from __future__ import annotations

import logging
from typing import Any

from burst_guard.classifier import candidate_url_keys, has_video_media
from burst_guard.cleanup import CleanupService
from burst_guard.config import Settings
from burst_guard.logging import log_event
from burst_guard.metrics import Metrics
from burst_guard.models import IncomingMessage
from burst_guard.notifications import SanctionNotifier
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
    reply_markup = getattr(message, "reply_markup", None)
    for row in getattr(reply_markup, "rows", None) or ():
        for button in getattr(row, "buttons", None) or ():
            button_url = getattr(button, "url", None)
            if button_url:
                urls.append(str(button_url))
    web_preview = getattr(message, "web_preview", None)
    for attribute in ("url", "display_url"):
        preview_url = getattr(web_preview, attribute, None)
        if preview_url:
            urls.append(str(preview_url))
    return tuple(urls)


def to_incoming_message(event: Any, sender: Any) -> IncomingMessage:
    message = event.message
    document = getattr(message, "document", None)
    sender_id = getattr(event, "sender_id", None)
    if not isinstance(sender_id, int) or sender_id <= 0:
        sender_id = None
    username = getattr(sender, "username", None)
    reply_to_message_id = getattr(message, "reply_to_msg_id", None)
    if reply_to_message_id is None:
        reply_to = getattr(message, "reply_to", None)
        reply_to_message_id = getattr(reply_to, "reply_to_msg_id", None)
    if not isinstance(reply_to_message_id, int) or reply_to_message_id <= 0:
        reply_to_message_id = None
    return IncomingMessage(
        chat_id=int(event.chat_id),
        message_id=int(message.id),
        sender_id=sender_id,
        sender_username=str(username) if username else None,
        reply_to_message_id=reply_to_message_id,
        text=getattr(message, "message", None),
        entity_urls=_entity_urls(message),
        has_video=getattr(message, "video", None) is not None,
        has_video_note=getattr(message, "video_note", None) is not None,
        is_animated=getattr(message, "gif", None) is not None,
        is_sticker=getattr(message, "sticker", None) is not None,
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
        *,
        notifier: SanctionNotifier | None = None,
    ) -> None:
        self._settings = settings
        self._state = state
        self._cleanup = cleanup
        self._metrics = metrics
        self._logger = logger
        self._notifier = notifier

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
        target_key = self._settings.target_key(incoming.sender_id, incoming.sender_username)
        is_parser_sender = incoming.sender_id in self._settings.parser_sender_ids
        if getattr(sender, "bot", False):
            await self._handle_parser_output(
                incoming,
                use_next_link_source=is_parser_sender,
            )
            return
        if (target_key is None or is_parser_sender) and has_video_media(incoming):
            parser_url_keys = candidate_url_keys(incoming, self._settings.video_domains)
            if parser_url_keys or incoming.reply_to_message_id is not None or is_parser_sender:
                await self._handle_parser_output(
                    incoming,
                    parser_url_keys,
                    use_next_link_source=is_parser_sender,
                )
                return
        self._metrics.increment("group_messages_received")

        url_keys = (
            candidate_url_keys(incoming, self._settings.video_domains)
            if target_key is not None
            else frozenset()
        )
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
            self._metrics.increment("target_windows_started")

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
        )
        if result.cleanup is not None:
            report = await self._cleanup.execute(result.cleanup)
            if self._notifier is not None and report.deleted > 0 and target_key is not None:
                await self._notifier.notify(
                    chat_id=incoming.chat_id,
                    target_key=target_key,
                    sender_id=incoming.sender_id,
                    sender_username=incoming.sender_username,
                )

    async def _handle_parser_output(
        self,
        incoming: IncomingMessage,
        url_keys: frozenset[str] | None = None,
        *,
        use_next_link_source: bool = False,
    ) -> None:
        if not has_video_media(incoming):
            return
        if url_keys is None:
            url_keys = candidate_url_keys(incoming, self._settings.video_domains)
        if not url_keys and incoming.reply_to_message_id is None and not use_next_link_source:
            return
        self._metrics.increment("parser_outputs_received")
        result = await self._state.process_parser_output(
            chat_id=incoming.chat_id,
            message_id=incoming.message_id,
            url_keys=url_keys,
            reply_to_message_id=incoming.reply_to_message_id,
            use_next_link_source=use_next_link_source,
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
