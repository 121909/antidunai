from __future__ import annotations

import re

from pyrogram import Client, enums, filters
from pyrogram.types import Message

from core import bs
from log import logger
from services.burst_guard import BurstGuardService
from services.parser import ParseService

logger = logger.bind(name="BurstGuard")

_HANDLER_GROUP = -100
_URL_PATTERN = re.compile(r"https?://[^\s<>()]+", re.IGNORECASE)
_URL_TRAILING_PUNCTUATION = ".,;:!?]}\"'"
_GROUP_CHAT_TYPES = frozenset({enums.ChatType.GROUP, enums.ChatType.SUPERGROUP})

burst_guard = BurstGuardService(threshold=bs.burst_guard_threshold)


def _platform_id(url: str) -> str | None:
    try:
        platform = ParseService().parser.get_platform(url)
    except Exception:
        return None
    return platform.id.casefold() if platform else None


def video_platform_ids(message: Message) -> frozenset[str]:
    text = message.text or message.caption or ""
    platform_ids: set[str] = set()
    for match in _URL_PATTERN.finditer(text):
        url = match.group(0).rstrip(_URL_TRAILING_PUNCTUATION)
        if platform_id := _platform_id(url):
            platform_ids.add(platform_id)
    return frozenset(platform_ids)


def is_video_file(message: Message) -> bool:
    if message.video is not None or message.video_note is not None:
        return True
    document = message.document
    mime_type = document.mime_type if document is not None else None
    return bool(mime_type and mime_type.casefold().startswith("video/"))


def is_video_candidate(message: Message, platform_allowlist: frozenset[str]) -> bool:
    if is_video_file(message):
        return True
    return bool(video_platform_ids(message) & platform_allowlist)


async def handle_burst_guard_message(_: Client, message: Message) -> None:
    chat = message.chat
    if not bs.burst_guard_enabled or chat is None or chat.type not in _GROUP_CHAT_TYPES:
        return
    chat_id = chat.id
    if chat_id is None:
        return

    if getattr(message, "service", None):
        return

    sender = message.from_user
    if sender is not None and sender.is_bot:
        return

    if message.sender_chat is not None or sender is None:
        await burst_guard.close_run(chat_id)
        return

    is_target = bs.is_burst_guard_target(sender.id, sender.username)
    if not is_target:
        await burst_guard.close_run(chat_id)
        return

    if is_video_candidate(message, bs.burst_guard_video_platforms):
        registration = await burst_guard.register_candidate(chat_id, sender.id, message.id)
        logger.debug(
            "候选消息已登记: chat_id={}, user_id={}, message_id={}, run_id={}, count={}, status={}",
            chat_id,
            sender.id,
            message.id,
            registration.run_id,
            registration.candidate_count,
            registration.status,
        )
        return

    # A target's ordinary text only keeps its own run alive. A different target ends it.
    await burst_guard.close_run_for_other_user(chat_id, sender.id)


@Client.on_message(filters.group, group=_HANDLER_GROUP)
async def burst_guard_listener(client: Client, message: Message) -> None:
    await handle_burst_guard_message(client, message)
