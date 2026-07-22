from __future__ import annotations

import asyncio
import re
from collections.abc import Sequence
from itertools import batched

from pyrogram import Client, enums, filters
from pyrogram.errors import FloodWait, MessageIdInvalid, RPCError
from pyrogram.types import Message

from core import bs
from log import logger
from services.burst_guard import BurstCleanup, BurstGuardService
from services.parser import ParseService

logger = logger.bind(name="BurstGuard")

_HANDLER_GROUP = -100
_FINALIZER_GROUP = 100
_URL_PATTERN = re.compile(r"https?://[^\s<>()]+", re.IGNORECASE)
_URL_TRAILING_PUNCTUATION = ".,;:!?]}\"'"
_GROUP_CHAT_TYPES = frozenset({enums.ChatType.GROUP, enums.ChatType.SUPERGROUP})
_DELETE_BATCH_SIZE = 100
_DELETE_MAX_RETRIES = 5

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


async def delete_burst_messages(client: Client, chat_id: int, message_ids: Sequence[int]) -> None:
    unique_ids = tuple(dict.fromkeys(message_ids))
    for message_batch in batched(unique_ids, _DELETE_BATCH_SIZE):
        await _delete_message_batch(client, chat_id, tuple(message_batch))


async def _delete_message_batch(client: Client, chat_id: int, message_ids: tuple[int, ...]) -> None:
    for attempt in range(1, _DELETE_MAX_RETRIES + 1):
        try:
            await client.delete_messages(chat_id, list(message_ids))
            return
        except FloodWait as error:
            if attempt == _DELETE_MAX_RETRIES:
                logger.error(
                    "批量删除达到重试上限: chat_id={}, message_ids={}, retry_after={}",
                    chat_id,
                    message_ids,
                    error.value,
                )
                return
            logger.warning(
                "批量删除触发限流: chat_id={}, message_ids={}, retry_after={}, attempt={}/{}",
                chat_id,
                message_ids,
                error.value,
                attempt,
                _DELETE_MAX_RETRIES,
            )
            await asyncio.sleep(error.value)
        except MessageIdInvalid:
            if len(message_ids) == 1:
                logger.debug("消息已不存在: chat_id={}, message_id={}", chat_id, message_ids[0])
                return
            for message_id in message_ids:
                await _delete_message_batch(client, chat_id, (message_id,))
            return
        except RPCError as error:
            logger.error(
                "批量删除失败: chat_id={}, message_ids={}, error_id={}, error={}",
                chat_id,
                message_ids,
                error.ID,
                error,
            )
            return
        except Exception as error:
            logger.opt(exception=error).error(
                "批量删除异常: chat_id={}, message_ids={}, error={}",
                chat_id,
                message_ids,
                error,
            )
            return


async def cleanup_evictions(client: Client, cleanups: Sequence[BurstCleanup]) -> None:
    for cleanup in cleanups:
        cleanup.cancel_tasks()
    for cleanup in cleanups:
        try:
            await delete_burst_messages(client, cleanup.chat_id, cleanup.message_ids)
        finally:
            await burst_guard.finish_cleanup(cleanup.chat_id, cleanup.source_message_id)


def is_parser_bot_output(message: Message) -> bool:
    via_bot = getattr(message, "via_bot", None)
    if via_bot is not None and via_bot.is_self:
        return True
    forwarded_sender = getattr(getattr(message, "forward_origin", None), "sender_user", None)
    return bool(forwarded_sender is not None and forwarded_sender.is_bot)


async def handle_burst_guard_message(client: Client, message: Message) -> None:
    chat = message.chat
    if not bs.burst_guard_enabled or chat is None or chat.type not in _GROUP_CHAT_TYPES:
        return
    chat_id = chat.id
    if chat_id is None:
        return

    if getattr(message, "service", None) or is_parser_bot_output(message):
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

    platform_ids = video_platform_ids(message)
    video_file = is_video_file(message)
    if video_file or platform_ids & bs.burst_guard_video_platforms:
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
        await cleanup_evictions(client, registration.evictions)
        if video_file and not platform_ids:
            await burst_guard.finish_processing(chat_id, message.id)
        return

    # A target's ordinary text only keeps its own run alive. A different target ends it.
    await burst_guard.close_run_for_other_user(chat_id, sender.id)


@Client.on_message(filters.group, group=_HANDLER_GROUP)
async def burst_guard_listener(client: Client, message: Message) -> None:
    await handle_burst_guard_message(client, message)


@Client.on_message(filters.group, group=_FINALIZER_GROUP)
async def burst_guard_finalizer(_: Client, message: Message) -> None:
    if not bs.burst_guard_enabled or message.chat is None or message.chat.id is None or message.id is None:
        return
    await burst_guard.finish_processing(message.chat.id, message.id)
