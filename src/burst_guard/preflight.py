from __future__ import annotations

import logging
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from telethon import types

from burst_guard.config import Settings
from burst_guard.logging import log_event


class PreflightClient(Protocol):
    async def connect(self) -> None: ...

    async def is_user_authorized(self) -> bool: ...

    async def get_me(self) -> Any: ...

    async def get_entity(self, entity: int) -> Any: ...

    async def get_permissions(self, entity: Any, user: Any) -> Any: ...


@dataclass(frozen=True, slots=True)
class PreflightSummary:
    account_id: int
    chat_count: int
    target_count: int
    group_size: int
    threshold: int
    domain_count: int
    dry_run: bool


def validate_session_file(path: Path) -> None:
    if path.is_symlink():
        raise RuntimeError("Telegram session path must not be a symbolic link")
    if not path.is_file():
        raise RuntimeError(f"Telegram session file does not exist: {path}")
    mode = stat.S_IMODE(path.stat().st_mode)
    if mode & 0o077:
        raise RuntimeError("Telegram session file permissions must not allow group/world access")
    if not mode & stat.S_IRUSR or not mode & stat.S_IWUSR:
        raise RuntimeError("Telegram session file must be readable and writable by its owner")


def _is_managed_group(entity: Any) -> bool:
    if isinstance(entity, types.Chat):
        return True
    return isinstance(entity, types.Channel) and bool(entity.megagroup) and not entity.broadcast


async def run_preflight(
    client: PreflightClient, settings: Settings, logger: logging.Logger
) -> PreflightSummary:
    validate_session_file(settings.telegram_session_path)
    await client.connect()
    if not await client.is_user_authorized():
        raise RuntimeError("Telegram session is not authorized; run burst-guard-login first")

    me = await client.get_me()
    if me is None or getattr(me, "bot", True):
        raise RuntimeError("the Telegram session must belong to a user account, not a bot")
    account_id = int(me.id)
    if account_id != settings.telegram_expected_user_id:
        raise RuntimeError(
            f"authorized account ID {account_id} does not match TELEGRAM_EXPECTED_USER_ID"
        )
    username = getattr(me, "username", None)
    if username and username.lower() in settings.target_usernames:
        raise RuntimeError("the automation account must not be a target username")

    for chat_id in settings.chat_ids:
        entity = await client.get_entity(chat_id)
        if not _is_managed_group(entity):
            raise RuntimeError(f"configured chat {chat_id} is not a group or supergroup")
        permissions = await client.get_permissions(entity, me)
        is_admin = bool(
            getattr(permissions, "is_admin", False) or getattr(permissions, "is_creator", False)
        )
        can_delete = bool(
            getattr(permissions, "delete_messages", False)
            or getattr(permissions, "is_creator", False)
        )
        if not is_admin or not can_delete:
            raise RuntimeError(f"account lacks administrator delete permission in chat {chat_id}")

    summary = PreflightSummary(
        account_id=account_id,
        chat_count=len(settings.chat_ids),
        target_count=len(settings.targets),
        group_size=settings.group_size,
        threshold=settings.threshold,
        domain_count=len(settings.video_domains),
        dry_run=settings.dry_run,
    )
    log_event(
        logger,
        logging.INFO,
        "preflight_succeeded",
        account_id=summary.account_id,
        chat_count=summary.chat_count,
        target_count=summary.target_count,
        group_size=summary.group_size,
        threshold=summary.threshold,
        domain_count=summary.domain_count,
        dry_run=summary.dry_run,
    )
    return summary
