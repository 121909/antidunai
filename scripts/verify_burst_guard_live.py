from __future__ import annotations

import asyncio
import os
from typing import Any

from dotenv import load_dotenv
from pyrogram import Client, enums

_REQUIRED_ENV = ("API_ID", "API_HASH", "BOT_TOKEN", "BURST_GUARD_TEST_CHAT_ID")
_GROUP_TYPES = frozenset({enums.ChatType.GROUP, enums.ChatType.SUPERGROUP})
_ACTIVE_MEMBER_STATUSES = frozenset(
    {
        enums.ChatMemberStatus.OWNER,
        enums.ChatMemberStatus.ADMINISTRATOR,
        enums.ChatMemberStatus.MEMBER,
        enums.ChatMemberStatus.RESTRICTED,
    }
)


class LivePreflightError(RuntimeError):
    pass


def validate_live_settings(settings: Any) -> None:
    if not settings.burst_guard_enabled:
        raise LivePreflightError("BURST_GUARD_ENABLED must be true")
    if settings.burst_guard_threshold < 3:
        raise LivePreflightError("BURST_GUARD_THRESHOLD must be at least 3")
    if not settings.burst_guard_targets:
        raise LivePreflightError("BURST_GUARD_TARGETS must contain at least one target")
    if not settings.burst_guard_video_platforms:
        raise LivePreflightError("BURST_GUARD_VIDEO_PLATFORMS must contain at least one platform")


async def verify_live_prerequisites(client: Any, settings: Any, chat_id: int) -> tuple[str, ...]:
    validate_live_settings(settings)
    chat = await client.get_chat(chat_id)
    if chat.type not in _GROUP_TYPES:
        raise LivePreflightError("BURST_GUARD_TEST_CHAT_ID must identify a group or supergroup")

    bot_user = await client.get_me()
    bot_member = await client.get_chat_member(chat_id, bot_user.id)
    if bot_member.status not in {enums.ChatMemberStatus.OWNER, enums.ChatMemberStatus.ADMINISTRATOR}:
        raise LivePreflightError("the bot must be an administrator in the test group")
    if bot_member.status is enums.ChatMemberStatus.ADMINISTRATOR:
        privileges = bot_member.privileges
        if chat.type is enums.ChatType.SUPERGROUP and (
            privileges is None or not privileges.can_delete_messages
        ):
            raise LivePreflightError("the bot needs can_delete_messages in the test supergroup")

    for target in sorted(settings.burst_guard_targets):
        peer: int | str = int(target) if target.isdecimal() else target
        user = await client.get_users(peer)
        member = await client.get_chat_member(chat_id, user.id)
        if member.status not in _ACTIVE_MEMBER_STATUSES:
            raise LivePreflightError(f"configured target @{target} is not an active test-group member")

    return (
        "burst guard configuration enabled",
        "test chat is a group",
        "bot administrator deletion permission available",
        "configured targets are active group members",
    )


async def _run() -> int:
    load_dotenv()
    missing = [name for name in _REQUIRED_ENV if not os.getenv(name)]
    if missing:
        print(f"FAILED: missing environment variables: {', '.join(missing)}")
        return 2

    from core import bs

    chat_id = int(os.environ["BURST_GUARD_TEST_CHAT_ID"])
    client = Client(
        "burst-guard-live-preflight",
        api_id=int(bs.api_id),
        api_hash=bs.api_hash,
        bot_token=bs.bot_token,
        in_memory=True,
        no_updates=True,
    )
    try:
        async with client:
            checks = await verify_live_prerequisites(client, bs, chat_id)
    except Exception as error:
        print(f"FAILED: {error}")
        return 1

    for check in checks:
        print(f"PASS: {check}")
    print("PASS: prerequisites are ready; execute the seven manual group scenarios in DEVELOPMENT_PLAN.md")
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(_run()))


if __name__ == "__main__":
    main()
