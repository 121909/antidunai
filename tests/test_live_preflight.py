from types import SimpleNamespace
from typing import Any

import pytest
from pyrogram import enums

from scripts.verify_burst_guard_live import LivePreflightError, validate_live_settings, verify_live_prerequisites


def settings(**overrides: Any) -> SimpleNamespace:
    values: dict[str, Any] = {
        "burst_guard_enabled": True,
        "burst_guard_threshold": 3,
        "burst_guard_targets": frozenset({"123", "target_user"}),
        "burst_guard_video_platforms": frozenset({"youtube"}),
    }
    values.update(overrides)
    return SimpleNamespace(**values)


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"burst_guard_enabled": False}, "BURST_GUARD_ENABLED"),
        ({"burst_guard_threshold": 2}, "BURST_GUARD_THRESHOLD"),
        ({"burst_guard_targets": frozenset()}, "BURST_GUARD_TARGETS"),
        ({"burst_guard_video_platforms": frozenset()}, "BURST_GUARD_VIDEO_PLATFORMS"),
    ],
)
def test_live_settings_fail_closed(overrides: dict[str, Any], expected: str) -> None:
    with pytest.raises(LivePreflightError, match=expected):
        validate_live_settings(settings(**overrides))


@pytest.mark.asyncio
async def test_live_preflight_checks_group_permissions_and_targets_without_mutation() -> None:
    class FakeClient:
        def __init__(self) -> None:
            self.read_calls: list[tuple[str, object]] = []

        async def get_chat(self, chat_id: int) -> SimpleNamespace:
            self.read_calls.append(("get_chat", chat_id))
            return SimpleNamespace(type=enums.ChatType.SUPERGROUP)

        async def get_me(self) -> SimpleNamespace:
            self.read_calls.append(("get_me", 0))
            return SimpleNamespace(id=999)

        async def get_users(self, peer: int | str) -> SimpleNamespace:
            self.read_calls.append(("get_users", peer))
            return SimpleNamespace(id=peer if isinstance(peer, int) else 456)

        async def get_chat_member(self, chat_id: int, user_id: int) -> SimpleNamespace:
            self.read_calls.append(("get_chat_member", user_id))
            if user_id == 999:
                return SimpleNamespace(
                    status=enums.ChatMemberStatus.ADMINISTRATOR,
                    privileges=SimpleNamespace(can_delete_messages=True),
                )
            return SimpleNamespace(status=enums.ChatMemberStatus.MEMBER)

    client = FakeClient()

    checks = await verify_live_prerequisites(client, settings(), -100)

    assert len(checks) == 4
    assert {call[0] for call in client.read_calls} == {"get_chat", "get_me", "get_users", "get_chat_member"}


@pytest.mark.asyncio
async def test_live_preflight_rejects_missing_delete_permission() -> None:
    class FakeClient:
        async def get_chat(self, _: int) -> SimpleNamespace:
            return SimpleNamespace(type=enums.ChatType.SUPERGROUP)

        async def get_me(self) -> SimpleNamespace:
            return SimpleNamespace(id=999)

        async def get_chat_member(self, _: int, __: int) -> SimpleNamespace:
            return SimpleNamespace(
                status=enums.ChatMemberStatus.ADMINISTRATOR,
                privileges=SimpleNamespace(can_delete_messages=False),
            )

    with pytest.raises(LivePreflightError, match="can_delete_messages"):
        await verify_live_prerequisites(FakeClient(), settings(), -100)
