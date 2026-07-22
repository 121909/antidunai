from __future__ import annotations

import logging
import os
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from telethon import types

from burst_guard.config import Settings
from burst_guard.preflight import _is_managed_group, run_preflight, validate_session_file


def group() -> types.Chat:
    return types.Chat(
        id=1,
        title="test",
        photo=types.ChatPhotoEmpty(),
        participants_count=2,
        date=None,
        version=1,
    )


class FakeClient:
    def __init__(
        self,
        *,
        authorized: bool = True,
        me: object | None = None,
        entity: object | None = None,
        permissions: object | None = None,
    ) -> None:
        self.authorized = authorized
        self.me = me or SimpleNamespace(id=9001, bot=False, username="guard_account")
        self.entity = entity or group()
        self.permissions = permissions or SimpleNamespace(
            is_admin=True, is_creator=False, delete_messages=True
        )
        self.connected = False
        self.requested_chats: list[int] = []

    async def connect(self) -> None:
        self.connected = True

    async def is_user_authorized(self) -> bool:
        return self.authorized

    async def get_me(self) -> Any:
        return self.me

    async def get_entity(self, entity: int) -> Any:
        self.requested_chats.append(entity)
        return self.entity

    async def get_permissions(self, entity: Any, user: Any) -> Any:
        return self.permissions


def secure_session(settings: Settings) -> Path:
    path = settings.telegram_session_path
    path.touch()
    os.chmod(path, 0o600)
    return path


@pytest.mark.asyncio
async def test_preflight_checks_account_and_every_chat(
    settings_factory: Callable[..., Settings],
) -> None:
    settings = settings_factory()
    secure_session(settings)
    client = FakeClient()

    summary = await run_preflight(client, settings, logging.getLogger("test-preflight"))

    assert summary.account_id == 9001
    assert summary.chat_count == 2
    assert summary.window_size == 10
    assert summary.threshold == 3
    assert set(client.requested_chats) == {-1001, -1002}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("client", "error"),
    [
        (FakeClient(authorized=False), "not authorized"),
        (FakeClient(me=SimpleNamespace(id=1, bot=False, username="guard")), "does not match"),
        (FakeClient(me=SimpleNamespace(id=9001, bot=True, username="guard")), "not a bot"),
        (FakeClient(entity=SimpleNamespace()), "not a group"),
        (
            FakeClient(
                permissions=SimpleNamespace(is_admin=True, is_creator=False, delete_messages=False)
            ),
            "lacks administrator",
        ),
    ],
)
async def test_preflight_rejects_invalid_runtime_state(
    settings_factory: Callable[..., Settings], client: FakeClient, error: str
) -> None:
    settings = settings_factory()
    secure_session(settings)

    with pytest.raises(RuntimeError, match=error):
        await run_preflight(client, settings, logging.getLogger("test-preflight"))


def test_session_permissions_must_be_private(
    settings_factory: Callable[..., Settings],
) -> None:
    settings = settings_factory()
    path = secure_session(settings)
    os.chmod(path, 0o644)

    with pytest.raises(RuntimeError, match="group/world"):
        validate_session_file(path)


def test_only_groups_and_megagroups_are_allowed() -> None:
    megagroup = types.Channel(
        id=1,
        title="supergroup",
        photo=types.ChatPhotoEmpty(),
        date=None,
        megagroup=True,
        broadcast=False,
    )
    broadcast = types.Channel(
        id=2,
        title="channel",
        photo=types.ChatPhotoEmpty(),
        date=None,
        megagroup=False,
        broadcast=True,
    )

    assert _is_managed_group(group())
    assert _is_managed_group(megagroup)
    assert not _is_managed_group(broadcast)
