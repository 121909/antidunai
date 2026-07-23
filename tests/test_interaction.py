from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from burst_guard.interaction import INFO_REPLY_TEXT, InfoReplyService
from burst_guard.metrics import Metrics
from burst_guard.models import IncomingMessage


class MessageEntityMention:
    pass


class MessageEntityMentionName:
    def __init__(self, user_id: int) -> None:
        self.user_id = user_id


class FakeMessage:
    def __init__(
        self, message_id: int, *, entities: list[tuple[object, str]] | None = None
    ) -> None:
        self.id = message_id
        self.entities = [entity for entity, _ in entities or []]
        self._entities_text = entities or []

    def get_entities_text(self) -> list[tuple[object, str]]:
        return self._entities_text


class FakeEvent:
    def __init__(
        self,
        message_id: int,
        *,
        chat_id: int = -1001,
        reply_message: object | None = None,
        entities: list[tuple[object, str]] | None = None,
    ) -> None:
        self.chat_id = chat_id
        self.message = FakeMessage(message_id, entities=entities)
        self._reply_message = reply_message

    async def get_reply_message(self) -> object | None:
        return self._reply_message


def incoming(
    message_id: int,
    *,
    chat_id: int = -1001,
    reply_to_message_id: int | None = None,
) -> IncomingMessage:
    return IncomingMessage(
        chat_id=chat_id,
        message_id=message_id,
        sender_id=101,
        reply_to_message_id=reply_to_message_id,
    )


def make_service(*, clock=lambda: 100.0, cooldown_seconds: float = 60.0) -> InfoReplyService:
    return InfoReplyService(
        9001,
        "guard_user",
        Metrics(),
        logging.getLogger("test-interaction"),
        cooldown_seconds=cooldown_seconds,
        clock=clock,
    )


@pytest.mark.asyncio
async def test_mention_sends_fixed_reply() -> None:
    service = make_service()
    sent: list[tuple[int, str, int]] = []

    async def send_reply(chat_id: int, text: str, reply_to: int) -> object:
        sent.append((chat_id, text, reply_to))
        return SimpleNamespace(id=700)

    event = FakeEvent(42, entities=[(MessageEntityMention(), "@guard_user")])

    assert await service.maybe_reply(event, incoming(42), send_reply) is True
    assert sent == [(-1001, INFO_REPLY_TEXT, 42)]


@pytest.mark.asyncio
async def test_reply_to_account_message_sends_fixed_reply() -> None:
    service = make_service()
    sent: list[str] = []

    async def send_reply(_: int, text: str, __: int) -> object:
        sent.append(text)
        return SimpleNamespace(id=701)

    event = FakeEvent(43, reply_message=SimpleNamespace(sender_id=9001))

    assert await service.maybe_reply(event, incoming(43, reply_to_message_id=12), send_reply)
    assert sent == [INFO_REPLY_TEXT]


@pytest.mark.asyncio
async def test_plain_at_text_and_reply_to_other_user_do_not_trigger() -> None:
    service = make_service()
    calls = 0

    async def send_reply(_: int, __: str, ___: int) -> object:
        nonlocal calls
        calls += 1
        return SimpleNamespace(id=702)

    plain_at = FakeEvent(44)
    wrong_reply = FakeEvent(45, reply_message=SimpleNamespace(sender_id=77))

    assert not await service.maybe_reply(plain_at, incoming(44), send_reply)
    assert not await service.maybe_reply(
        wrong_reply, incoming(45, reply_to_message_id=13), send_reply
    )
    assert calls == 0


@pytest.mark.asyncio
async def test_mention_name_entity_matches_account_id() -> None:
    service = make_service()
    sent: list[str] = []

    async def send_reply(_: int, text: str, __: int) -> object:
        sent.append(text)
        return SimpleNamespace(id=703)

    event = FakeEvent(46, entities=[(MessageEntityMentionName(9001), "display name")])

    assert await service.maybe_reply(event, incoming(46), send_reply)
    assert sent == [INFO_REPLY_TEXT]


@pytest.mark.asyncio
async def test_group_cooldown_suppresses_repeated_mentions() -> None:
    now = [100.0]
    service = make_service(clock=lambda: now[0])
    sent: list[int] = []

    async def send_reply(_: int, __: str, reply_to: int) -> object:
        sent.append(reply_to)
        return SimpleNamespace(id=704 + len(sent))

    first = FakeEvent(47, entities=[(MessageEntityMention(), "@guard_user")])
    second = FakeEvent(48, entities=[(MessageEntityMention(), "@guard_user")])

    assert await service.maybe_reply(first, incoming(47), send_reply)
    assert not await service.maybe_reply(second, incoming(48), send_reply)
    now[0] = 161.0
    assert await service.maybe_reply(second, incoming(48), send_reply)
    assert sent == [47, 48]


@pytest.mark.asyncio
async def test_failed_send_releases_cooldown() -> None:
    service = make_service()
    attempts = 0

    async def send_reply(_: int, __: str, ___: int) -> object:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("network")
        return SimpleNamespace(id=705)

    event = FakeEvent(49, entities=[(MessageEntityMention(), "@guard_user")])

    assert not await service.maybe_reply(event, incoming(49), send_reply)
    assert await service.maybe_reply(event, incoming(49), send_reply)
    assert attempts == 2
