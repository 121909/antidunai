from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
from typing import Any

from telethon import TelegramClient, events

from burst_guard.cleanup import CleanupService
from burst_guard.config import Settings
from burst_guard.handlers import TelegramUpdateHandler
from burst_guard.health import HealthServer, HealthState
from burst_guard.interaction import InfoReplyService
from burst_guard.logging import configure_logging, log_event
from burst_guard.metrics import Metrics
from burst_guard.notifications import SanctionNotifier
from burst_guard.preflight import run_preflight
from burst_guard.session_lock import SessionLock
from burst_guard.state import BurstStateService


class TelegramDeleteAdapter:
    def __init__(self, client: TelegramClient) -> None:
        self._client = client

    async def delete_messages(self, entity: int, message_ids: Any) -> object:
        return await self._client.delete_messages(entity, message_ids)


class TelegramMessageAdapter:
    def __init__(self, client: TelegramClient) -> None:
        self._client = client

    async def send_message(
        self,
        entity: int,
        message: str,
        *,
        parse_mode: object | None = None,
    ) -> object:
        return await self._client.send_message(entity, message, parse_mode=parse_mode)

    async def send_reply(self, entity: int, message: str, reply_to_message_id: int) -> object:
        return await self._client.send_message(
            entity,
            message,
            parse_mode=None,
            reply_to=reply_to_message_id,
        )


async def _state_janitor(state: BurstStateService, interval: float, logger: logging.Logger) -> None:
    while True:
        await asyncio.sleep(interval)
        removed = await state.cleanup_expired()
        if removed:
            log_event(logger, logging.DEBUG, "idle_state_removed", chat_count=removed)


async def run_service(settings: Settings) -> None:
    logger = configure_logging(settings.log_level)
    metrics = Metrics()
    state = BurstStateService(
        settings.threshold,
        settings.idempotency_ttl_seconds,
        group_size=settings.group_size,
    )
    client = TelegramClient(
        str(settings.telegram_session_path),
        settings.telegram_api_id,
        settings.telegram_api_hash.get_secret_value(),
    )
    cleanup = CleanupService(
        TelegramDeleteAdapter(client),
        batch_size=settings.delete_batch_size,
        retry_limit=settings.delete_retry_limit,
        dry_run=settings.dry_run,
        metrics=metrics,
        logger=logger,
    )
    message_adapter = TelegramMessageAdapter(client)
    notifier = SanctionNotifier(message_adapter, metrics, logger)
    health_state = HealthState(config_valid=True)
    health = HealthServer(
        settings.health_host, settings.health_port, health_state, client.is_connected
    )
    stop_requested = asyncio.Event()
    loop = asyncio.get_running_loop()
    installed_signals: list[signal.Signals] = []

    def request_stop() -> None:
        stop_requested.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, request_stop)
            installed_signals.append(sig)

    janitor: asyncio.Task[None] | None = None
    listener: asyncio.Future[Any] | None = None
    stop_waiter: asyncio.Task[bool] | None = None
    try:
        with SessionLock(settings.telegram_session_path):
            await health.start()
            await run_preflight(client, settings, logger)
            health_state.preflight_passed = True
            me = await client.get_me()
            account_username = getattr(me, "username", None)
            info_reply = InfoReplyService(
                settings.telegram_expected_user_id,
                str(account_username) if account_username else None,
                metrics,
                logger,
                enabled=settings.info_reply_enabled,
                cooldown_seconds=settings.info_reply_cooldown_seconds,
            )
            handler = TelegramUpdateHandler(
                settings,
                state,
                cleanup,
                metrics,
                logger,
                notifier=notifier,
                info_reply=info_reply,
                reply_sender=message_adapter.send_reply,
            )
            client.add_event_handler(handler, events.NewMessage(chats=list(settings.chat_ids)))
            health_state.listener_running = True
            janitor = asyncio.create_task(
                _state_janitor(
                    state,
                    max(1.0, min(300.0, settings.idempotency_ttl_seconds / 2)),
                    logger,
                ),
                name="state-janitor",
            )
            listener = asyncio.ensure_future(client.run_until_disconnected())
            stop_waiter = asyncio.create_task(stop_requested.wait(), name="signal-waiter")
            log_event(
                logger,
                logging.INFO,
                "service_ready",
                enabled=settings.enabled,
                dry_run=settings.dry_run,
                chat_count=len(settings.chat_ids),
            )
            done, _ = await asyncio.wait(
                {listener, stop_waiter}, return_when=asyncio.FIRST_COMPLETED
            )
            if stop_waiter in done:
                await client.disconnect()
            await listener
    finally:
        health_state.listener_running = False
        if stop_waiter is not None:
            stop_waiter.cancel()
        if janitor is not None:
            janitor.cancel()
        for task in (stop_waiter, janitor):
            if task is not None:
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        await health.close()
        if client.is_connected():
            await client.disconnect()
        for sig in installed_signals:
            loop.remove_signal_handler(sig)
        log_event(logger, logging.INFO, "service_stopped", metrics=metrics.snapshot())


def run() -> None:
    settings = Settings()  # type: ignore[call-arg]
    asyncio.run(run_service(settings))
