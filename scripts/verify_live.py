from __future__ import annotations

import argparse
import asyncio
import json
import logging

from telethon import TelegramClient, events

from burst_guard.app import TelegramDeleteAdapter
from burst_guard.cleanup import CleanupService
from burst_guard.config import Settings
from burst_guard.handlers import TelegramUpdateHandler
from burst_guard.logging import configure_logging
from burst_guard.metrics import Metrics
from burst_guard.preflight import run_preflight
from burst_guard.session_lock import SessionLock
from burst_guard.state import BurstStateService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Listen to a test group and verify counters.")
    parser.add_argument("--duration", type=int, default=180)
    parser.add_argument("--minimum-candidates", type=int, default=3)
    parser.add_argument("--minimum-planned", type=int, default=2)
    parser.add_argument("--allow-real-delete", action="store_true")
    return parser.parse_args()


async def verify(args: argparse.Namespace, settings: Settings) -> int:
    if args.duration < 1 or args.minimum_candidates < 0 or args.minimum_planned < 0:
        raise ValueError("duration and minimum counters must not be negative")
    if not settings.enabled:
        raise RuntimeError("BURST_GUARD_ENABLED must be true for live verification")
    if not settings.dry_run and not args.allow_real_delete:
        raise RuntimeError("refusing real deletion without --allow-real-delete")

    logger = configure_logging(settings.log_level)
    metrics = Metrics()
    client = TelegramClient(
        str(settings.telegram_session_path),
        settings.telegram_api_id,
        settings.telegram_api_hash.get_secret_value(),
    )
    state = BurstStateService(settings.threshold, settings.idempotency_ttl_seconds)
    cleanup = CleanupService(
        TelegramDeleteAdapter(client),
        batch_size=settings.delete_batch_size,
        retry_limit=settings.delete_retry_limit,
        dry_run=settings.dry_run,
        metrics=metrics,
        logger=logger,
    )
    handler = TelegramUpdateHandler(settings, state, cleanup, metrics, logger)

    with SessionLock(settings.telegram_session_path):
        try:
            await run_preflight(client, settings, logger)
            client.add_event_handler(handler, events.NewMessage(chats=list(settings.chat_ids)))
            await asyncio.sleep(args.duration)
        finally:
            if client.is_connected():
                await client.disconnect()

    snapshot = metrics.snapshot()
    print(json.dumps(snapshot, sort_keys=True))
    if snapshot.get("candidate_messages", 0) < args.minimum_candidates:
        return 1
    if snapshot.get("deletions_planned", 0) < args.minimum_planned:
        return 1
    return 0


def main() -> None:
    args = parse_args()
    try:
        settings = Settings()  # type: ignore[call-arg]
        raise SystemExit(asyncio.run(verify(args, settings)))
    except (RuntimeError, ValueError) as exc:
        logging.getLogger("burst_guard").error("live verification failed: %s", exc)
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
