from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from typing import Protocol

from telethon.errors import (
    ChatAdminRequiredError,
    FloodWaitError,
    MessageDeleteForbiddenError,
    MessageIdInvalidError,
    RPCError,
)

from burst_guard.logging import log_event
from burst_guard.metrics import Metrics
from burst_guard.models import CleanupFailure, CleanupReport, CleanupRequest


class DeleteClient(Protocol):
    async def delete_messages(self, entity: int, message_ids: Sequence[int]) -> object: ...


class CleanupService:
    def __init__(
        self,
        client: DeleteClient,
        *,
        batch_size: int,
        retry_limit: int,
        dry_run: bool,
        metrics: Metrics,
        logger: logging.Logger,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if not 1 <= batch_size <= 100:
            raise ValueError("batch size must be between 1 and 100")
        if retry_limit < 0:
            raise ValueError("retry limit must not be negative")
        self._client = client
        self._batch_size = batch_size
        self._retry_limit = retry_limit
        self._dry_run = dry_run
        self._metrics = metrics
        self._logger = logger
        self._sleep = sleep

    async def execute(self, request: CleanupRequest) -> CleanupReport:
        total = len(request.message_ids)
        self._metrics.increment("deletions_planned", total)
        log_event(
            self._logger,
            logging.INFO,
            "cleanup_planned",
            chat_id=request.chat_id,
            run_id=request.run_id,
            delete_count=total,
            dry_run=self._dry_run,
            reason=request.reason,
        )
        if self._dry_run:
            return CleanupReport(planned=total)

        deleted = failed = retries = 0
        for offset in range(0, total, self._batch_size):
            batch = request.message_ids[offset : offset + self._batch_size]
            report = await self._delete_batch(request, batch)
            deleted += report.deleted
            failed += report.failed
            retries += report.retries
        return CleanupReport(planned=total, deleted=deleted, failed=failed, retries=retries)

    async def _delete_batch(self, request: CleanupRequest, batch: tuple[int, ...]) -> CleanupReport:
        retries = 0
        while True:
            try:
                await self._client.delete_messages(request.chat_id, batch)
            except FloodWaitError as exc:
                if retries >= self._retry_limit:
                    return self._record_failure(
                        request, batch, CleanupFailure.RPC, retries, type(exc).__name__
                    )
                retries += 1
                self._metrics.increment("flood_wait_retries")
                wait_seconds = max(0, int(exc.seconds))
                log_event(
                    self._logger,
                    logging.WARNING,
                    "cleanup_flood_wait",
                    chat_id=request.chat_id,
                    run_id=request.run_id,
                    delete_count=len(batch),
                    retry=retries,
                    wait_seconds=wait_seconds,
                    error_type=type(exc).__name__,
                )
                await self._sleep(wait_seconds)
                continue
            except MessageIdInvalidError as exc:
                return self._record_failure(
                    request,
                    batch,
                    CleanupFailure.INVALID_MESSAGE,
                    retries,
                    type(exc).__name__,
                )
            except (ChatAdminRequiredError, MessageDeleteForbiddenError) as exc:
                return self._record_failure(
                    request, batch, CleanupFailure.PERMISSION, retries, type(exc).__name__
                )
            except RPCError as exc:
                return self._record_failure(
                    request, batch, CleanupFailure.RPC, retries, type(exc).__name__
                )
            except Exception as exc:
                return self._record_failure(
                    request, batch, CleanupFailure.UNKNOWN, retries, type(exc).__name__
                )
            else:
                self._metrics.increment("deletions_succeeded", len(batch))
                log_event(
                    self._logger,
                    logging.INFO,
                    "cleanup_succeeded",
                    chat_id=request.chat_id,
                    run_id=request.run_id,
                    delete_count=len(batch),
                )
                return CleanupReport(planned=len(batch), deleted=len(batch), retries=retries)

    def _record_failure(
        self,
        request: CleanupRequest,
        batch: tuple[int, ...],
        failure: CleanupFailure,
        retries: int,
        error_type: str,
    ) -> CleanupReport:
        self._metrics.increment("deletions_failed", len(batch))
        log_event(
            self._logger,
            logging.ERROR,
            "cleanup_failed",
            chat_id=request.chat_id,
            run_id=request.run_id,
            delete_count=len(batch),
            failure=failure.value,
            error_type=error_type,
        )
        return CleanupReport(planned=len(batch), failed=len(batch), retries=retries)
