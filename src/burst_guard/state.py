from __future__ import annotations

import asyncio
import random
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol

from burst_guard.models import BurstRun, CleanupRequest, StateResult


class RandomSource(Protocol):
    def randrange(self, stop: int) -> int: ...


@dataclass(slots=True)
class ChatState:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    active_run: BurstRun | None = None
    recently_seen_message_ids: dict[int, float] = field(default_factory=dict)
    last_touched: float = 0.0


class BurstStateService:
    def __init__(
        self,
        threshold: int,
        idempotency_ttl_seconds: int,
        *,
        random_source: RandomSource | None = None,
        clock: Callable[[], float] = time.monotonic,
        run_id_factory: Callable[[], str] = lambda: uuid.uuid4().hex,
    ) -> None:
        if threshold < 3:
            raise ValueError("threshold must be at least 3")
        self.threshold = threshold
        self.idempotency_ttl_seconds = idempotency_ttl_seconds
        self._random = random_source or random.SystemRandom()
        self._clock = clock
        self._run_id_factory = run_id_factory
        self._chats: dict[int, ChatState] = {}

    def chat_state(self, chat_id: int) -> ChatState:
        return self._chats.setdefault(chat_id, ChatState(last_touched=self._clock()))

    async def process(
        self,
        *,
        chat_id: int,
        message_id: int,
        target_key: str | None,
        is_candidate: bool,
    ) -> StateResult:
        state = self.chat_state(chat_id)
        async with state.lock:
            now = self._clock()
            state.last_touched = now
            self._purge_seen(state, now)
            if message_id in state.recently_seen_message_ids:
                return StateResult(duplicate=True)
            state.recently_seen_message_ids[message_id] = now

            run_closed = False
            if target_key is None:
                run_closed = self._close_active(state)
                return StateResult(run_closed=run_closed)

            if state.active_run is not None and state.active_run.target_key != target_key:
                run_closed = self._close_active(state)

            if not is_candidate:
                return StateResult(run_closed=run_closed)

            run_started = False
            if state.active_run is None:
                state.active_run = BurstRun(
                    run_id=self._run_id_factory(), chat_id=chat_id, target_key=target_key
                )
                run_started = True
            run = state.active_run
            run.candidate_count += 1
            cleanup = self._register_candidate(run, message_id)
            return StateResult(
                cleanup=cleanup,
                run_started=run_started,
                run_closed=run_closed,
                candidate_count=run.candidate_count,
                retained_message_id=run.retained_message_id,
                run_id=run.run_id,
            )

    def _register_candidate(self, run: BurstRun, message_id: int) -> CleanupRequest | None:
        count = run.candidate_count
        if count < self.threshold:
            run.warmup_message_ids.append(message_id)
            return None

        if count == self.threshold:
            candidates = [*run.warmup_message_ids, message_id]
            retained_index = self._random.randrange(count)
            run.retained_message_id = candidates[retained_index]
            run.warmup_message_ids.clear()
            deleted = tuple(
                candidate for index, candidate in enumerate(candidates) if index != retained_index
            )
            return CleanupRequest(run.chat_id, deleted, "threshold_reached", run.run_id)

        if run.retained_message_id is None:
            raise RuntimeError("active run lost its retained message")
        if self._random.randrange(count) == 0:
            deleted = (run.retained_message_id,)
            run.retained_message_id = message_id
            reason = "reservoir_replaced"
        else:
            deleted = (message_id,)
            reason = "reservoir_rejected"
        return CleanupRequest(run.chat_id, deleted, reason, run.run_id)

    @staticmethod
    def _close_active(state: ChatState) -> bool:
        if state.active_run is None:
            return False
        state.active_run.closed = True
        state.active_run = None
        return True

    def _purge_seen(self, state: ChatState, now: float) -> None:
        cutoff = now - self.idempotency_ttl_seconds
        expired = [
            message_id
            for message_id, seen_at in state.recently_seen_message_ids.items()
            if seen_at <= cutoff
        ]
        for message_id in expired:
            del state.recently_seen_message_ids[message_id]

    async def cleanup_expired(self) -> int:
        now = self._clock()
        removed = 0
        for chat_id, state in list(self._chats.items()):
            async with state.lock:
                self._purge_seen(state, now)
                idle = now - state.last_touched >= self.idempotency_ttl_seconds
                if (
                    idle
                    and state.active_run is None
                    and not state.recently_seen_message_ids
                    and self._chats.get(chat_id) is state
                ):
                    del self._chats[chat_id]
                    removed += 1
        return removed
