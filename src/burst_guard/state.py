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


@dataclass(frozen=True, slots=True)
class DiscardedUrl:
    discarded_at: float
    run_id: str


@dataclass(slots=True)
class ChatState:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    active_run: BurstRun | None = None
    recently_seen_message_ids: dict[int, float] = field(default_factory=dict)
    pending_parser_messages: dict[str, dict[int, float]] = field(default_factory=dict)
    discarded_url_keys: dict[str, DiscardedUrl] = field(default_factory=dict)
    protected_url_keys: dict[str, dict[int, float]] = field(default_factory=dict)
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
        candidate_url_keys: frozenset[str] = frozenset(),
    ) -> StateResult:
        state = self.chat_state(chat_id)
        async with state.lock:
            now = self._clock()
            state.last_touched = now
            self._purge_expired(state, now)
            if self._mark_seen(state, message_id, now):
                return StateResult(duplicate=True)

            run_closed = False
            if target_key is None:
                run_closed = self._close_active(state, now)
                return StateResult(run_closed=run_closed)

            if state.active_run is not None and state.active_run.target_key != target_key:
                run_closed = self._close_active(state, now)

            if not is_candidate:
                return StateResult(run_closed=run_closed)

            for url_key in candidate_url_keys:
                state.discarded_url_keys.pop(url_key, None)

            run_started = False
            if state.active_run is None:
                state.active_run = BurstRun(
                    run_id=self._run_id_factory(), chat_id=chat_id, target_key=target_key
                )
                run_started = True
            run = state.active_run
            run.candidate_count += 1
            cleanup = self._register_candidate(state, run, message_id, candidate_url_keys, now)
            return StateResult(
                cleanup=cleanup,
                run_started=run_started,
                run_closed=run_closed,
                candidate_count=run.candidate_count,
                retained_message_id=run.retained_message_id,
                run_id=run.run_id,
            )

    async def process_parser_output(
        self,
        *,
        chat_id: int,
        message_id: int,
        url_keys: frozenset[str],
    ) -> StateResult:
        state = self.chat_state(chat_id)
        async with state.lock:
            now = self._clock()
            state.last_touched = now
            self._purge_expired(state, now)
            if self._mark_seen(state, message_id, now):
                return StateResult(duplicate=True)

            protected_keys = self._active_url_keys(state) | frozenset(state.protected_url_keys)
            discarded = [
                state.discarded_url_keys[key] for key in url_keys if key in state.discarded_url_keys
            ]
            if discarded and not url_keys & protected_keys:
                run_id = min(discarded, key=lambda item: item.discarded_at).run_id
                return StateResult(
                    cleanup=CleanupRequest(
                        chat_id,
                        (message_id,),
                        "parser_output_for_discarded_candidate",
                        run_id,
                    ),
                    run_id=run_id,
                )

            tracked_keys = url_keys & self._active_url_keys(state)
            for url_key in tracked_keys:
                state.pending_parser_messages.setdefault(url_key, {})[message_id] = now
            return StateResult()

    def _register_candidate(
        self,
        state: ChatState,
        run: BurstRun,
        message_id: int,
        url_keys: frozenset[str],
        now: float,
    ) -> CleanupRequest | None:
        count = run.candidate_count
        if count < self.threshold:
            run.warmup_message_ids.append(message_id)
            run.warmup_url_keys[message_id] = url_keys
            return None

        if count == self.threshold:
            candidates = [*run.warmup_message_ids, message_id]
            urls_by_message = {**run.warmup_url_keys, message_id: url_keys}
            retained_index = self._random.randrange(count)
            run.retained_message_id = candidates[retained_index]
            run.retained_url_keys = urls_by_message[run.retained_message_id]
            self._protect_links(
                state,
                run.retained_message_id,
                run.retained_url_keys,
                now,
            )
            run.warmup_message_ids.clear()
            run.warmup_url_keys.clear()
            deleted = tuple(
                candidate for index, candidate in enumerate(candidates) if index != retained_index
            )
            parser_message_ids: set[int] = set()
            for candidate in deleted:
                parser_message_ids.update(
                    self._discard_links(
                        state,
                        urls_by_message[candidate],
                        run.retained_url_keys,
                        run.run_id,
                        now,
                    )
                )
            message_ids = (*deleted, *sorted(parser_message_ids - set(deleted)))
            return CleanupRequest(run.chat_id, message_ids, "threshold_reached", run.run_id)

        if run.retained_message_id is None:
            raise RuntimeError("active run lost its retained message")
        if self._random.randrange(count) == 0:
            deleted = (run.retained_message_id,)
            deleted_url_keys = run.retained_url_keys
            self._unprotect_links(state, run.retained_message_id, deleted_url_keys)
            run.retained_message_id = message_id
            run.retained_url_keys = url_keys
            self._protect_links(state, message_id, url_keys, now)
            reason = "reservoir_replaced"
        else:
            deleted = (message_id,)
            deleted_url_keys = url_keys
            reason = "reservoir_rejected"
        parser_message_ids = self._discard_links(
            state,
            deleted_url_keys,
            run.retained_url_keys,
            run.run_id,
            now,
        )
        message_ids = (*deleted, *sorted(parser_message_ids - set(deleted)))
        return CleanupRequest(run.chat_id, message_ids, reason, run.run_id)

    @staticmethod
    def _active_url_keys(state: ChatState) -> frozenset[str]:
        run = state.active_run
        if run is None:
            return frozenset()
        keys = set(run.retained_url_keys)
        for candidate_keys in run.warmup_url_keys.values():
            keys.update(candidate_keys)
        return frozenset(keys)

    @staticmethod
    def _forget_parser_messages(state: ChatState, message_ids: set[int]) -> None:
        if not message_ids:
            return
        for url_key, messages in list(state.pending_parser_messages.items()):
            for message_id in message_ids:
                messages.pop(message_id, None)
            if not messages:
                del state.pending_parser_messages[url_key]

    def _discard_links(
        self,
        state: ChatState,
        url_keys: frozenset[str],
        protected_url_keys: frozenset[str],
        run_id: str,
        now: float,
    ) -> set[int]:
        parser_message_ids: set[int] = set()
        all_protected_keys = protected_url_keys | frozenset(state.protected_url_keys)
        protected_message_ids: set[int] = set()
        for url_key in all_protected_keys:
            protected_message_ids.update(state.pending_parser_messages.get(url_key, {}))
        for url_key in url_keys - all_protected_keys:
            state.discarded_url_keys[url_key] = DiscardedUrl(now, run_id)
            parser_message_ids.update(state.pending_parser_messages.get(url_key, {}))
        parser_message_ids.difference_update(protected_message_ids)
        self._forget_parser_messages(state, parser_message_ids)
        return parser_message_ids

    @staticmethod
    def _protect_links(
        state: ChatState,
        message_id: int,
        url_keys: frozenset[str],
        now: float,
    ) -> None:
        for url_key in url_keys:
            state.protected_url_keys.setdefault(url_key, {})[message_id] = now
            state.discarded_url_keys.pop(url_key, None)

    @staticmethod
    def _unprotect_links(state: ChatState, message_id: int, url_keys: frozenset[str]) -> None:
        for url_key in url_keys:
            protected = state.protected_url_keys.get(url_key)
            if protected is None:
                continue
            protected.pop(message_id, None)
            if not protected:
                del state.protected_url_keys[url_key]

    def _close_active(self, state: ChatState, now: float) -> bool:
        if state.active_run is None:
            return False
        run = state.active_run
        if run.candidate_count < self.threshold:
            for message_id, url_keys in run.warmup_url_keys.items():
                self._protect_links(state, message_id, url_keys, now)
        run.closed = True
        state.active_run = None
        return True

    @staticmethod
    def _mark_seen(state: ChatState, message_id: int, now: float) -> bool:
        if message_id in state.recently_seen_message_ids:
            return True
        state.recently_seen_message_ids[message_id] = now
        return False

    def _purge_expired(self, state: ChatState, now: float) -> None:
        cutoff = now - self.idempotency_ttl_seconds
        expired = [
            message_id
            for message_id, seen_at in state.recently_seen_message_ids.items()
            if seen_at <= cutoff
        ]
        for message_id in expired:
            del state.recently_seen_message_ids[message_id]
        for url_key, discarded in list(state.discarded_url_keys.items()):
            if discarded.discarded_at <= cutoff:
                del state.discarded_url_keys[url_key]
        for url_key, messages in list(state.pending_parser_messages.items()):
            for message_id, seen_at in list(messages.items()):
                if seen_at <= cutoff:
                    del messages[message_id]
            if not messages:
                del state.pending_parser_messages[url_key]
        for url_key, messages in list(state.protected_url_keys.items()):
            for message_id, protected_at in list(messages.items()):
                if protected_at <= cutoff:
                    del messages[message_id]
            if not messages:
                del state.protected_url_keys[url_key]

    async def cleanup_expired(self) -> int:
        now = self._clock()
        removed = 0
        for chat_id, state in list(self._chats.items()):
            async with state.lock:
                self._purge_expired(state, now)
                idle = now - state.last_touched >= self.idempotency_ttl_seconds
                if (
                    idle
                    and state.active_run is None
                    and not state.recently_seen_message_ids
                    and not state.pending_parser_messages
                    and not state.discarded_url_keys
                    and not state.protected_url_keys
                    and self._chats.get(chat_id) is state
                ):
                    del self._chats[chat_id]
                    removed += 1
        return removed
