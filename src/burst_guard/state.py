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
        group_size: int | None = None,
        random_source: RandomSource | None = None,
        clock: Callable[[], float] = time.monotonic,
        run_id_factory: Callable[[], str] = lambda: uuid.uuid4().hex,
    ) -> None:
        if threshold < 1:
            raise ValueError("threshold must be at least 1")
        resolved_group_size = threshold if group_size is None else group_size
        if resolved_group_size < threshold:
            raise ValueError("group size must be greater than or equal to threshold")
        self.threshold = threshold
        self.group_size = resolved_group_size
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

            run_started = False
            if state.active_run is None:
                state.active_run = BurstRun(
                    run_id=self._run_id_factory(),
                    chat_id=chat_id,
                    target_key="fixed-group",
                )
                run_started = True
            run = state.active_run
            run.message_count += 1
            if target_key is not None and is_candidate:
                run.candidate_count += 1
                run.warmup_message_ids.append(message_id)
                run.warmup_url_keys[message_id] = candidate_url_keys

            run_closed = run.message_count == self.group_size
            cleanup = None
            if run_closed:
                cleanup = self._finish_group(state, run, now)
                run.closed = True
                state.active_run = None
            return StateResult(
                cleanup=cleanup,
                run_started=run_started,
                run_closed=run_closed,
                message_count=run.message_count,
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

    def _finish_group(
        self,
        state: ChatState,
        run: BurstRun,
        now: float,
    ) -> CleanupRequest | None:
        candidates = list(run.warmup_message_ids)
        if len(candidates) < self.threshold:
            for candidate in candidates:
                self._protect_links(
                    state,
                    candidate,
                    run.warmup_url_keys[candidate],
                    now,
                )
            return None

        retained_index = self._random.randrange(len(candidates))
        run.retained_message_id = candidates[retained_index]
        run.retained_url_keys = run.warmup_url_keys[run.retained_message_id]
        self._protect_links(
            state,
            run.retained_message_id,
            run.retained_url_keys,
            now,
        )
        deleted = tuple(
            candidate for index, candidate in enumerate(candidates) if index != retained_index
        )
        parser_message_ids: set[int] = set()
        for candidate in deleted:
            parser_message_ids.update(
                self._discard_links(
                    state,
                    run.warmup_url_keys[candidate],
                    run.retained_url_keys,
                    run.run_id,
                    now,
                )
            )
        message_ids = (*deleted, *sorted(parser_message_ids - set(deleted)))
        if not message_ids:
            return None
        return CleanupRequest(
            run.chat_id,
            message_ids,
            "fixed_group_threshold_reached",
            run.run_id,
        )

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
