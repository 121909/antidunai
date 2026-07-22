from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Protocol


class RandomSource(Protocol):
    def randrange(self, stop: int) -> int: ...


class BurstItemStatus(StrEnum):
    PENDING = "pending"
    RETAINED = "retained"
    EVICTED = "evicted"


@dataclass(slots=True)
class BurstItem:
    chat_id: int
    user_id: int
    run_id: int
    source_message_id: int
    status: BurstItemStatus = BurstItemStatus.PENDING
    output_message_ids: set[int] = field(default_factory=set)
    tasks: set[asyncio.Future[Any]] = field(default_factory=set)
    processing_complete: bool = False
    cleanup_complete: bool = False


@dataclass(slots=True)
class BurstRun:
    chat_id: int
    user_id: int
    run_id: int
    candidate_count: int = 0
    current_retained_source_message_id: int | None = None
    closed: bool = False
    items: dict[int, BurstItem] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class BurstCleanup:
    chat_id: int
    source_message_id: int
    message_ids: tuple[int, ...]
    tasks: tuple[asyncio.Future[Any], ...]

    @classmethod
    def from_item(cls, item: BurstItem) -> BurstCleanup:
        message_ids = tuple(sorted({item.source_message_id, *item.output_message_ids}))
        return cls(
            chat_id=item.chat_id,
            source_message_id=item.source_message_id,
            message_ids=message_ids,
            tasks=tuple(item.tasks),
        )

    def cancel_tasks(self) -> None:
        for task in self.tasks:
            if not task.done():
                task.cancel()


@dataclass(frozen=True, slots=True)
class CandidateRegistration:
    chat_id: int
    user_id: int
    run_id: int
    source_message_id: int
    candidate_count: int
    status: BurstItemStatus
    evictions: tuple[BurstCleanup, ...] = ()
    is_new: bool = True


@dataclass(frozen=True, slots=True)
class OutputRegistration:
    tracked: bool
    delete_immediately: bool
    message_ids: tuple[int, ...] = ()


@dataclass(frozen=True, slots=True)
class BurstRunSnapshot:
    chat_id: int
    user_id: int
    run_id: int
    candidate_count: int
    current_retained_source_message_id: int | None
    closed: bool
    item_statuses: tuple[tuple[int, BurstItemStatus], ...]


@dataclass(slots=True)
class _GroupState:
    next_run_id: int = 1
    current_run_id: int | None = None
    runs: dict[int, BurstRun] = field(default_factory=dict)


DeleteMessages = Callable[[int, tuple[int, ...]], Awaitable[None]]


class OutputTracker:
    """Records parse outputs and removes outputs that arrive after eviction."""

    def __init__(
        self,
        service: BurstGuardService,
        chat_id: int,
        source_message_id: int,
        delete_messages: DeleteMessages,
    ) -> None:
        self._service = service
        self._chat_id = chat_id
        self._source_message_id = source_message_id
        self._delete_messages = delete_messages

    async def track(self, message_ids: Iterable[int]) -> OutputRegistration:
        registration = await self._service.record_output(
            self._chat_id,
            self._source_message_id,
            message_ids,
        )
        if registration.delete_immediately and registration.message_ids:
            # Telegram I/O deliberately happens after record_output releases the group lock.
            await self._delete_messages(self._chat_id, registration.message_ids)
        return registration


class BurstGuardService:
    """Concurrency-safe in-memory state for burst candidate runs."""

    def __init__(self, threshold: int = 3, rng: RandomSource | None = None) -> None:
        if threshold < 3:
            raise ValueError("burst guard threshold must be at least 3")
        self.threshold = threshold
        self._rng = rng or random.SystemRandom()
        self._groups: dict[int, _GroupState] = {}
        self._locks: dict[int, asyncio.Lock] = {}
        self._items: dict[tuple[int, int], BurstItem] = {}

    def output_tracker(
        self,
        chat_id: int,
        source_message_id: int,
        delete_messages: DeleteMessages,
    ) -> OutputTracker:
        return OutputTracker(self, chat_id, source_message_id, delete_messages)

    async def candidate_output_tracker(
        self,
        chat_id: int,
        source_message_id: int,
        delete_messages: DeleteMessages,
    ) -> OutputTracker | None:
        item_key = (chat_id, source_message_id)
        if item_key not in self._items:
            return None
        lock = self._lock_for(chat_id)
        async with lock:
            if item_key not in self._items:
                return None
            return OutputTracker(self, chat_id, source_message_id, delete_messages)

    async def register_candidate(
        self,
        chat_id: int,
        user_id: int,
        source_message_id: int,
    ) -> CandidateRegistration:
        lock = self._lock_for(chat_id)
        async with lock:
            existing = self._items.get((chat_id, source_message_id))
            if existing is not None:
                existing_run = self._groups[chat_id].runs[existing.run_id]
                return self._registration(existing, existing_run, (), is_new=False)

            group = self._groups.setdefault(chat_id, _GroupState())
            run = self._current_run(group)
            if run is None or run.closed or run.user_id != user_id:
                if run is not None and not run.closed:
                    self._close_run_locked(group, run)
                run = self._new_run(chat_id, user_id, group)

            item = BurstItem(
                chat_id=chat_id,
                user_id=user_id,
                run_id=run.run_id,
                source_message_id=source_message_id,
            )
            run.items[source_message_id] = item
            run.candidate_count += 1
            self._items[(chat_id, source_message_id)] = item

            evictions = self._select_retained_locked(run, item)
            return self._registration(item, run, evictions)

    async def close_run(self, chat_id: int) -> BurstRunSnapshot | None:
        lock = self._lock_for(chat_id)
        async with lock:
            group = self._groups.get(chat_id)
            if group is None:
                return None
            run = self._current_run(group)
            if run is None:
                return None
            self._close_run_locked(group, run)
            snapshot = self._snapshot(run)
            self._prune_run_locked(group, run)
            return snapshot

    async def close_run_for_other_user(self, chat_id: int, user_id: int) -> BurstRunSnapshot | None:
        lock = self._lock_for(chat_id)
        async with lock:
            group = self._groups.get(chat_id)
            if group is None:
                return None
            run = self._current_run(group)
            if run is None or run.user_id == user_id:
                return None
            self._close_run_locked(group, run)
            snapshot = self._snapshot(run)
            self._prune_run_locked(group, run)
            return snapshot

    async def record_output(
        self,
        chat_id: int,
        source_message_id: int,
        message_ids: Iterable[int],
    ) -> OutputRegistration:
        incoming = tuple(dict.fromkeys(message_ids))
        item_key = (chat_id, source_message_id)
        if not incoming or item_key not in self._items:
            return OutputRegistration(tracked=False, delete_immediately=False)

        lock = self._lock_for(chat_id)
        async with lock:
            item = self._items.get((chat_id, source_message_id))
            if item is None:
                return OutputRegistration(tracked=False, delete_immediately=False)
            new_ids = tuple(message_id for message_id in incoming if message_id not in item.output_message_ids)
            item.output_message_ids.update(new_ids)
            evicted = item.status is BurstItemStatus.EVICTED
            return OutputRegistration(
                tracked=True,
                delete_immediately=evicted,
                message_ids=new_ids if evicted else (),
            )

    async def attach_task(
        self,
        chat_id: int,
        source_message_id: int,
        task: asyncio.Future[Any],
    ) -> bool:
        if (chat_id, source_message_id) not in self._items:
            return False
        lock = self._lock_for(chat_id)
        async with lock:
            item = self._items.get((chat_id, source_message_id))
            if item is None:
                return False
            item.tasks.add(task)
            should_continue = item.status is not BurstItemStatus.EVICTED
        if not should_continue and not task.done():
            task.cancel()
        return should_continue

    async def detach_task(
        self,
        chat_id: int,
        source_message_id: int,
        task: asyncio.Future[Any],
    ) -> None:
        if (chat_id, source_message_id) not in self._items:
            return
        lock = self._lock_for(chat_id)
        async with lock:
            item = self._items.get((chat_id, source_message_id))
            if item is None:
                return
            item.tasks.discard(task)
            self._prune_item_locked(item)

    async def finish_processing(self, chat_id: int, source_message_id: int) -> None:
        if (chat_id, source_message_id) not in self._items:
            return
        lock = self._lock_for(chat_id)
        async with lock:
            item = self._items.get((chat_id, source_message_id))
            if item is None:
                return
            item.processing_complete = True
            self._prune_item_locked(item)

    async def finish_cleanup(self, chat_id: int, source_message_id: int) -> None:
        if (chat_id, source_message_id) not in self._items:
            return
        lock = self._lock_for(chat_id)
        async with lock:
            item = self._items.get((chat_id, source_message_id))
            if item is None:
                return
            item.cleanup_complete = True
            self._prune_item_locked(item)

    async def get_run(self, chat_id: int) -> BurstRunSnapshot | None:
        lock = self._lock_for(chat_id)
        async with lock:
            group = self._groups.get(chat_id)
            if group is None:
                return None
            run = self._current_run(group)
            return self._snapshot(run) if run is not None else None

    def _lock_for(self, chat_id: int) -> asyncio.Lock:
        return self._locks.setdefault(chat_id, asyncio.Lock())

    @staticmethod
    def _current_run(group: _GroupState) -> BurstRun | None:
        if group.current_run_id is None:
            return None
        return group.runs.get(group.current_run_id)

    @staticmethod
    def _snapshot(run: BurstRun) -> BurstRunSnapshot:
        return BurstRunSnapshot(
            chat_id=run.chat_id,
            user_id=run.user_id,
            run_id=run.run_id,
            candidate_count=run.candidate_count,
            current_retained_source_message_id=run.current_retained_source_message_id,
            closed=run.closed,
            item_statuses=tuple(sorted((message_id, item.status) for message_id, item in run.items.items())),
        )

    @staticmethod
    def _registration(
        item: BurstItem,
        run: BurstRun,
        evictions: tuple[BurstCleanup, ...],
        *,
        is_new: bool = True,
    ) -> CandidateRegistration:
        return CandidateRegistration(
            chat_id=item.chat_id,
            user_id=item.user_id,
            run_id=item.run_id,
            source_message_id=item.source_message_id,
            candidate_count=run.candidate_count,
            status=item.status,
            evictions=evictions,
            is_new=is_new,
        )

    @staticmethod
    def _new_run(chat_id: int, user_id: int, group: _GroupState) -> BurstRun:
        run = BurstRun(chat_id=chat_id, user_id=user_id, run_id=group.next_run_id)
        group.next_run_id += 1
        group.current_run_id = run.run_id
        group.runs[run.run_id] = run
        return run

    def _select_retained_locked(
        self,
        run: BurstRun,
        new_item: BurstItem,
    ) -> tuple[BurstCleanup, ...]:
        count = run.candidate_count
        if count < self.threshold:
            return ()

        if count == self.threshold:
            selected_index = self._rng.randrange(count)
            selected_source_id = tuple(run.items)[selected_index]
            run.current_retained_source_message_id = selected_source_id
            evicted: list[BurstCleanup] = []
            for item in run.items.values():
                if item.source_message_id == selected_source_id:
                    item.status = BurstItemStatus.RETAINED
                else:
                    evicted.append(self._evict_locked(item))
            return tuple(evicted)

        if self._rng.randrange(count) == 0:
            retained_source_id = run.current_retained_source_message_id
            if retained_source_id is None:
                raise RuntimeError("triggered burst run has no retained item")
            retained_item = run.items[retained_source_id]
            cleanup = self._evict_locked(retained_item)
            new_item.status = BurstItemStatus.RETAINED
            run.current_retained_source_message_id = new_item.source_message_id
            return (cleanup,)

        return (self._evict_locked(new_item),)

    @staticmethod
    def _evict_locked(item: BurstItem) -> BurstCleanup:
        item.status = BurstItemStatus.EVICTED
        return BurstCleanup.from_item(item)

    def _close_run_locked(self, group: _GroupState, run: BurstRun) -> None:
        run.closed = True
        if group.current_run_id == run.run_id:
            group.current_run_id = None
        self._prune_run_locked(group, run)

    def _prune_item_locked(self, item: BurstItem) -> None:
        if not self._can_prune(item):
            return
        group = self._groups.get(item.chat_id)
        if group is None:
            return
        run = group.runs.get(item.run_id)
        if run is None or (not run.closed and item.status is not BurstItemStatus.EVICTED):
            return
        run.items.pop(item.source_message_id, None)
        self._items.pop((item.chat_id, item.source_message_id), None)
        self._prune_run_locked(group, run)

    def _prune_run_locked(self, group: _GroupState, run: BurstRun) -> None:
        if not run.closed:
            return
        for item in tuple(run.items.values()):
            if self._can_prune(item):
                run.items.pop(item.source_message_id, None)
                self._items.pop((item.chat_id, item.source_message_id), None)
        if not run.items:
            group.runs.pop(run.run_id, None)

    @staticmethod
    def _can_prune(item: BurstItem) -> bool:
        if not item.processing_complete or item.tasks:
            return False
        return item.status is not BurstItemStatus.EVICTED or item.cleanup_complete
