from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


@dataclass(frozen=True, slots=True)
class IncomingMessage:
    chat_id: int
    message_id: int
    sender_id: int | None
    sender_username: str | None = None
    text: str | None = None
    caption: str | None = None
    entity_urls: tuple[str, ...] = ()
    has_video: bool = False
    has_video_note: bool = False
    document_mime_type: str | None = None


@dataclass(frozen=True, slots=True)
class CleanupRequest:
    chat_id: int
    message_ids: tuple[int, ...]
    reason: str
    run_id: str


@dataclass(slots=True)
class BurstRun:
    run_id: str
    chat_id: int
    target_key: str
    candidate_count: int = 0
    retained_message_id: int | None = None
    retained_url_keys: frozenset[str] = frozenset()
    warmup_message_ids: list[int] = field(default_factory=list)
    warmup_url_keys: dict[int, frozenset[str]] = field(default_factory=dict)
    closed: bool = False


@dataclass(frozen=True, slots=True)
class StateResult:
    cleanup: CleanupRequest | None = None
    duplicate: bool = False
    run_started: bool = False
    run_closed: bool = False
    candidate_count: int = 0
    retained_message_id: int | None = None
    run_id: str | None = None


class CleanupFailure(StrEnum):
    PERMISSION = "permission"
    INVALID_MESSAGE = "invalid_message"
    RPC = "rpc"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class CleanupReport:
    planned: int = 0
    deleted: int = 0
    failed: int = 0
    retries: int = 0
