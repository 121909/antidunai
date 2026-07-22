from __future__ import annotations

import re
from pathlib import Path
from typing import Annotated, Any
from urllib.parse import urlsplit

from pydantic import AliasChoices, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

CsvSet = Annotated[frozenset[str], NoDecode]
CsvIntSet = Annotated[frozenset[int], NoDecode]

_USERNAME_RE = re.compile(r"^[A-Za-z0-9_]{3,64}$")


def _csv(value: Any) -> list[str]:
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, (list, tuple, set, frozenset)):
        return [str(item).strip() for item in value if str(item).strip()]
    raise ValueError("must be a comma-separated list")


def normalize_username(value: str) -> str:
    username = value.strip().removeprefix("@").lower()
    if not _USERNAME_RE.fullmatch(username):
        raise ValueError(f"invalid Telegram username: {value!r}")
    return username


def normalize_domain(value: str) -> str:
    raw = value.strip().lower().rstrip(".")
    if not raw or "://" in raw or "/" in raw:
        raise ValueError(f"invalid domain: {value!r}")
    parsed = urlsplit(f"//{raw}")
    _ = parsed.port  # Access validates malformed ports before hostname normalization.
    raw = parsed.hostname or ""
    if not raw:
        raise ValueError(f"invalid domain: {value!r}")
    try:
        return raw.encode("idna").decode("ascii").lower().rstrip(".")
    except UnicodeError as exc:
        raise ValueError(f"invalid domain: {value!r}") from exc


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=True,
        populate_by_name=True,
    )

    telegram_api_id: int = Field(validation_alias="TELEGRAM_API_ID", gt=0)
    telegram_api_hash: SecretStr = Field(validation_alias="TELEGRAM_API_HASH", min_length=1)
    telegram_expected_user_id: int = Field(validation_alias="TELEGRAM_EXPECTED_USER_ID", gt=0)
    telegram_session_path: Path = Field(validation_alias="TELEGRAM_SESSION_PATH")

    enabled: bool = Field(default=False, validation_alias="BURST_GUARD_ENABLED")
    dry_run: bool = Field(default=True, validation_alias="BURST_GUARD_DRY_RUN")
    chat_ids: CsvIntSet = Field(validation_alias="BURST_GUARD_CHAT_IDS", min_length=1)
    targets: CsvSet = Field(validation_alias="BURST_GUARD_TARGETS", min_length=1)
    group_size: int = Field(
        default=10,
        validation_alias=AliasChoices("BURST_GUARD_WINDOW_SIZE", "BURST_GUARD_GROUP_SIZE"),
        ge=1,
    )
    threshold: int = Field(default=3, validation_alias="BURST_GUARD_THRESHOLD", ge=1)
    video_domains: CsvSet = Field(default=frozenset(), validation_alias="BURST_GUARD_VIDEO_DOMAINS")
    delete_batch_size: int = Field(
        default=100,
        validation_alias="BURST_GUARD_DELETE_BATCH_SIZE",
        ge=1,
        le=100,
    )
    delete_retry_limit: int = Field(
        default=3, validation_alias="BURST_GUARD_DELETE_RETRY_LIMIT", ge=0, le=20
    )
    idempotency_ttl_seconds: int = Field(
        default=3600, validation_alias="BURST_GUARD_IDEMPOTENCY_TTL_SECONDS", ge=1
    )
    log_level: str = Field(default="INFO", validation_alias="BURST_GUARD_LOG_LEVEL")
    health_host: str = Field(default="0.0.0.0", validation_alias="BURST_GUARD_HEALTH_HOST")
    health_port: int = Field(
        default=8080, validation_alias="BURST_GUARD_HEALTH_PORT", ge=1, le=65535
    )

    @field_validator("chat_ids", mode="before")
    @classmethod
    def parse_chat_ids(cls, value: Any) -> frozenset[int]:
        try:
            result = frozenset(int(item) for item in _csv(value))
        except ValueError as exc:
            raise ValueError("chat IDs must be comma-separated integers") from exc
        if not result:
            raise ValueError("at least one chat ID is required")
        return result

    @field_validator("targets", mode="before")
    @classmethod
    def parse_targets(cls, value: Any) -> frozenset[str]:
        result: set[str] = set()
        for item in _csv(value):
            if item.lstrip("+").isdigit():
                user_id = int(item)
                if user_id <= 0:
                    raise ValueError("target user IDs must be positive")
                result.add(str(user_id))
            else:
                result.add(f"@{normalize_username(item)}")
        if not result:
            raise ValueError("at least one target is required")
        return frozenset(result)

    @field_validator("video_domains", mode="before")
    @classmethod
    def parse_video_domains(cls, value: Any) -> frozenset[str]:
        return frozenset(normalize_domain(item) for item in _csv(value))

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, value: str) -> str:
        level = value.upper()
        if level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError("log level must be DEBUG, INFO, WARNING, ERROR, or CRITICAL")
        return level

    @field_validator("telegram_session_path")
    @classmethod
    def validate_session_path(cls, value: Path) -> Path:
        if not str(value).strip():
            raise ValueError("session path must not be empty")
        return value.expanduser().resolve()

    @model_validator(mode="after")
    def validate_cross_field_rules(self) -> Settings:
        if self.telegram_expected_user_id in self.target_user_ids:
            raise ValueError("the automation account must not be a target user")
        if self.group_size < self.threshold:
            raise ValueError(
                "BURST_GUARD_WINDOW_SIZE must be greater than or equal to BURST_GUARD_THRESHOLD"
            )
        return self

    @property
    def target_user_ids(self) -> frozenset[int]:
        return frozenset(int(item) for item in self.targets if not item.startswith("@"))

    @property
    def target_usernames(self) -> frozenset[str]:
        return frozenset(item[1:] for item in self.targets if item.startswith("@"))

    def target_key(self, user_id: int | None, username: str | None) -> str | None:
        if user_id is not None and user_id in self.target_user_ids:
            return f"id:{user_id}"
        if username:
            normalized = username.removeprefix("@").lower()
            if normalized in self.target_usernames:
                return f"id:{user_id}" if user_id is not None else f"username:{normalized}"
        return None


class LoginSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=True,
        populate_by_name=True,
    )

    telegram_api_id: int = Field(validation_alias="TELEGRAM_API_ID", gt=0)
    telegram_api_hash: SecretStr = Field(validation_alias="TELEGRAM_API_HASH", min_length=1)
    telegram_session_path: Path = Field(validation_alias="TELEGRAM_SESSION_PATH")

    @field_validator("telegram_session_path")
    @classmethod
    def expand_session_path(cls, value: Path) -> Path:
        return value.expanduser().resolve()
