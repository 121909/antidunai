from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from burst_guard.config import Settings


@pytest.fixture
def settings_factory(tmp_path: Path) -> Callable[..., Settings]:
    def make(**overrides: Any) -> Settings:
        values: dict[str, Any] = {
            "telegram_api_id": 12345,
            "telegram_api_hash": "test-api-hash",
            "telegram_expected_user_id": 9001,
            "telegram_session_path": tmp_path / "guard.session",
            "enabled": True,
            "dry_run": True,
            "chat_ids": "-1001,-1002",
            "targets": "101,@Video_User",
            "video_domains": "youtube.com,bilibili.com",
        }
        values.update(overrides)
        return Settings(**values)

    return make
