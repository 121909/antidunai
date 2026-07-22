from pathlib import Path

import pytest
from pydantic import ValidationError

from core.config import BotSettings


def make_settings(tmp_path: Path, **overrides: object) -> BotSettings:
    values: dict[str, object] = {
        "bot_token": "1:test-token",
        "api_id": "1",
        "api_hash": "test-hash",
        "data_path": tmp_path,
        "database_url": f"sqlite+aiosqlite:///{tmp_path / 'database.db'}",
    }
    values.update(overrides)
    return BotSettings(**values)  # type: ignore[arg-type]


def test_burst_guard_defaults_to_disabled(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)

    assert settings.burst_guard_enabled is False
    assert settings.burst_guard_threshold == 3
    assert settings.burst_guard_targets == frozenset()
    assert settings.burst_guard_video_platforms == frozenset()


def test_burst_guard_csv_values_are_normalized(tmp_path: Path) -> None:
    settings = make_settings(
        tmp_path,
        burst_guard_enabled=True,
        burst_guard_targets="123456, @Some_User, some_user",
        burst_guard_video_platforms="DouYin, bilibili, YOUTUBE",
    )

    assert settings.burst_guard_targets == frozenset({"123456", "some_user"})
    assert settings.burst_guard_video_platforms == frozenset({"douyin", "bilibili", "youtube"})
    assert settings.is_burst_guard_target(123456, None) is True
    assert settings.is_burst_guard_target(999, "@SOME_USER") is True
    assert settings.is_burst_guard_target(999, None) is False


def test_burst_guard_csv_values_load_from_environment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("BURST_GUARD_TARGETS", "123456,@Some_User")
    monkeypatch.setenv("BURST_GUARD_VIDEO_PLATFORMS", "douyin,bilibili,youtube")

    settings = make_settings(tmp_path)

    assert settings.burst_guard_targets == frozenset({"123456", "some_user"})
    assert settings.burst_guard_video_platforms == frozenset({"douyin", "bilibili", "youtube"})


def test_burst_guard_target_id_takes_precedence_over_missing_username(tmp_path: Path) -> None:
    settings = make_settings(tmp_path, burst_guard_targets=["123456", "named_user"])

    assert settings.is_burst_guard_target(123456, None) is True


def test_burst_guard_threshold_cannot_be_below_three(tmp_path: Path) -> None:
    with pytest.raises(ValidationError):
        make_settings(tmp_path, burst_guard_threshold=2)
