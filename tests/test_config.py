from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from burst_guard.config import Settings, normalize_domain


def test_csv_values_are_normalized(settings_factory: Callable[..., Settings]) -> None:
    settings = settings_factory(
        chat_ids=" -1002, -1001,-1001 ",
        targets="101, @Video_User,video_user",
        video_domains="YouTube.COM.,例子.测试,YouTube.com:443",
        parser_sender_ids="7947627028, 7947627028",
    )

    assert settings.chat_ids == frozenset({-1001, -1002})
    assert settings.target_user_ids == frozenset({101})
    assert settings.target_usernames == frozenset({"video_user"})
    assert settings.video_domains == frozenset({"youtube.com", "xn--fsqu00a.xn--0zwm56d"})
    assert settings.parser_sender_ids == frozenset({7947627028})
    assert settings.group_size == 10
    assert settings.threshold == 3
    assert settings.enabled is True
    assert settings.dry_run is True
    assert settings.info_reply_enabled is True
    assert settings.info_reply_cooldown_seconds == 60


def test_target_matching_prefers_stable_user_id(
    settings_factory: Callable[..., Settings],
) -> None:
    settings = settings_factory()

    assert settings.target_key(101, None) == "id:101"
    assert settings.target_key(202, "VIDEO_USER") == "id:202"
    assert settings.target_key(None, "@video_user") == "username:video_user"
    assert settings.target_key(202, "someone_else") is None


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("telegram_api_id", 0),
        ("threshold", 0),
        ("group_size", 0),
        ("delete_batch_size", 0),
        ("delete_batch_size", 101),
        ("info_reply_cooldown_seconds", 9),
        ("info_reply_cooldown_seconds", 86401),
        ("chat_ids", ""),
        ("targets", ""),
        ("targets", "bad-user!"),
        ("video_domains", "https://youtube.com/watch"),
        ("parser_sender_ids", "0"),
        ("log_level", "verbose"),
    ],
)
def test_invalid_configuration_fails_clearly(
    settings_factory: Callable[..., Settings], field: str, value: object
) -> None:
    with pytest.raises(ValidationError):
        settings_factory(**{field: value})


def test_automation_account_cannot_be_target(
    settings_factory: Callable[..., Settings],
) -> None:
    with pytest.raises(ValidationError, match="automation account"):
        settings_factory(targets="9001")


def test_group_size_must_cover_threshold(
    settings_factory: Callable[..., Settings],
) -> None:
    with pytest.raises(ValidationError, match="WINDOW_SIZE"):
        settings_factory(group_size=2, threshold=3)


def test_threshold_is_not_limited_to_three(
    settings_factory: Callable[..., Settings],
) -> None:
    settings = settings_factory(group_size=2, threshold=1)

    assert settings.group_size == 2
    assert settings.threshold == 1


@pytest.mark.parametrize("window_setting", ["BURST_GUARD_WINDOW_SIZE", "BURST_GUARD_GROUP_SIZE"])
def test_new_and_legacy_window_environment_names_are_accepted(
    tmp_path: Path, window_setting: str
) -> None:
    values: dict[str, Any] = {
        "telegram_api_id": 1,
        "telegram_api_hash": "hash",
        "telegram_expected_user_id": 9001,
        "telegram_session_path": tmp_path / "guard.session",
        "chat_ids": "-1001",
        "targets": "101",
        "threshold": 2,
        window_setting: 5,
    }

    settings = Settings(**values)

    assert settings.group_size == 5


def test_domain_normalization_removes_port_and_trailing_dot() -> None:
    assert normalize_domain("WWW.Example.COM.:8443") == "www.example.com"
