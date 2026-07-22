from __future__ import annotations

import pytest

from burst_guard.classifier import extract_urls, is_video_candidate
from burst_guard.models import IncomingMessage


def message(**values: object) -> IncomingMessage:
    defaults: dict[str, object] = {"chat_id": -1, "message_id": 1, "sender_id": 2}
    defaults.update(values)
    return IncomingMessage(**defaults)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "candidate",
    [
        message(has_video=True),
        message(has_video_note=True),
        message(document_mime_type="video/mp4"),
        message(text="watch https://youtube.com/a"),
        message(caption="https://m.youtube.com/a"),
        message(entity_urls=("https://bilibili.com/video/BV1",)),
        message(entity_urls=("youtube.com/video/without-scheme",)),
        message(text="www.youtube.com/watch?v=1"),
    ],
)
def test_video_candidates(candidate: IncomingMessage) -> None:
    assert is_video_candidate(candidate, frozenset({"youtube.com", "bilibili.com"}))


@pytest.mark.parametrize(
    "ordinary",
    [
        message(document_mime_type="image/mp4"),
        message(text="https://notyoutube.com/a"),
        message(text="https://youtube.com.evil.test/a"),
        message(text="https://youtu.be/a"),
        message(text="plain text"),
    ],
)
def test_non_candidates(ordinary: IncomingMessage) -> None:
    assert not is_video_candidate(ordinary, frozenset({"youtube.com", "bilibili.com"}))


def test_unicode_hostname_is_compared_as_idna() -> None:
    candidate = message(text="https://视频.例子.测试/watch")
    domains = frozenset({"xn--fsqu00a.xn--0zwm56d"})

    assert is_video_candidate(candidate, domains)


def test_multiple_urls_still_form_one_candidate_message() -> None:
    candidate = message(
        text="https://youtube.com/a https://youtube.com/b",
        entity_urls=("https://youtube.com/c",),
    )

    assert len(extract_urls(candidate)) == 3
    assert is_video_candidate(candidate, frozenset({"youtube.com"}))


def test_url_trailing_punctuation_is_ignored() -> None:
    assert is_video_candidate(
        message(text="(https://youtube.com/watch?v=1)。"), frozenset({"youtube.com"})
    )
