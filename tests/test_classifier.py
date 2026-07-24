from __future__ import annotations

import pytest

from burst_guard.classifier import (
    candidate_url_keys,
    canonical_url_key,
    extract_bilibili_bvids,
    extract_urls,
    has_video_media,
    is_video_link_candidate,
)
from burst_guard.models import IncomingMessage


def message(**values: object) -> IncomingMessage:
    defaults: dict[str, object] = {"chat_id": -1, "message_id": 1, "sender_id": 2}
    defaults.update(values)
    return IncomingMessage(**defaults)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "candidate",
    [
        message(text="watch https://youtube.com/a"),
        message(caption="https://m.youtube.com/a"),
        message(entity_urls=("https://bilibili.com/video/BV1",)),
        message(entity_urls=("youtube.com/video/without-scheme",)),
        message(text="www.youtube.com/watch?v=1"),
    ],
)
def test_video_link_candidates(candidate: IncomingMessage) -> None:
    assert is_video_link_candidate(candidate, frozenset({"youtube.com", "bilibili.com"}))


@pytest.mark.parametrize(
    "ordinary",
    [
        message(document_mime_type="image/mp4"),
        message(has_video=True),
        message(has_video_note=True),
        message(document_mime_type="video/mp4"),
        message(text="https://notyoutube.com/a"),
        message(text="https://youtube.com.evil.test/a"),
        message(text="https://youtu.be/a"),
        message(text="plain text"),
    ],
)
def test_non_candidates(ordinary: IncomingMessage) -> None:
    assert not is_video_link_candidate(ordinary, frozenset({"youtube.com", "bilibili.com"}))


def test_unicode_hostname_is_compared_as_idna() -> None:
    candidate = message(text="https://视频.例子.测试/watch")
    domains = frozenset({"xn--fsqu00a.xn--0zwm56d"})

    assert is_video_link_candidate(candidate, domains)


def test_multiple_urls_still_form_one_candidate_message() -> None:
    candidate = message(
        text="https://youtube.com/a https://youtube.com/b",
        entity_urls=("https://youtube.com/c",),
    )

    assert len(extract_urls(candidate)) == 3
    assert is_video_link_candidate(candidate, frozenset({"youtube.com"}))


def test_url_trailing_punctuation_is_ignored() -> None:
    assert is_video_link_candidate(
        message(text="(https://youtube.com/watch?v=1)。"), frozenset({"youtube.com"})
    )


@pytest.mark.parametrize(
    "media",
    [
        message(has_video=True),
        message(has_video_note=True),
        message(document_mime_type="video/mp4"),
    ],
)
def test_video_media_is_detected_independently_from_link_candidates(
    media: IncomingMessage,
) -> None:
    assert has_video_media(media)
    assert not is_video_link_candidate(media, frozenset({"youtube.com"}))


@pytest.mark.parametrize(
    "animated_media",
    [
        message(has_video=True, document_mime_type="video/mp4", is_animated=True),
        message(document_mime_type="image/gif", is_animated=True),
        message(document_mime_type="video/webm", is_sticker=True),
    ],
)
def test_animations_and_stickers_are_not_video_media(animated_media: IncomingMessage) -> None:
    assert not has_video_media(animated_media)


def test_tiktok_domain_and_subdomains_are_candidates() -> None:
    domains = frozenset({"tiktok.com"})

    assert is_video_link_candidate(message(text="https://www.tiktok.com/@user/video/123"), domains)
    assert is_video_link_candidate(message(text="https://vm.tiktok.com/abc"), domains)


def test_original_url_key_ignores_presentation_only_differences() -> None:
    expected = "youtube.com/watch?v=1"

    assert canonical_url_key("https://YouTube.com:443/watch/?v=1#source") == expected
    assert canonical_url_key("http://youtube.com/watch?v=1") == expected
    assert canonical_url_key("youtube.com/watch?v=1") == expected


def test_candidate_url_keys_only_include_configured_video_domains() -> None:
    incoming = message(
        text="https://youtube.com/watch?v=1 https://example.com/watch?v=1",
        entity_urls=("https://m.youtube.com/watch?v=2",),
    )

    assert candidate_url_keys(incoming, frozenset({"youtube.com"})) == frozenset(
        {
            "youtube.com/watch?v=1",
            "youtube.com/watch?v=2",
            "platform:youtube:1",
            "platform:youtube:2",
        }
    )


def test_bilibili_source_url_matches_original_despite_share_parameters() -> None:
    domains = frozenset({"bilibili.com"})
    original = message(text="https://www.bilibili.com/video/BV123?spm_id_from=x&vd_source=y")
    source = message(entity_urls=("https://m.bilibili.com/video/BV123?p=1",))

    original_keys = candidate_url_keys(original, domains)
    source_keys = candidate_url_keys(source, domains)

    assert original_keys & source_keys == frozenset(
        {"bilibili.com/video/BV123", "platform:bilibili:bv123"}
    )


def test_bare_bilibili_bvid_is_a_candidate_and_matches_full_url() -> None:
    domains = frozenset({"bilibili.com"})
    bare = message(text="推荐 BV1vN7G6uE2Q 可以看看")
    full = message(text="https://www.bilibili.com/video/BV1vN7G6uE2Q")

    bare_keys = candidate_url_keys(bare, domains)
    full_keys = candidate_url_keys(full, domains)

    assert bare_keys == frozenset(
        {
            "bilibili.com/video/BV1vN7G6uE2Q",
            "platform:bilibili:bv1vn7g6ue2q",
        }
    )
    assert bare_keys <= full_keys
    assert is_video_link_candidate(bare, domains)


@pytest.mark.parametrize(
    "text",
    [
        "BV1vN7G6uE2",
        "BV1vN7G6uE2Q0",
        "xBV1vN7G6uE2Q",
        "BV1vN7G6uE2Qx",
        "bv1vN7G6uE2Q",
    ],
)
def test_invalid_or_embedded_bvid_is_not_detected(text: str) -> None:
    incoming = message(text=text)

    assert extract_bilibili_bvids(incoming) == ()
    assert not is_video_link_candidate(incoming, frozenset({"bilibili.com"}))


def test_bare_bvid_requires_bilibili_domain_to_be_enabled() -> None:
    incoming = message(caption="BV1vN7G6uE2Q")

    assert extract_bilibili_bvids(incoming) == ("BV1vN7G6uE2Q",)
    assert not is_video_link_candidate(incoming, frozenset({"youtube.com"}))
