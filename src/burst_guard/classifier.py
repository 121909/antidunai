from __future__ import annotations

import re
from urllib.parse import SplitResult, urlsplit

from burst_guard.models import IncomingMessage

_URL_RE = re.compile(r"(?:https?://|www\.)[^\s<>\[\]{}\"']+", re.IGNORECASE)
_TRAILING_PUNCTUATION = ".,;:!?)]}\uff0c\u3002\uff1b\uff1a\uff01\uff1f\uff09\u3011\u300b"


def _split_url(url: str) -> SplitResult | None:
    try:
        normalized_url = url.strip().rstrip(_TRAILING_PUNCTUATION)
        if "://" not in normalized_url and not normalized_url.startswith("//"):
            normalized_url = f"//{normalized_url}"
        parsed = urlsplit(normalized_url)
        if parsed.scheme and parsed.scheme.lower() not in {"http", "https"}:
            return None
        _ = parsed.port
    except ValueError:
        return None
    return parsed


def _hostname(url: str) -> str | None:
    parsed = _split_url(url)
    hostname = parsed.hostname if parsed else None
    if not hostname:
        return None
    try:
        return hostname.rstrip(".").encode("idna").decode("ascii").lower()
    except UnicodeError:
        return None


def _domain_matches(host: str, domain: str) -> bool:
    return host == domain or host.endswith(f".{domain}")


def extract_urls(message: IncomingMessage) -> tuple[str, ...]:
    found: list[str] = list(message.entity_urls)
    for value in (message.text, message.caption):
        if value:
            found.extend(match.group(0) for match in _URL_RE.finditer(value))
    return tuple(found)


def canonical_url_key(url: str) -> str | None:
    parsed = _split_url(url)
    if parsed is None or parsed.hostname is None:
        return None
    try:
        host = parsed.hostname.rstrip(".").encode("idna").decode("ascii").lower()
    except UnicodeError:
        return None
    port = parsed.port
    authority = host if port in {None, 80, 443} else f"{host}:{port}"
    path = parsed.path.rstrip("/") or "/"
    query = f"?{parsed.query}" if parsed.query else ""
    return f"{authority}{path}{query}"


def candidate_url_keys(message: IncomingMessage, domains: frozenset[str]) -> frozenset[str]:
    keys: set[str] = set()
    for url in extract_urls(message):
        host = _hostname(url)
        if not host or not any(_domain_matches(host, domain) for domain in domains):
            continue
        key = canonical_url_key(url)
        if key:
            keys.add(key)
    return frozenset(keys)


def has_video_media(message: IncomingMessage) -> bool:
    return bool(
        message.has_video
        or message.has_video_note
        or (message.document_mime_type and message.document_mime_type.lower().startswith("video/"))
    )


def is_video_candidate(message: IncomingMessage, domains: frozenset[str]) -> bool:
    return has_video_media(message) or bool(candidate_url_keys(message, domains))
