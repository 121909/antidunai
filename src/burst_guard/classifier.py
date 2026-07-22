from __future__ import annotations

import re
from urllib.parse import urlsplit

from burst_guard.models import IncomingMessage

_URL_RE = re.compile(r"(?:https?://|www\.)[^\s<>\[\]{}\"']+", re.IGNORECASE)
_TRAILING_PUNCTUATION = ".,;:!?)]}\uff0c\u3002\uff1b\uff1a\uff01\uff1f\uff09\u3011\u300b"


def _hostname(url: str) -> str | None:
    try:
        normalized_url = url.strip().rstrip(_TRAILING_PUNCTUATION)
        if "://" not in normalized_url and not normalized_url.startswith("//"):
            normalized_url = f"//{normalized_url}"
        hostname = urlsplit(normalized_url).hostname
    except ValueError:
        return None
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


def is_video_candidate(message: IncomingMessage, domains: frozenset[str]) -> bool:
    if message.has_video or message.has_video_note:
        return True
    if message.document_mime_type and message.document_mime_type.lower().startswith("video/"):
        return True
    for url in extract_urls(message):
        host = _hostname(url)
        if host and any(_domain_matches(host, domain) for domain in domains):
            return True
    return False
