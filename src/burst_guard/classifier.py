from __future__ import annotations

import re
from urllib.parse import SplitResult, parse_qsl, urlencode, urlsplit

from burst_guard.models import IncomingMessage

_URL_RE = re.compile(r"(?:https?://|www\.)[^\s<>\[\]{}\"']+", re.IGNORECASE)
_TRAILING_PUNCTUATION = ".,;:!?)]}\uff0c\u3002\uff1b\uff1a\uff01\uff1f\uff09\u3011\u300b"
_TRACKING_QUERY_KEYS = frozenset(
    {
        "bbid",
        "feature",
        "from",
        "from_source",
        "is_from_webapp",
        "platform",
        "refer",
        "sender_device",
        "share_app_id",
        "share_medium",
        "share_plat",
        "share_session_id",
        "share_source",
        "share_tag",
        "share_times",
        "si",
        "spm_id_from",
        "timestamp",
        "ts",
        "unique_k",
        "vd_source",
    }
)
_PLATFORM_QUERY_KEYS: dict[str, frozenset[str]] = {
    "bilibili.com": frozenset({"aid", "bvid", "cid", "ep_id", "p", "season_id"}),
    "douyin.com": frozenset(),
    "tiktok.com": frozenset(),
    "youtube.com": frozenset({"v"}),
    "youtu.be": frozenset(),
}


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


def _normalized_query(parsed: SplitResult, domain: str) -> str:
    query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
    allowed_keys = _PLATFORM_QUERY_KEYS.get(domain)
    if allowed_keys is not None:
        query_pairs = [(key, value) for key, value in query_pairs if key in allowed_keys]
    else:
        query_pairs = [
            (key, value)
            for key, value in query_pairs
            if key not in _TRACKING_QUERY_KEYS and not key.startswith("utm_")
        ]
    if domain == "bilibili.com":
        query_pairs = [
            (key, value) for key, value in query_pairs if not (key == "p" and value == "1")
        ]
    return urlencode(sorted(query_pairs))


def canonical_url_key(url: str, canonical_domain: str | None = None) -> str | None:
    parsed = _split_url(url)
    if parsed is None or parsed.hostname is None:
        return None
    try:
        host = parsed.hostname.rstrip(".").encode("idna").decode("ascii").lower()
    except UnicodeError:
        return None
    domain = canonical_domain or host
    port = parsed.port
    authority = domain if port in {None, 80, 443} else f"{domain}:{port}"
    path = parsed.path.rstrip("/") or "/"
    normalized_query = _normalized_query(parsed, domain)
    query = f"?{normalized_query}" if normalized_query else ""
    return f"{authority}{path}{query}"


def _platform_identity_key(url: str, domain: str) -> str | None:
    parsed = _split_url(url)
    if parsed is None:
        return None
    path_parts = [part for part in parsed.path.split("/") if part]
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if domain == "bilibili.com":
        for part in path_parts:
            if re.fullmatch(r"(?i)(?:BV[0-9A-Za-z]+|av\d+|ep\d+|ss\d+)", part):
                return f"platform:bilibili:{part.lower()}"
        for key in ("bvid", "aid", "ep_id", "season_id"):
            if query.get(key):
                return f"platform:bilibili:{key}:{query[key].lower()}"
    if domain == "youtube.com":
        video_id = query.get("v")
        if video_id:
            return f"platform:youtube:{video_id}"
        if len(path_parts) >= 2 and path_parts[0] in {"embed", "shorts", "live"}:
            return f"platform:youtube:{path_parts[1]}"
    if domain == "youtu.be" and path_parts:
        return f"platform:youtube:{path_parts[0]}"
    if domain in {"tiktok.com", "douyin.com"} and "video" in path_parts:
        video_index = path_parts.index("video")
        if len(path_parts) > video_index + 1:
            return f"platform:{domain}:{path_parts[video_index + 1]}"
    return None


def candidate_url_keys(message: IncomingMessage, domains: frozenset[str]) -> frozenset[str]:
    keys: set[str] = set()
    for url in extract_urls(message):
        host = _hostname(url)
        matching_domains = [domain for domain in domains if host and _domain_matches(host, domain)]
        if not matching_domains:
            continue
        domain = max(matching_domains, key=len)
        key = canonical_url_key(url, domain)
        if key:
            keys.add(key)
        identity_key = _platform_identity_key(url, domain)
        if identity_key:
            keys.add(identity_key)
    return frozenset(keys)


def has_video_media(message: IncomingMessage) -> bool:
    if message.is_animated or message.is_sticker:
        return False
    return bool(
        message.has_video
        or message.has_video_note
        or (message.document_mime_type and message.document_mime_type.lower().startswith("video/"))
    )


def is_video_link_candidate(message: IncomingMessage, domains: frozenset[str]) -> bool:
    return bool(candidate_url_keys(message, domains))
