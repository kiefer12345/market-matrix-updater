"""Shared record shapes and helpers: URL keys, dates, text fingerprints."""
from __future__ import annotations

import hashlib
import html
import re
from datetime import date, datetime, timezone
from urllib.parse import parse_qs, urlparse

ACQUISITION = ("direct_fetch", "official_api", "user_import", "llm_search", "search_index")
COMPLETENESS = ("full", "excerpt", "snippet", "title_only")


def today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def platform_of(url: str) -> str:
    host = urlparse(url).netloc.lower().removeprefix("www.").removeprefix("old.")
    for needle, name in (("oursteps", "oursteps"), ("reddit.com", "reddit"), ("xiaohongshu", "xiaohongshu"),
                         ("rednote", "xiaohongshu"), ("facebook.com", "facebook"), ("ozbargain", "ozbargain"),
                         ("boredofstudies", "boredofstudies"), ("whirlpool", "whirlpool")):
        if needle in host:
            return name
    return host or "unknown"


def thread_key(url: str) -> str:
    """A stable id for the thread a URL belongs to, so mirrors and page 2 collapse."""
    u = urlparse(url)
    plat = platform_of(url)
    if plat == "oursteps":
        tid = parse_qs(u.query).get("tid") or re.findall(r"tid-(\d+)", url)
        if tid:
            return f"oursteps:{tid[0]}"
    if plat == "reddit":
        m = re.search(r"/comments/([a-z0-9]+)", u.path)
        if m:
            return f"reddit:{m.group(1)}"
    if plat == "ozbargain":
        m = re.search(r"/node/(\d+)", u.path)
        if m:
            return f"ozbargain:{m.group(1)}"
    path = re.sub(r"/(page-\d+|\d+/?)$", "", u.path.rstrip("/"))
    return f"{plat}:{path}{'?' + u.query if u.query and plat not in ('reddit',) else ''}"


def normalise_text(text: str) -> str:
    text = html.unescape(text or "").lower()
    return re.sub(r"[\W_]+", "", text, flags=re.UNICODE)


def content_hash(text: str) -> str:
    return hashlib.sha1(normalise_text(text).encode("utf-8")).hexdigest()[:16]


def shingles(text: str, n: int = 5) -> set[str]:
    t = normalise_text(text)
    return {t[i:i + n] for i in range(max(1, len(t) - n + 1))}


def similarity(a: str, b: str) -> float:
    sa, sb = shingles(a), shingles(b)
    return len(sa & sb) / len(sa | sb) if sa and sb else 0.0


def parse_date(value) -> tuple[str | None, str]:
    """Return (ISO date or None, precision) where precision is day/month/year/unknown.

    Nothing is ever guessed: a bare year stays a year, a missing date stays unknown.
    """
    if not value:
        return None, "unknown"
    s = str(value).strip()
    m = re.search(r"(20\d{2}|19\d{2})[-/.年](\d{1,2})[-/.月](\d{1,2})", s)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3))).isoformat(), "day"
        except ValueError:
            return None, "unknown"
    m = re.search(r"(20\d{2})[-/.年](\d{1,2})(?!\d)", s)
    if m and 1 <= int(m.group(2)) <= 12:
        return f"{m.group(1)}-{int(m.group(2)):02d}", "month"
    m = re.fullmatch(r"(20\d{2})", s)
    if m:
        return m.group(1), "year"
    return None, "unknown"


def lead(url, *, source, title="", query="", published=None, snippet="", persist=True, note=""):
    pub, precision = parse_date(published)
    return {
        "key": thread_key(url), "url": url, "platform": platform_of(url), "source": source,
        "title": title[:200], "query": query, "published": pub, "published_precision": precision,
        "snippet": snippet[:300], "discovered_at": today(), "status": "new", "persist": persist, "note": note,
    }
