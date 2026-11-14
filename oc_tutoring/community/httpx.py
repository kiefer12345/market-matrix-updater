"""HTTP with a timeout, a small bounded retry and classified errors.

Every adapter goes through `request` so tests can swap the transport and so a
block, a rate limit and a timeout are told apart in the run report.
"""
from __future__ import annotations

import json
import socket
import time
import urllib.error
import urllib.request

UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
RETRYABLE = {"rate_limited", "timeout", "server_error", "network"}


class FetchError(Exception):
    def __init__(self, kind: str, detail: str = "", status: int | None = None):
        super().__init__(f"{kind}: {detail}" if detail else kind)
        self.kind, self.detail, self.status = kind, detail, status


def classify(status: int) -> str:
    if status in (401, 403):
        return "forbidden"
    if status == 404:
        return "not_found"
    if status == 429:
        return "rate_limited"
    if status >= 500:
        return "server_error"
    return "http_error"


def urllib_transport(url, headers=None, data=None, timeout=30):
    """Return (status, body_text). Raises FetchError for transport failures."""
    req = urllib.request.Request(url, data=data, headers={"User-Agent": UA, **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            charset = resp.headers.get_content_charset() or "utf-8"
            return resp.status, raw.decode(charset, errors="replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", errors="replace")
    except (socket.timeout, TimeoutError) as exc:
        raise FetchError("timeout", str(exc))
    except urllib.error.URLError as exc:
        kind = "timeout" if "timed out" in str(exc.reason) else "network"
        raise FetchError(kind, str(exc.reason))


def request(url, *, headers=None, data=None, timeout=30, retries=2, backoff=2.0,
            transport=None, sleep=time.sleep):
    """GET/POST with at most `retries` extra attempts on transient failures only."""
    transport = transport or urllib_transport
    last = None
    for attempt in range(retries + 1):
        try:
            status, body = transport(url, headers=headers, data=data, timeout=timeout)
            if 200 <= status < 300:
                return body
            last = FetchError(classify(status), body[:200], status)
        except FetchError as exc:
            last = exc
        if last.kind not in RETRYABLE or attempt == retries:
            raise last
        sleep(backoff * (attempt + 1))
    raise last  # pragma: no cover


def request_json(url, **kw):
    body = request(url, **kw)
    try:
        return json.loads(body)
    except ValueError:
        raise FetchError("bad_response", body[:200])
