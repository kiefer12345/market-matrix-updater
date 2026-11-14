"""Discuz! forums we can read directly (oursteps.com.au)."""
from __future__ import annotations

import html
import re

from ..httpx import request
from ..model import lead
from .base import Adapter

ROW_RE = re.compile(r'id="click_normalthread_(\d+)"[^>]*class="xst"\s*>([^<]+)<')
LASTPOST_RE = re.compile(r'tid=(\d+)&(?:amp;)?goto=lastpost#lastpost">(?:<span title=")?(\d{4}-\d{1,2}-\d{1,2})')
POST_RE = re.compile(
    r'id="authorposton(\d+)">\s*发表于\s*(?:<span title=")?(\d{4}-\d{1,2}-\d{1,2}).*?'
    r'id="postmessage_\1"[^>]*>(.*?)</td>', re.S)
PAGES_RE = re.compile(r'title="共 (\d+) 页"')
QUOTE_RE = re.compile(r'<div class="quote">.*?</div>|<blockquote.*?</blockquote>', re.S)
STRIP_RE = re.compile(r"<(script|style)[^>]*>.*?</\1>|<[^>]+>", re.S)


def clean(fragment: str) -> str:
    text = html.unescape(STRIP_RE.sub(" ", QUOTE_RE.sub(" ", fragment)))
    text = re.sub(r"^\s*本帖最后由 .*? 编辑\s*", "", re.sub(r"\s+", " ", text))
    return text.strip()


def parse_listing(page: str):
    last = dict(LASTPOST_RE.findall(page))
    return [(tid, html.unescape(title).strip(), last.get(tid)) for tid, title in ROW_RE.findall(page)]


def parse_posts(page: str):
    return [{"post_id": pid, "published": day, "text": clean(body), "completeness": "full"}
            for pid, day, body in POST_RE.findall(page) if clean(body)]


class DiscuzAdapter(Adapter):
    name = "oursteps"

    def _get(self, url):
        return request(url, timeout=self.config.get("timeout", 30), retries=self.config.get("retries", 2),
                       transport=self.transport, sleep=self.sleep)

    def thread_url(self, tid):
        return f"{self.config['base']}/forum.php?mod=viewthread&tid={tid}"

    def discover(self, ctx):
        keep = re.compile(self.config["title_keywords"], re.I)
        skip = re.compile(self.config.get("title_skip", r"$^"), re.I)
        for fid in self.config["boards"]:
            for page in range(1, self.config.get("listing_pages", 2) + 1):
                body = self._get(f"{self.config['base']}/forum.php?mod=forumdisplay&fid={fid}&page={page}")
                for tid, title, last in parse_listing(body):
                    if keep.search(title) and not skip.search(title):
                        yield lead(self.thread_url(tid), source=self.name, title=title, note=f"lastpost={last or ''}")
                self.sleep(self.config.get("delay", 1.0))

    def read(self, lead_, ctx):
        url = lead_["url"]
        first = self._get(url)
        pages = [first]
        m = PAGES_RE.search(first)
        for n in range(2, min(int(m.group(1)) if m else 1, self.config.get("max_pages", 8)) + 1):
            self.sleep(self.config.get("delay", 1.0))
            pages.append(self._get(f"{url}&page={n}"))
        seen = set()
        for page in pages:
            for post in parse_posts(page):
                if post["post_id"] not in seen:
                    seen.add(post["post_id"])
                    yield post
