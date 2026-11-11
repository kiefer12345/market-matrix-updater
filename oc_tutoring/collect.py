#!/usr/bin/env python3
"""抓取论坛帖子，找出提到补习机构的段落，生成候选证据。

  python oc_tutoring/collect.py                  # 抓 data/sources.json 里的全部帖子
  python oc_tutoring/collect.py --url <链接>      # 只抓一个
  python oc_tutoring/collect.py --file page.html --url <链接>   # 解析已保存的网页

结果写到 data/candidates.json。候选不会自动进入排行：这里的褒贬只是关键词匹配，
需要人看过原文后再用 add_evidence.py 录入。
"""
from __future__ import annotations

import argparse
import html
import json
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

DATA = Path(__file__).resolve().parent / "data"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"

POSITIVE = [
    "推荐", "不错", "很好", "挺好", "喜欢", "进步", "有帮助", "有用", "靠谱", "值得", "考上", "准", "负责",
    "recommend", "great", "excellent", "good", "helped", "improved", "worth", "got in", "happy with",
]
NEGATIVE = [
    "不推荐", "避雷", "一般", "不好", "没用", "帮助不大", "太难", "太简单", "题海", "坑", "后悔", "退费", "失望",
    "not recommend", "avoid", "waste", "rip-off", "rip off", "disappointed", "too big", "poor", "bad",
]
YEAR_RE = re.compile(r"(20[0-2]\d)[-/.年]\s?\d{1,2}[-/.月]\s?\d{1,2}")
BLOCK_RE = re.compile(r"<(?:div|p|td|li|blockquote|article|br)[^>]*>", re.I)
TAG_RE = re.compile(r"<[^>]+>")
SCRIPT_RE = re.compile(r"<(script|style)[^>]*>.*?</\1>", re.I | re.S)


def fetch(url: str, timeout: int = 30) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
        charset = resp.headers.get_content_charset()
    for enc in filter(None, [charset, "utf-8", "gbk"]):
        try:
            return raw.decode(enc)
        except (UnicodeDecodeError, LookupError):
            continue
    return raw.decode("utf-8", errors="replace")


def to_blocks(page: str) -> list[str]:
    """Split a page into text blocks, roughly one per post or paragraph."""
    page = SCRIPT_RE.sub(" ", page)
    blocks = []
    for chunk in BLOCK_RE.split(page):
        text = re.sub(r"\s+", " ", html.unescape(TAG_RE.sub(" ", chunk))).strip()
        if len(text) >= 8:
            blocks.append(text)
    return blocks


DISCUZ_POST_RE = re.compile(
    r'id="authorposton(\d+)">\s*发表于\s*(?:<span title=")?(\d{4})-(\d{1,2})-(\d{1,2}).*?'
    r'id="postmessage_\1"[^>]*>(.*?)</td>', re.S)
XENFORO_POST_RE = re.compile(
    r'<article class="message .*?datetime="(\d{4})-.*?<div class="bbWrapper">(.*?)</div>\s*(?:<div class="js-selectToQuoteEnd"|</div>\s*</article>)',
    re.S)
QUOTE_RE = re.compile(r'<div class="quote">.*?</div>|<blockquote.*?</blockquote>', re.S)


def clean(fragment: str) -> str:
    fragment = QUOTE_RE.sub(" ", SCRIPT_RE.sub(" ", fragment))
    return re.sub(r"\s+", " ", html.unescape(TAG_RE.sub(" ", fragment))).strip()


def extract_posts(page: str):
    """Return [(year, text)] for forum software we recognise, else None."""
    posts = [(int(m.group(2)), clean(m.group(5))) for m in DISCUZ_POST_RE.finditer(page)]
    if not posts:
        posts = [(int(m.group(1)), clean(m.group(2))) for m in XENFORO_POST_RE.finditer(page)]
    return [(y, t) for y, t in posts if t] or None


def page_urls(url: str, first_page: str) -> list[str]:
    """Other pages of the same thread (Discuz and XenForo)."""
    m = re.search(r'title="共 (\d+) 页"', first_page)
    if m and "viewthread" in url:
        base = re.sub(r"[&?]page=\d+", "", url)
        return [f"{base}&page={n}" for n in range(2, int(m.group(1)) + 1)]
    pages = [int(n) for n in re.findall(r'/page-(\d+)"', first_page)]
    if pages and "/threads/" in url:
        base = url.split("/page-")[0].rstrip("/")
        return [f"{base}/page-{n}" for n in range(2, max(pages) + 1)]
    return []


def alias_patterns(centres):
    patterns = {}
    for c in centres:
        names = [c["name"]] + c.get("aliases", [])
        names += [n.strip() for n in re.split(r"[/、]", c.get("name_zh", "")) if n.strip()]
        parts = []
        for n in {x for x in names if len(x) >= 2}:
            esc = re.escape(n)
            # Latin names must match as whole words; CJK names can match anywhere.
            parts.append(rf"(?<![A-Za-z0-9]){esc}(?![A-Za-z0-9])" if re.search(r"[A-Za-z]", n) else esc)
        if parts:
            patterns[c["id"]] = re.compile("|".join(parts), re.I)
    return patterns


def guess_stance(text: str) -> str:
    low = text.lower()
    # Check negatives first so "不推荐" is not read as "推荐".
    neg = sum(low.count(w) for w in NEGATIVE)
    pos = sum(low.count(w) for w in POSITIVE) - low.count("不推荐") - low.count("not recommend")
    if pos > 0 and neg > 0:
        return "mixed"
    if neg > 0:
        return "negative"
    if pos > 0:
        return "positive"
    return "mention"


def scan(page: str, url: str, source: dict, patterns) -> list[dict]:
    out = []
    year = source.get("year")
    posts = extract_posts(page)
    if posts is None:
        # Unknown site: fall back to text blocks, taking the nearest date above each one.
        posts = []
        for block in to_blocks(page):
            m = YEAR_RE.search(block)
            if m:
                year = int(m.group(1))
            if len(block) <= 1500:  # Longer blocks are navigation or whole-page blobs.
                posts.append((year, block))
    for year, block in posts:
        for cid, pat in patterns.items():
            if pat.search(block):
                out.append(
                    {
                        "centre": cid,
                        "source_type": source.get("source_type", "parent_forum"),
                        "platform": source.get("platform", ""),
                        "year": year,
                        "stance_guess": guess_stance(block),
                        "relevance": source.get("relevance", "oc"),
                        "verification": "original_read",
                        "url": url,
                        "title": source.get("title", ""),
                        "text": block[:1200],
                    }
                )
    return out


def main(argv=None):
    p = argparse.ArgumentParser(description="抓取帖子并生成候选证据")
    p.add_argument("--url", help="只抓这一个链接")
    p.add_argument("--file", help="解析本地保存的网页（配合 --url 标明出处）")
    p.add_argument("--delay", type=float, default=2.0, help="两次请求之间的间隔秒数")
    args = p.parse_args(argv)

    centres = json.loads((DATA / "centres.json").read_text(encoding="utf-8"))
    patterns = alias_patterns(centres)
    spath = DATA / "sources.json"
    sources = json.loads(spath.read_text(encoding="utf-8")) if spath.exists() else []
    by_url = {s["url"]: s for s in sources}

    if args.file:
        if not args.url:
            p.error("--file 需要同时给 --url 标明出处")
        todo = [by_url.get(args.url, {"url": args.url})]
    elif args.url:
        todo = [by_url.get(args.url, {"url": args.url})]
    else:
        todo = sources

    candidates, failed = [], []
    for i, source in enumerate(todo):
        url = source["url"]
        try:
            page = Path(args.file).read_text(encoding="utf-8", errors="replace") if args.file else fetch(url)
        except (urllib.error.URLError, OSError, ValueError) as exc:
            failed.append((url, str(exc)))
            print(f"  失败 {url}: {exc}", file=sys.stderr)
            continue
        found = scan(page, url, source, patterns)
        for extra in ([] if args.file else page_urls(url, page)):
            time.sleep(args.delay)
            try:
                found += scan(fetch(extra), url, source, patterns)
            except (urllib.error.URLError, OSError, ValueError) as exc:
                print(f"  失败 {extra}: {exc}", file=sys.stderr)
        candidates.extend(found)
        print(f"  {len(found):>3} 条候选  {source.get('title') or url}")
        if not args.file and i + 1 < len(todo):
            time.sleep(args.delay)

    out = DATA / "candidates.json"
    out.write_text(json.dumps(candidates, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"共 {len(candidates)} 条候选，写入 {out}；{len(failed)} 个链接抓取失败")
    return 1 if failed and not candidates else 0


if __name__ == "__main__":
    raise SystemExit(main())
