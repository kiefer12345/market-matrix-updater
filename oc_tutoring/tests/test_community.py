"""Run with:  python -m unittest discover -s oc_tutoring/tests -v"""
import hashlib
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from community import pipeline, quality  # noqa: E402
from community.adapters.discuz import parse_listing, parse_posts  # noqa: E402
from community.httpx import FetchError, request  # noqa: E402
from community.model import parse_date, thread_key  # noqa: E402

LISTING = '''<a id="click_normalthread_111" href="x" class="xst" >求推荐 selective 补习班</a>
<a href="forum.php?mod=redirect&tid=111&goto=lastpost#lastpost">2026-11-01 10:00</a>
<a id="click_normalthread_222" href="x" class="xst" >周末去哪玩</a>'''


def thread(posts):
    return "".join(
        f'<em id="authorposton{pid}">发表于 {day} 10:00</em><td class="t_f" id="postmessage_{pid}">{text}</td>'
        for pid, day, text in posts)


POSTS = [("9001", "2026-10-30", "我家孩子上过 Alpha One 两个学期，老师讲题很细，推荐。"),
         ("9002", "2026-10-31", "Pre-Uni 折扣码 R1234 私信我"),
         ("9003", "2026-11-01", "今天天气不错"),
         ("9004", "2026-11-01", "我家孩子上过 Alpha One 两个学期，老师讲题很细，推荐！")]


class FakeWeb:
    """A transport that serves canned pages and records every URL asked for."""

    def __init__(self, routes):
        self.routes, self.calls = routes, []

    def __call__(self, url, headers=None, data=None, timeout=30):
        self.calls.append(url)
        for needle, reply in self.routes:
            if needle in url:
                if isinstance(reply, Exception):
                    raise reply
                return reply
        return 404, "not found"


def config(**sources):
    base = {"oursteps": {"enabled": True, "adapter": "discuz", "base": "https://www.oursteps.com.au/bbs",
                         "boards": [101], "listing_pages": 1, "title_keywords": "补习|selective",
                         "no_delay": True, "retries": 1},
            "brave": {"enabled": True, "adapter": "brave", "max_queries": 1, "no_delay": True, "retries": 1},
            "openai_web_search": {"enabled": True, "adapter": "openai_search", "model": "m", "max_queries": 1,
                                  "no_delay": True, "retries": 0},
            "reddit_api": {"enabled": True, "adapter": "reddit_api", "user_agent": "t", "no_delay": True}}
    base.update(sources)
    return {"queries": ["alpha one review"], "sources": base, "excerpt_chars": 280}


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        for name in ("centres.json", "evidence.json"):
            shutil.copy(ROOT / "data" / name, self.tmp / name)
        self.web = FakeWeb([("forumdisplay", (200, LISTING)), ("viewthread&tid=111", (200, thread(POSTS)))])

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def run_pipeline(self, env=None, web=None, cfg=None, **kw):
        return pipeline.run(cfg or config(), env=env or {}, transport=web or self.web, data_root=self.tmp,
                            log=lambda *_: None, **kw)

    def rows(self, name):
        p = self.tmp / "community" / name
        return [json.loads(x) for x in p.read_text(encoding="utf-8").splitlines()] if p.exists() else []


class Parsing(unittest.TestCase):
    def test_listing_and_posts(self):
        self.assertEqual(parse_listing(LISTING)[0], ("111", "求推荐 selective 补习班", "2026-11-01"))
        posts = parse_posts(thread(POSTS))
        self.assertEqual([p["post_id"] for p in posts], ["9001", "9002", "9003", "9004"])

    def test_dates_are_never_invented(self):
        self.assertEqual(parse_date("2026-11-1"), ("2026-11-01", "day"))
        self.assertEqual(parse_date("2026"), ("2026", "year"))
        self.assertEqual(parse_date(None), (None, "unknown"))
        self.assertEqual(parse_date("last week"), (None, "unknown"))

    def test_thread_keys_collapse_variants(self):
        self.assertEqual(thread_key("https://www.reddit.com/r/sydney/comments/1du43j9/title/"),
                         thread_key("https://old.reddit.com/r/sydney/comments/1du43j9"))
        self.assertEqual(thread_key("https://oursteps.co/bbs/forum.php?mod=viewthread&tid=5&page=2"),
                         "oursteps:5")


class Pipeline(Base):
    def test_free_channels_run_without_any_api_key(self):
        rep = self.run_pipeline()
        s = rep["sources"]
        self.assertEqual(s["oursteps"]["status"], "ok")
        self.assertIn("no_credentials", s["brave"]["status"])
        self.assertIn("no_credentials", s["openai_web_search"]["status"])
        self.assertIn("no_credentials", s["reddit_api"]["status"])
        self.assertEqual(s["oursteps"]["leads_found"], 1)          # the off-topic thread is not a lead
        self.assertFalse(any("brave.com" in u or "openai.com" in u or "reddit.com" in u for u in self.web.calls))

    def test_candidates_carry_quality_fields_and_stay_out_of_evidence(self):
        before = (self.tmp / "evidence.json").read_bytes()
        self.run_pipeline()
        cands = {c["key"]: c for c in self.rows("candidates.jsonl")}
        good = cands["oursteps:111#9001"]
        self.assertEqual(good["centres"], ["alpha_one"])
        self.assertEqual((good["published"], good["published_precision"]), ("2026-10-30", "day"))
        self.assertEqual(good["acquisition"], "direct_fetch")
        self.assertTrue(good["first_hand"] and good["passes_gate"])
        self.assertFalse(good["eligible_for_scoring"])              # a person must approve first
        promo = cands["oursteps:111#9002"]
        self.assertTrue(promo["promo_suspect"])
        self.assertIn("possible_promotion", promo["gate_reasons"])
        self.assertNotIn("oursteps:111#9003", cands)                # names no centre
        self.assertEqual((self.tmp / "evidence.json").read_bytes(), before)

    def test_reply_that_only_inherits_the_centre_from_the_title_fails_the_gate(self):
        listing = LISTING.replace("求推荐 selective 补习班", "Alpha One 补习怎么样")
        web = FakeWeb([("forumdisplay", (200, listing)),
                       ("viewthread", (200, thread([("1", "2026-01-02", "我家孩子觉得还行，上了一个学期")])))])
        self.run_pipeline(web=web)
        c = self.rows("candidates.jsonl")[0]
        self.assertEqual(c["centres"], ["alpha_one"])
        self.assertIn("centre_only_in_thread_title", c["gate_reasons"])

    def test_same_story_twice_is_marked_duplicate(self):
        self.run_pipeline()
        dup = {c["key"]: c for c in self.rows("candidates.jsonl")}["oursteps:111#9004"]
        self.assertEqual(dup["duplicate_of"], "oursteps:111#9001")
        self.assertFalse(dup["passes_gate"])
        self.assertEqual(dup["scoring_units"], ["alpha_one@oursteps:111"])   # same unit as the first telling

    def test_rerun_adds_nothing(self):
        self.run_pipeline()
        first = (self.tmp / "community" / "candidates.jsonl").read_bytes()
        rep = self.run_pipeline()
        self.assertEqual(rep["sources"]["oursteps"]["candidates_new"], 0)
        self.assertEqual((self.tmp / "community" / "candidates.jsonl").read_bytes(), first)

    def test_dry_run_writes_nothing(self):
        rep = self.run_pipeline(dry_run=True)
        self.assertGreater(rep["totals"]["candidates_new"], 0)
        self.assertFalse((self.tmp / "community").exists())

    def test_403_on_one_source_does_not_stop_the_others(self):
        web = FakeWeb([("forumdisplay", (200, LISTING)), ("viewthread", (200, thread(POSTS))),
                       ("brave.com", (403, "forbidden"))])
        rep = self.run_pipeline(env={"BRAVE_API_KEY": "k", "COMMUNITY_ALLOW_PAID": "1"}, web=web)
        self.assertEqual(rep["sources"]["brave"]["status"], "error")
        self.assertEqual(rep["sources"]["brave"]["errors"][0]["kind"], "forbidden")
        self.assertEqual(rep["sources"]["oursteps"]["status"], "ok")
        self.assertGreater(rep["sources"]["oursteps"]["candidates_new"], 0)

    def test_429_is_retried_a_bounded_number_of_times(self):
        web = FakeWeb([("forumdisplay", (429, "slow down"))])
        rep = self.run_pipeline(web=web)
        self.assertEqual(rep["sources"]["oursteps"]["errors"][0]["kind"], "rate_limited")
        self.assertEqual(len(web.calls), 2)                         # one try + retries=1

    def test_timeout_is_reported_not_raised(self):
        rep = self.run_pipeline(web=FakeWeb([("forumdisplay", FetchError("timeout", "t"))]))
        self.assertEqual(rep["sources"]["oursteps"]["errors"][0]["kind"], "timeout")

    def test_no_results(self):
        rep = self.run_pipeline(web=FakeWeb([("forumdisplay", (200, "<html></html>"))]))
        self.assertEqual(rep["sources"]["oursteps"]["status"], "ok")
        self.assertEqual(rep["totals"]["candidates_new"], 0)

    def test_paid_source_needs_explicit_permission(self):
        rep = self.run_pipeline(env={"BRAVE_API_KEY": "k", "OPENAI_API_KEY": "k"})
        self.assertIn("paid_calls_not_authorised", rep["sources"]["brave"]["status"])
        self.assertIn("paid_calls_not_authorised", rep["sources"]["openai_web_search"]["status"])

    def test_search_leads_are_not_stored_and_never_become_candidates(self):
        brave = {"web": {"results": [{"url": "https://www.reddit.com/r/sydney/comments/1du43j9/x/",
                                      "title": "Selective tutoring?", "description": "Alpha One was great",
                                      "page_age": "2024-07-03T00:00:00"}]}}
        web = FakeWeb([("forumdisplay", (200, "")), ("brave.com", (200, json.dumps(brave)))])
        rep = self.run_pipeline(env={"BRAVE_API_KEY": "k", "COMMUNITY_ALLOW_PAID": "1"}, web=web)
        self.assertEqual(rep["sources"]["brave"]["leads_found"], 1)
        self.assertEqual(rep["totals"]["leads_no_reader"], 1)       # Reddit has no approved reader
        self.assertEqual(self.rows("leads.jsonl"), [])              # Brave results may not be kept
        self.assertEqual(self.rows("candidates.jsonl"), [])

    def test_openai_answer_is_a_lead_only(self):
        answer = {"usage": {"input_tokens": 10, "output_tokens": 20}, "output": [{"type": "message", "content": [
            {"type": "output_text", "text": "Parents liked Alpha One.", "annotations": [
                {"type": "url_citation", "start_index": 0, "end_index": 24,
                 "url": "https://www.reddit.com/r/sydney/comments/18jh0yi/", "title": "thread"}]}]}]}
        web = FakeWeb([("forumdisplay", (200, "")), ("openai.com", (200, json.dumps(answer)))])
        rep = self.run_pipeline(env={"OPENAI_API_KEY": "k", "COMMUNITY_ALLOW_PAID": "1"}, web=web)
        s = rep["sources"]["openai_web_search"]
        self.assertEqual((s["leads_found"], s["usage"]["requests"], s["usage"]["output_tokens"]), (1, 1, 20))
        self.assertEqual(self.rows("leads.jsonl")[0]["key"], "reddit:18jh0yi")
        self.assertEqual(self.rows("candidates.jsonl"), [])

    def test_reddit_api_needs_approval_as_well_as_keys(self):
        rep = self.run_pipeline(env={"REDDIT_CLIENT_ID": "a", "REDDIT_CLIENT_SECRET": "b"})
        self.assertIn("not_approved", rep["sources"]["reddit_api"]["status"])

    def test_user_import(self):
        inbox = self.tmp / "community" / "inbox"
        inbox.mkdir(parents=True)
        (inbox / "a.json").write_text(json.dumps({"platform": "xiaohongshu", "url": "", "title": "群聊",
            "posts": [{"id": "1", "published": "2026-10", "text": "我家在 CS Education 上了一年，进步很大"}]}),
            encoding="utf-8")
        cfg = config(user_import={"enabled": True, "adapter": "user_import", "inbox": str(inbox)})
        self.run_pipeline(cfg=cfg, web=FakeWeb([("forumdisplay", (200, ""))]))
        c = self.rows("candidates.jsonl")[0]
        self.assertEqual((c["acquisition"], c["platform"], c["published_precision"]),
                         ("user_import", "xiaohongshu", "month"))
        self.assertEqual(c["verifiability"], "user_supplied_private")
        self.assertIsNone(c["independent"])

    def test_gate_rejects_snippets(self):
        c = {"acquisition": "llm_search", "completeness": "snippet", "centres": ["x"], "first_hand": True}
        out = quality.assess(c)
        self.assertFalse(out["passes_gate"])
        self.assertEqual(out["gate_reasons"], ["not_read_directly", "incomplete_text"])


class HttpLayer(unittest.TestCase):
    def test_retry_then_success(self):
        replies = iter([(500, "x"), (200, "ok")])
        self.assertEqual(request("http://x", transport=lambda *a, **k: next(replies), sleep=lambda s: None), "ok")

    def test_403_is_not_retried(self):
        calls = []
        with self.assertRaises(FetchError) as cm:
            request("http://x", transport=lambda *a, **k: calls.append(1) or (403, ""), sleep=lambda s: None)
        self.assertEqual((cm.exception.kind, len(calls)), ("forbidden", 1))


class HistoryIsUntouched(unittest.TestCase):
    """The 378 records and both rankings must match the baseline saved before this work."""

    def test_evidence_and_rankings_match_baseline(self):
        sums = dict(reversed(line.split()) for line in (ROOT / "baseline" / "SHA256SUMS").read_text().splitlines())
        digest = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
        self.assertEqual(digest(ROOT / "data" / "evidence.json"), sums["evidence.json"])
        self.assertEqual(len(json.loads((ROOT / "data" / "evidence.json").read_text(encoding="utf-8"))), 378)
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp) / "oc"
            shutil.copytree(ROOT, work, ignore=shutil.ignore_patterns("__pycache__", "community", "tests"))
            for args, name in (([], "ranking.json"), (["--focus", "selective"], "ranking_selective.json")):
                subprocess.run([sys.executable, str(work / "oc_ranker.py"), *args], check=True, capture_output=True)
                self.assertEqual(digest(work / name), sums[name], name)

    def test_no_secrets_in_code(self):
        for path in list((ROOT / "community").rglob("*.py")) + [ROOT / "community_run.py"]:
            text = path.read_text(encoding="utf-8")
            self.assertNotRegex(text, r"sk-[A-Za-z0-9]{20,}|BSA[A-Za-z0-9_-]{20,}", path.name)


if __name__ == "__main__":
    unittest.main()
