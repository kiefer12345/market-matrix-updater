"""Run the sources, turn what they find into leads and candidates, and report.

Order of work for each enabled source:
  availability -> discover leads -> read the leads it can open -> analyse each
  post -> de-duplicate -> quality gate -> candidates store.
Nothing here writes to data/evidence.json, so scores cannot change.
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path

from . import analysis, quality
from .adapters.brave import BraveAdapter
from .adapters.discuz import DiscuzAdapter
from .adapters.openai_search import OpenAISearchAdapter
from .adapters.reddit_api import RedditAPIAdapter
from .adapters.user_import import UserImportAdapter
from .httpx import FetchError
from .model import normalise_text, parse_date, platform_of, thread_key, today
from .store import Store

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
ADAPTERS = {"discuz": DiscuzAdapter, "brave": BraveAdapter, "openai_search": OpenAISearchAdapter,
            "reddit_api": RedditAPIAdapter, "user_import": UserImportAdapter}


def load_config(path=None):
    return json.loads(Path(path or HERE / "sources.json").read_text(encoding="utf-8"))


def evidence_index(evidence):
    """Thread -> normalised quotes already in the evidence base."""
    idx = {}
    for e in evidence:
        idx.setdefault(thread_key(e["url"]), []).append((e["id"], normalise_text(e.get("quote", ""))))
    return idx


def build_candidate(adapter, lead, post, patterns, ev_index, excerpt_chars):
    text = post["text"]
    in_text = analysis.centres_in(text, patterns)
    centres = in_text or analysis.centres_in(lead.get("title", ""), patterns)
    pub, precision = parse_date(post.get("published"))
    cand = {
        "key": f"{lead['key']}#{post['post_id']}", "thread": lead["key"], "url": lead["url"],
        "platform": lead["platform"], "source": adapter.name, "title": lead.get("title", ""),
        "centres": centres, "centre_named_in_post": bool(in_text), "published": pub, "published_precision": precision,
        "discovered_at": lead.get("discovered_at", today()), "fetched_at": today(),
        "acquisition": adapter.acquisition, "completeness": post.get("completeness", "full"),
        "independent": None if adapter.acquisition == "user_import" else True,
        "storage_allowed": adapter.storage_allowed, "text_chars": len(text),
        "excerpt": text[:excerpt_chars] if adapter.storage_allowed else "",
        # One thread counts once per centre however many parents repeat the story.
        "scoring_units": [f"{c}@{lead['key']}" for c in centres],
        **analysis.signals(text),
    }
    body = normalise_text(text)
    hits = [eid for eid, q in ev_index.get(lead["key"], []) if q and (q in body or q[:18] in body)]
    if hits:
        cand["matches_evidence"] = hits
    return cand


def run(config=None, *, only=None, dry_run=False, env=None, transport=None, data_root=None, log=print):
    config = config or load_config()
    env = os.environ if env is None else env
    data_root = Path(data_root or ROOT / "data")
    centres = json.loads((data_root / "centres.json").read_text(encoding="utf-8"))
    evidence = json.loads((data_root / "evidence.json").read_text(encoding="utf-8"))
    patterns, ev_index = analysis.compile_centres(centres), evidence_index(evidence)
    store = Store(data_root / "community", dry_run=dry_run)
    report = {"started": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "dry_run": dry_run, "sources": {}}
    ctx = {"queries": config.get("queries", [])}
    session_leads = []   # includes leads that may not be written to disk

    adapters = {}
    for name, cfg in config["sources"].items():
        if only and name not in only:
            continue
        cfg = dict(cfg)
        if cfg.get("inbox") and not Path(cfg["inbox"]).is_absolute():
            cfg["inbox"] = str(data_root / "community" / "inbox") if cfg["inbox"].startswith("data/") else cfg["inbox"]
        adapters[name] = (ADAPTERS[cfg["adapter"]](cfg, env, transport=transport), cfg)

    for name, (adapter, cfg) in adapters.items():
        adapter.name = name
        r = report["sources"][name] = {"status": "ok", "leads_found": 0, "leads_new": 0, "threads_read": 0,
                                       "posts_seen": 0, "candidates_new": 0, "duplicates": 0,
                                       "near_duplicates": 0, "passes_gate": 0, "errors": []}
        if not cfg.get("enabled", True):
            r["status"] = "disabled"
            continue
        ok, why = adapter.availability()
        if not ok:
            r["status"] = "skipped: " + why
            continue
        try:
            for lead in adapter.discover(ctx):
                r["leads_found"] += 1
                lead["persist"] = lead.get("persist", True) and adapter.persist_leads
                if store.add_lead({k: v for k, v in lead.items() if not k.startswith("_")}):
                    r["leads_new"] += 1
                session_leads.append((name, lead))
        except FetchError as exc:
            r["status"] = "error"
            r["errors"].append({"stage": "discover", "kind": exc.kind, "status": exc.status})
        except Exception as exc:   # a broken adapter must not stop the others
            r["status"] = "error"
            r["errors"].append({"stage": "discover", "kind": "adapter_bug", "detail": repr(exc)[:200]})
        for extra in ("calls", "usage"):
            if hasattr(adapter, extra):
                r[extra] = getattr(adapter, extra)

    # Read every lead with whichever available adapter can open that platform.
    readers = {"oursteps": "oursteps", "reddit": "reddit_api"}
    budget = {}
    for found_by, lead in sorted(session_leads, key=lambda x: x[1].get("note", ""), reverse=True):
        reader = found_by if found_by == "user_import" else readers.get(lead["platform"])
        if reader not in adapters or report["sources"][reader]["status"] != "ok":
            continue
        adapter, cfg = adapters[reader]
        r = report["sources"][reader]
        marker = lead.get("note", "")
        if reader != "user_import" and store.state.get(lead["key"], {}).get("marker") == marker and marker:
            continue   # nothing new since the last run
        budget[reader] = budget.get(reader, 0) + 1
        if budget[reader] > cfg.get("max_threads_per_run", 10 ** 6):
            continue
        try:
            seen_ids = set(store.state.get(lead["key"], {}).get("posts", []))
            for post in adapter.read(lead, ctx):
                r["posts_seen"] += 1
                if post["post_id"] in seen_ids:
                    r["duplicates"] += 1
                    continue
                seen_ids.add(post["post_id"])
                cand = build_candidate(adapter, lead, post, patterns, ev_index, config.get("excerpt_chars", 280))
                if not cand["centres"]:
                    continue   # not about a known centre: remembered as seen, not stored
                outcome = store.add_candidate(cand, post["text"])
                if outcome == "duplicate":
                    r["duplicates"] += 1
                    continue
                if outcome == "near_duplicate":
                    r["near_duplicates"] += 1
                cand.update(quality.assess(cand))
                r["candidates_new"] += 1
                r["passes_gate"] += cand["passes_gate"]
            r["threads_read"] += 1
            store.state[lead["key"]] = {"marker": marker, "posts": sorted(seen_ids), "read_at": today()}
        except FetchError as exc:
            r["errors"].append({"stage": "read", "url": lead["url"], "kind": exc.kind, "status": exc.status})
        except Exception as exc:
            r["errors"].append({"stage": "read", "url": lead["url"], "kind": "adapter_bug", "detail": repr(exc)[:200]})

    def readable(found_by, lead):
        reader = found_by if found_by == "user_import" else readers.get(lead["platform"])
        return reader in adapters and report["sources"][reader]["status"] == "ok"

    unread = [lead for found_by, lead in session_leads if not readable(found_by, lead)]
    report["totals"] = {
        "leads_total": len(store.leads), "candidates_total": len(store.candidates),
        "leads_new": len(store.new_leads), "candidates_new": len(store.new_candidates),
        "candidates_passing_gate": sum(1 for c in store.candidates if c.get("passes_gate")),
        "candidates_pending_review": sum(1 for c in store.candidates if c.get("review_status") == "pending"),
        "leads_no_reader": len(unread), "evidence_records": len(evidence),
    }
    store.save(report)
    for name, r in report["sources"].items():
        log(f"{name:<18} {r['status']:<52} leads {r['leads_found']:>3} (new {r['leads_new']:>3})  "
            f"threads {r['threads_read']:>3}  posts {r['posts_seen']:>4}  new candidates {r['candidates_new']:>3}  "
            f"dup {r['duplicates']:>4}  errors {len(r['errors'])}")
    log(json.dumps(report["totals"], ensure_ascii=False))
    return report
