#!/usr/bin/env python3
"""社区口碑采集：发现线索 → 读取 → 去重 → 质量门槛 → 候选库。

  python oc_tutoring/community_run.py run                 # 跑全部已启用来源
  python oc_tutoring/community_run.py run --source oursteps
  python oc_tutoring/community_run.py run --dry-run        # 不写任何文件
  python oc_tutoring/community_run.py status               # 各来源可用性和库存
  python oc_tutoring/community_run.py review --key K --set approved|rejected [--note ...]
  python oc_tutoring/community_run.py export-approved      # 输出 add_evidence.py 能读的行

候选库和正式证据库是分开的：这里的任何命令都不会改 data/evidence.json，
所以不会改变评分。把候选变成正式证据要人看过原文，再用 add_evidence.py 录入。
"""
import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from community import pipeline  # noqa: E402
from community.store import Store  # noqa: E402

DATA = Path(__file__).resolve().parent / "data"


def main(argv=None):
    p = argparse.ArgumentParser(description="社区口碑采集")
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--source", action="append", help="只跑这个来源，可重复")
    r.add_argument("--dry-run", action="store_true")
    sub.add_parser("status")
    v = sub.add_parser("review")
    v.add_argument("--key", required=True)
    v.add_argument("--set", required=True, choices=["approved", "rejected", "pending"])
    v.add_argument("--note", default="")
    sub.add_parser("export-approved")
    args = p.parse_args(argv)
    config = pipeline.load_config()

    if args.cmd == "run":
        report = pipeline.run(config, only=args.source, dry_run=args.dry_run)
        bad = [n for n, s in report["sources"].items() if s["status"] == "error"]
        ran = [n for n, s in report["sources"].items() if s["status"] == "ok"]
        return 1 if bad and not ran else 0

    store = Store(DATA / "community")
    if args.cmd == "status":
        for name, cfg in config["sources"].items():
            adapter = pipeline.ADAPTERS[cfg["adapter"]](cfg, os.environ)
            ok, why = adapter.availability() if cfg.get("enabled", True) else (False, "disabled in sources.json")
            print(f"{name:<18} {'可用' if ok else '不可用'}  {why}  付费={adapter.paid}  可存正文={adapter.storage_allowed}")
        c = store.candidates
        print(f"线索 {len(store.leads)}；候选 {len(c)}；过门槛 {sum(1 for x in c if x.get('passes_gate'))}；"
              f"待审 {sum(1 for x in c if x.get('review_status') == 'pending')}；"
              f"已批准 {sum(1 for x in c if x.get('review_status') == 'approved')}")
        return 0
    if args.cmd == "review":
        hit = [c for c in store.candidates if c["key"] == args.key]
        if not hit:
            sys.exit("没有这个候选")
        hit[0].update(review_status=args.set, review_note=args.note)
        store.save({"review": args.key, "set": args.set})
        return 0
    if args.cmd == "export-approved":
        for c in store.candidates:
            if c.get("review_status") == "approved":
                for centre in c["centres"]:
                    print("|".join(["E", centre, "parent_forum", c["platform"], (c.get("published") or "")[:4],
                                    "<立场>", "<相关度>", "original_read" if c["acquisition"] == "direct_fetch" else "secondhand",
                                    c["url"], c["title"].replace("|", "/"), "<摘要>", c.get("excerpt", "").replace("|", "/")[:120]]))
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
