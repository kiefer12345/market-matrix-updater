#!/usr/bin/env python3
"""往 data/evidence.json 追加证据，往 data/centres.json 追加机构。

从标准输入读，每行一条，字段用 | 分隔：

  机构：  C|id|英文名|中文名|地区(逗号分隔)|形式|备注
  证据：  E|机构id|来源类型|平台|年份|立场|相关度|核实程度|url|帖子标题|内容摘要|原话(可空)

年份可写 2024、2024?（估计值，会记 year_estimated）或留空（不明）。
立场可写 recommend/positive/mention/mixed/negative/warn，或 rating:4.8:103（评分:条数）。
重复的（同机构 + 同 url + 同摘要）会跳过。
"""
import json
import sys
from pathlib import Path

DATA = Path(__file__).resolve().parent / "data"


def main():
    cpath, epath = DATA / "centres.json", DATA / "evidence.json"
    centres = json.loads(cpath.read_text(encoding="utf-8")) if cpath.exists() else []
    evidence = json.loads(epath.read_text(encoding="utf-8")) if epath.exists() else []
    cids = {c["id"] for c in centres}
    seen = {(e["centre"], e["url"], e["summary"]) for e in evidence}
    next_id = max([int(e["id"][1:]) for e in evidence], default=0) + 1
    added_c = added_e = 0
    for n, raw in enumerate(sys.stdin, 1):
        line = raw.rstrip("\n")
        if not line.strip() or line.startswith("#"):
            continue
        f = [x.strip() for x in line.split("|")]
        if f[0] == "C":
            f += [""] * (7 - len(f))
            if f[1] in cids:
                continue
            centres.append({"id": f[1], "name": f[2], "name_zh": f[3],
                            "areas": [a for a in f[4].split(",") if a], "format": f[5], "notes": f[6]})
            cids.add(f[1])
            added_c += 1
        elif f[0] == "E":
            f += [""] * (12 - len(f))
            _, centre, stype, platform, year, stance, rel, verify, url, title, summary, quote = f[:12]
            if centre not in cids:
                sys.exit(f"第 {n} 行：未知机构 {centre}")
            if (centre, url, summary) in seen:
                continue
            e = {"id": f"e{next_id:04d}", "centre": centre, "source_type": stype, "platform": platform}
            if year:
                e["year"] = int(year.rstrip("?"))
                if year.endswith("?"):
                    e["year_estimated"] = True
            else:
                e["year"] = None
            if stance.startswith("rating:"):
                _, rating, count = stance.split(":")
                e.update(stance="rating", rating=float(rating), rating_count=int(count))
            else:
                e["stance"] = stance
            e.update(relevance=rel or "oc", verification=verify or "search_summary",
                     url=url, title=title, summary=summary)
            if quote:
                e["quote"] = quote
            evidence.append(e)
            seen.add((centre, url, summary))
            next_id += 1
            added_e += 1
        else:
            sys.exit(f"第 {n} 行：开头必须是 C 或 E")
    cpath.write_text(json.dumps(centres, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    epath.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"新增机构 {added_c}，新增证据 {added_e}；现有机构 {len(centres)}，证据 {len(evidence)}")


if __name__ == "__main__":
    main()
