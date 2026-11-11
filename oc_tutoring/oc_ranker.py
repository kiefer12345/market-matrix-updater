#!/usr/bin/env python3
"""OC 补习班口碑排行。

读取 data/centres.json（机构）和 data/evidence.json（一条条有出处的口碑证据），
按 weights.json 里的权重（来源类型 × 年份衰减 × 核实程度 × OC 相关度 × 立场）
算出每家机构的口碑分，生成：

  report.md      给人看的报告：排行 + 每家机构的全部证据和各自权重
  ranking.json   给程序用的同一份结果

用法：
  python oc_tutoring/oc_ranker.py                 # 生成报告
  python oc_tutoring/oc_ranker.py --year 2027     # 换一个参考年份
  python oc_tutoring/oc_ranker.py --check         # 只校验数据，不写文件
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
DATA = HERE / "data"

STANCE_LABEL = {
    "recommend": "推荐",
    "positive": "正面",
    "mention": "被提及",
    "mixed": "褒贬参半",
    "negative": "负面",
    "warn": "劝退",
}
NOT_INDEPENDENT = {"self_claim", "listing_blog", "staff_review", "directory"}
# Which exam the ranking is for. Evidence about the other exam still counts, at 0.7.
FOCUS = {
    "oc": {"label": "OC", "relevance": {"oc": 1.0, "selective_primary": 0.7, "general": 0.4}, "suffix": ""},
    "selective": {"label": "Selective（精英中学）", "relevance": {"oc": 0.7, "selective_primary": 1.0, "general": 0.4},
                  "suffix": "_selective"},
}
VERIFY_LABEL = {
    "original_read": "已读原帖",
    "search_summary": "仅搜索摘要",
    "secondhand": "转述",
}


def load(path: Path):
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def validate(centres, evidence, weights):
    """Return a list of problems; empty means the data is usable."""
    problems = []
    ids = {c["id"] for c in centres}
    if len(ids) != len(centres):
        problems.append("centres.json 里有重复的机构 id")
    seen = set()
    for i, e in enumerate(evidence):
        tag = e.get("id", f"#{i}")
        if tag in seen:
            problems.append(f"{tag}: 证据 id 重复")
        seen.add(tag)
        for field in ("id", "centre", "source_type", "platform", "url", "stance", "summary"):
            if not e.get(field):
                problems.append(f"{tag}: 缺少字段 {field}")
        if e.get("centre") not in ids:
            problems.append(f"{tag}: 未知机构 {e.get('centre')}")
        if e.get("source_type") not in weights["source_types"]:
            problems.append(f"{tag}: 未知来源类型 {e.get('source_type')}")
        if "rating" not in e and e.get("stance") not in weights["stance_sentiment"]:
            problems.append(f"{tag}: 未知立场 {e.get('stance')}")
        if e.get("verification", "search_summary") not in weights["verification"]:
            problems.append(f"{tag}: 未知核实程度 {e.get('verification')}")
        if e.get("relevance", "oc") not in weights["relevance"]:
            problems.append(f"{tag}: 未知相关度 {e.get('relevance')}")
        year = e.get("year")
        if year is not None and not (2000 <= int(year) <= weights["reference_year"]):
            problems.append(f"{tag}: 年份 {year} 不合理")
    return problems


def recency_weight(year, weights):
    cfg = weights["recency"]
    if year is None:
        return cfg["unknown_year_weight"]
    age = max(0, weights["reference_year"] - int(year))
    return max(cfg["floor"], 0.5 ** (age / cfg["half_life_years"]))


def sentiment_of(e, weights):
    """Sentiment in [-1, 1] and a volume factor in (0, 1]."""
    if "rating" in e:
        cfg = weights["aggregate_rating"]
        s = (float(e["rating"]) - cfg["neutral_rating"]) / cfg["scale"]
        n = int(e.get("rating_count", 1))
        volume = min(1.0, math.log10(n + 1) / 2)
        return max(-1.0, min(1.0, s)), volume
    return weights["stance_sentiment"][e["stance"]], 1.0


def score(centres, evidence, weights):
    by_centre = defaultdict(list)
    for e in evidence:
        by_centre[e["centre"]].append(e)

    results = []
    for c in centres:
        items = by_centre.get(c["id"], [])
        rows = []
        for e in items:
            sentiment, volume = sentiment_of(e, weights)
            w_source = weights["source_types"][e["source_type"]]["weight"]
            w_year = recency_weight(e.get("year"), weights)
            w_verify = weights["verification"][e.get("verification", "search_summary")]
            w_rel = weights["relevance"][e.get("relevance", "oc")]
            rows.append(
                {
                    "evidence": e,
                    "sentiment": sentiment,
                    "w_source": w_source,
                    "w_year": w_year,
                    "w_verify": w_verify,
                    "w_rel": w_rel,
                    "base_weight": w_source * w_year * w_verify * w_rel * volume,
                }
            )

        # Several opinions from one thread count progressively less.
        damp = weights["same_thread_damping"]["factor"]
        per_thread = defaultdict(int)
        for r in sorted(rows, key=lambda r: -r["base_weight"]):
            n = per_thread[r["evidence"]["url"]]
            r["weight"] = r["base_weight"] * (damp**n)
            r["points"] = r["weight"] * r["sentiment"]
            per_thread[r["evidence"]["url"]] += 1

        pos = sum(r["points"] for r in rows if r["points"] > 0)
        neg = -sum(r["points"] for r in rows if r["points"] < 0)
        total_w = sum(r["weight"] for r in rows)
        # Only first-hand opinion counts towards confidence: adverts, competitor
        # listicles and staff reviews say nothing reliable about teaching quality.
        independent = [r for r in rows if r["evidence"]["source_type"] not in NOT_INDEPENDENT]
        threads = {r["evidence"]["url"] for r in independent}
        years = [r["evidence"]["year"] for r in independent if r["evidence"].get("year")]
        latest = max(years) if years else None

        tiers = weights["confidence_tiers"]
        if latest and len(threads) >= tiers["high"]["min_independent_sources"] and latest >= tiers["high"]["min_latest_year"]:
            confidence = "高"
        elif latest and len(threads) >= tiers["medium"]["min_independent_sources"] and latest >= tiers["medium"]["min_latest_year"]:
            confidence = "中"
        else:
            confidence = "低"

        results.append(
            {
                "id": c["id"],
                "name": c["name"],
                "name_zh": c.get("name_zh", ""),
                "areas": c.get("areas", []),
                "format": c.get("format", ""),
                "notes": c.get("notes", ""),
                "score": round(pos - neg, 3),
                "positive": round(pos, 3),
                "negative": round(neg, 3),
                # Share of weighted opinion that is favourable; None when nobody has an opinion.
                "approval": round(pos / (pos + neg), 2) if (pos + neg) > 0 else None,
                "evidence_weight": round(total_w, 3),
                "evidence_count": len(rows),
                "independent_sources": len(threads),
                "latest_year": latest,
                "confidence": confidence,
                "rows": sorted(rows, key=lambda r: -abs(r["points"])),
            }
        )
    results.sort(key=lambda r: (-r["score"], -r["independent_sources"], r["name"]))
    return results


def fmt_year(y):
    return str(y) if y else "年份不明"


def render_markdown(results, weights, evidence, focus="oc"):
    ref = weights["reference_year"]
    exam = FOCUS[focus]["label"]
    out = []
    out.append(f"# 澳洲（NSW）{exam} 补习班口碑排行\n")
    out.append(
        f"参考年份 {ref}，共 {len(results)} 家机构、{len(evidence)} 条证据。"
        "本文件由 `oc_ranker.py` 自动生成，请改 `data/` 和 `weights.json`，不要手改这里。\n"
    )
    out.append("## 怎么读这份排行\n")
    out.append(
        f"- **口碑分** = 每条证据的「立场分 × 来源权重 × 年份衰减 × 核实程度 × {exam} 相关度」之和，正负相抵。\n"
        "- **好评占比** = 加权后正面意见占全部表态的比例，用来区分「口碑一边倒」和「争议大」。\n"
        "- **可信度** 看独立来源数和最近年份：来源少或只有旧帖的，分数再高也只是参考。\n"
        "- 机构官网、软文一律归为「机构自述」，权重 0.05，基本不影响排名，只列出备查。\n"
        "- 标「仅搜索摘要」的证据是通过搜索引擎摘要采集的，没有打开原帖逐字核对，权重已打七折。\n"
    )

    out.append("## 总排行\n")
    out.append("| 排名 | 机构 | 口碑分 | 正面 | 负面 | 好评占比 | 独立来源 | 最近年份 | 可信度 | 地区 |")
    out.append("|---:|---|---:|---:|---:|---:|---:|---:|:---:|---|")
    rated = [r for r in results if r["independent_sources"] > 0]
    listed = [r for r in results if r["independent_sources"] == 0]
    for i, r in enumerate(rated, 1):
        name = r["name"] + (f"（{r['name_zh']}）" if r["name_zh"] else "")
        approval = f"{int(r['approval'] * 100)}%" if r["approval"] is not None else "—"
        out.append(
            f"| {i} | {name} | {r['score']:.2f} | {r['positive']:.2f} | {r['negative']:.2f} | {approval} "
            f"| {r['independent_sources']} | {r['latest_year'] or '—'} | {r['confidence']} | {'、'.join(r['areas'])} |"
        )
    out.append("")

    out.append("## 只在名录或官网出现、暂无独立口碑的机构\n")
    out.append(
        f"下面 {len(listed)} 家确实在开相关课程，但没有找到家长的第一手评价，所以不参与排名。"
        "没有口碑不等于不好，只是公开渠道没人谈。\n"
    )
    out.append("| 机构 | 地区 | 形式 | 已知信息 |")
    out.append("|---|---|---|---|")
    for r in sorted(listed, key=lambda r: r["name"].lower()):
        name = r["name"] + (f"（{r['name_zh']}）" if r["name_zh"] else "")
        info = "；".join(row["evidence"]["summary"] for row in r["rows"][:2]).replace("|", "/")
        if r["notes"]:
            info = r["notes"].replace("|", "/") + " " + info
        out.append(f"| {name} | {'、'.join(r['areas'])} | {r['format']} | {info} |")
    out.append("")

    out.append("## 权重表\n")
    out.append("| 来源类型 | 权重 |")
    out.append("|---|---:|")
    for key, v in weights["source_types"].items():
        if key.startswith("_"):
            continue
        out.append(f"| {v['label']} | {v['weight']:.2f} |")
    out.append("")
    hl = weights["recency"]["half_life_years"]
    out.append(f"年份衰减（半衰期 {hl:g} 年）：")
    out.append("")
    out.append("| 年份 | " + " | ".join(str(y) for y in range(ref, ref - 11, -1)) + " |")
    out.append("|---|" + "---:|" * 11)
    out.append("| 权重 | " + " | ".join(f"{recency_weight(y, weights):.2f}" for y in range(ref, ref - 11, -1)) + " |")
    out.append("")

    out.append("## 各机构证据明细\n")
    for i, r in enumerate(rated, 1):
        name = r["name"] + (f"（{r['name_zh']}）" if r["name_zh"] else "")
        out.append(f"### {i}. {name}\n")
        meta = [f"口碑分 **{r['score']:.2f}**", f"可信度 **{r['confidence']}**", f"独立来源 {r['independent_sources']} 个"]
        if r["areas"]:
            meta.append("地区：" + "、".join(r["areas"]))
        if r["format"]:
            meta.append("形式：" + r["format"])
        out.append(" · ".join(meta) + "\n")
        if r["notes"]:
            out.append(r["notes"] + "\n")
        if not r["rows"]:
            out.append("暂无证据。\n")
            continue
        out.append("| 得分 | 立场 | 年份 | 来源 | 权重拆解（来源×年份×核实×相关） | 内容 |")
        out.append("|---:|:---:|:---:|---|---|---|")
        for row in r["rows"]:
            e = row["evidence"]
            if "rating" in e:
                stance = f"评分 {e['rating']}/5（{e.get('rating_count', '?')} 条）"
            else:
                stance = STANCE_LABEL[e["stance"]]
            src_label = weights["source_types"][e["source_type"]]["label"].split("（")[0]
            title = (e.get("title") or e["platform"]).replace("|", "/")
            source = f"[{e['platform']}：{title}]({e['url']})<br>{src_label}，{VERIFY_LABEL[e.get('verification', 'search_summary')]}"
            breakdown = f"{row['w_source']:.2f}×{row['w_year']:.2f}×{row['w_verify']:.2f}×{row['w_rel']:.2f} = {row['weight']:.3f}"
            text = e["summary"].replace("|", "/").replace("\n", " ")
            if e.get("quote"):
                text += "<br>原话：「" + e["quote"].replace("|", "/").replace("\n", " ") + "」"
            out.append(f"| {row['points']:+.3f} | {stance} | {fmt_year(e.get('year'))} | {source} | {breakdown} | {text} |")
        out.append("")
    return "\n".join(out) + "\n"


def to_json(results, weights):
    slim = []
    for r in results:
        d = {k: v for k, v in r.items() if k != "rows"}
        d["evidence"] = [
            {
                "id": row["evidence"]["id"],
                "points": round(row["points"], 4),
                "weight": round(row["weight"], 4),
                "sentiment": round(row["sentiment"], 2),
                "year": row["evidence"].get("year"),
                "source_type": row["evidence"]["source_type"],
                "platform": row["evidence"]["platform"],
                "url": row["evidence"]["url"],
                "summary": row["evidence"]["summary"],
            }
            for row in r["rows"]
        ]
        slim.append(d)
    return {"reference_year": weights["reference_year"], "centres": slim}


def main(argv=None):
    p = argparse.ArgumentParser(description="OC 补习班口碑排行")
    p.add_argument("--year", type=int, help="参考年份（默认取 weights.json）")
    p.add_argument("--check", action="store_true", help="只校验数据")
    p.add_argument("--focus", choices=sorted(FOCUS), default="oc", help="按哪个考试排：oc 或 selective")
    args = p.parse_args(argv)

    weights = load(HERE / "weights.json")
    if args.year:
        weights["reference_year"] = args.year
    weights["relevance"].update(FOCUS[args.focus]["relevance"])
    suffix = FOCUS[args.focus]["suffix"]
    centres = load(DATA / "centres.json")
    evidence = load(DATA / "evidence.json")

    problems = validate(centres, evidence, weights)
    if problems:
        print("数据有问题：", file=sys.stderr)
        for line in problems:
            print("  - " + line, file=sys.stderr)
        return 1
    if args.check:
        print(f"OK：{len(centres)} 家机构，{len(evidence)} 条证据")
        return 0

    results = score(centres, evidence, weights)
    (HERE / f"report{suffix}.md").write_text(render_markdown(results, weights, evidence, args.focus), encoding="utf-8")
    (HERE / f"ranking{suffix}.json").write_text(
        json.dumps(to_json(results, weights), ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(f"已生成 report{suffix}.md / ranking{suffix}.json（{len(centres)} 家机构，{len(evidence)} 条证据）")
    for i, r in enumerate(results[:15], 1):
        print(f"{i:>2}. {r['name']:<34} {r['score']:>6.2f}  来源 {r['independent_sources']}  可信度 {r['confidence']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
