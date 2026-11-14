"""Which centre a post is about, which exam, and rough signals about the post.

These are keyword heuristics. They decide what a reviewer sees first; they never
decide a centre's score.
"""
from __future__ import annotations

import re

EXAMS = {
    "oc": r"\bOC\b|opportunity class|OC班|天才班",
    "selective": r"selective|精英中学|精英考试|\bJR\b|james ruse|\bSTTC\b|\bASAT\b",
    "scholarship": r"scholarship|奖学金|私奖",
    "edutest": r"edutest|\bHAST\b|\bACER\b|插班|year ?[89] entry",
}
FIRST_HAND = (r"我家|我们家|我娃|我儿子|我女儿|孩子上过|上了.{0,6}(学期|term|年)|去过|我们在|亲身|我自己|"
              r"\bmy (son|daughter|kid|child|kids)\b|\bwe (went|used|tried|enrolled)\b|\bi went\b|my experience")
PROMO = (r"折扣码|推荐码|邀请码|优惠码|promo ?code|referral|\bR\d{4}\b|私信我|加微信|加我|联系我|vx|"
         r"欢迎咨询|报名热线|限时|免费试听名额|dm me|contact us|enrol now")
POSITIVE = r"推荐|不错|很好|挺好|喜欢|进步|有帮助|有用|靠谱|值得|考上|负责|准|recommend|great|helped|improved|worth"
NEGATIVE = r"不推荐|避雷|不好|没用|帮助不大|太难|题海|坑|后悔|退费|失望|垃圾|一般|avoid|waste|rip.?off|disappointed|poor"


def compile_centres(centres):
    out = {}
    for c in centres:
        names = [c["name"]] + c.get("aliases", [])
        names += [n.strip() for n in re.split(r"[/、]", c.get("name_zh", "")) if n.strip()]
        parts = []
        for n in {x for x in names if len(x) >= 2}:
            esc = re.escape(n)
            parts.append(rf"(?<![A-Za-z0-9]){esc}(?![A-Za-z0-9])" if re.search(r"[A-Za-z]", n) else esc)
        if parts:
            out[c["id"]] = re.compile("|".join(parts), re.I)
    return out


def centres_in(text, patterns):
    return sorted(cid for cid, pat in patterns.items() if pat.search(text))


def exams_in(text):
    return sorted(k for k, pat in EXAMS.items() if re.search(pat, text, re.I))


def stance_guess(text):
    pos = len(re.findall(POSITIVE, text, re.I)) - len(re.findall(r"不推荐|not recommend", text, re.I))
    neg = len(re.findall(NEGATIVE, text, re.I))
    if pos > 0 and neg > 0:
        return "mixed"
    return "negative" if neg > 0 else "positive" if pos > 0 else "mention"


def signals(text):
    return {
        "first_hand": bool(re.search(FIRST_HAND, text, re.I)),
        "promo_suspect": bool(re.search(PROMO, text, re.I)),
        "stance_guess": stance_guess(text),
        "exams": exams_in(text),
    }
