"""The gate between "something we found" and "evidence a reviewer may promote".

A candidate is never scored automatically. `passes_gate` only says whether it
is worth a reviewer's time and could be promoted without breaking the rules:
we read the text ourselves (or the user gave it to us), it is a whole post,
it names a centre, it reports a first-hand experience and does not look like
an advert.
"""
from __future__ import annotations

READ_BY_US = {"direct_fetch", "official_api", "user_import"}


def verifiability(candidate) -> str:
    if candidate["acquisition"] in ("direct_fetch", "official_api") and candidate.get("url", "").startswith("http"):
        return "public_url_rechecked_by_us"
    if candidate["acquisition"] == "user_import":
        return "user_supplied_public_url" if candidate.get("url", "").startswith("http") else "user_supplied_private"
    return "third_party_index_only"


def assess(candidate) -> dict:
    reasons = []
    if candidate["acquisition"] not in READ_BY_US:
        reasons.append("not_read_directly")
    if candidate["completeness"] != "full":
        reasons.append("incomplete_text")
    if not candidate.get("centres"):
        reasons.append("no_centre_identified")
    elif not candidate.get("centre_named_in_post", True):
        reasons.append("centre_only_in_thread_title")
    if not candidate.get("first_hand"):
        reasons.append("no_first_hand_experience")
    if candidate.get("promo_suspect"):
        reasons.append("possible_promotion")
    if candidate.get("duplicate_of"):
        reasons.append("duplicate")
    if candidate.get("matches_evidence"):
        reasons.append("already_in_evidence")
    return {
        "verifiability": verifiability(candidate),
        "passes_gate": not reasons,
        "gate_reasons": reasons,
        # Scoring always needs a human decision on top of the gate.
        "eligible_for_scoring": False,
        "review_status": candidate.get("review_status", "pending"),
    }
