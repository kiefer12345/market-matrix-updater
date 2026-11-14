"""Append-only stores kept apart from data/evidence.json.

leads.jsonl       things found but not read: never scored.
candidates.jsonl  posts we read, waiting for review: never scored.
state.json        per-thread progress so a re-run adds nothing twice.
runs.jsonl        one line per run with per-source results.
"""
from __future__ import annotations

import json
from pathlib import Path

from .model import content_hash, similarity

NEAR_DUPLICATE = 0.85


class Store:
    def __init__(self, root: Path, dry_run: bool = False):
        self.root, self.dry_run = Path(root), dry_run
        self.leads = self._read("leads.jsonl")
        self.candidates = self._read("candidates.jsonl")
        sp = self.root / "state.json"
        self.state = json.loads(sp.read_text(encoding="utf-8")) if sp.exists() else {}
        self._lead_keys = {(l["key"]) for l in self.leads}
        self._cand_keys = {c["key"] for c in self.candidates}
        self._hashes = {c["content_hash"]: c["key"] for c in self.candidates}
        self.new_leads, self.new_candidates = [], []

    def _read(self, name):
        p = self.root / name
        if not p.exists():
            return []
        return [json.loads(line) for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]

    def add_lead(self, lead) -> bool:
        if lead["key"] in self._lead_keys:
            return False
        self._lead_keys.add(lead["key"])
        if lead.get("persist", True):
            self.leads.append(lead)
            self.new_leads.append(lead)
        return True

    def add_candidate(self, cand, text: str) -> str:
        """Returns 'new', 'duplicate' (same post seen before) or 'near_duplicate'."""
        if cand["key"] in self._cand_keys:
            return "duplicate"
        cand["content_hash"] = content_hash(text)
        outcome = "new"
        if cand["content_hash"] in self._hashes:
            cand["duplicate_of"], outcome = self._hashes[cand["content_hash"]], "near_duplicate"
        else:
            for other in self.candidates:
                if other["thread"] == cand["thread"] and len(text) > 40 and \
                        similarity(text, other.get("excerpt", "")) >= NEAR_DUPLICATE:
                    cand["duplicate_of"], outcome = other["key"], "near_duplicate"
                    break
        self._cand_keys.add(cand["key"])
        self._hashes.setdefault(cand["content_hash"], cand["key"])
        self.candidates.append(cand)
        self.new_candidates.append(cand)
        return outcome

    def save(self, run_record: dict):
        if self.dry_run:
            return
        self.root.mkdir(parents=True, exist_ok=True)
        for name, rows in (("leads.jsonl", self.leads), ("candidates.jsonl", self.candidates)):
            (self.root / name).write_text(
                "".join(json.dumps(r, ensure_ascii=False, sort_keys=True) + "\n" for r in rows), encoding="utf-8")
        (self.root / "state.json").write_text(
            json.dumps(self.state, ensure_ascii=False, indent=1, sort_keys=True) + "\n", encoding="utf-8")
        with (self.root / "runs.jsonl").open("a", encoding="utf-8") as f:
            f.write(json.dumps(run_record, ensure_ascii=False, sort_keys=True) + "\n")
