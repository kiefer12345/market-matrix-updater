"""Material the user hands over: exported chats, copied posts, text from screenshots.

Put a JSON file in data/community/inbox/ shaped like:
  {"platform": "xiaohongshu", "url": "https://...", "title": "...", "exported_at": "2026-11-14",
   "via": "manual copy", "posts": [{"id": "1", "published": "2026-10-02", "text": "..."}]}
`url` may be a group name when there is no public link. Nothing is collected
from private groups by this program; it only reads files placed here.
"""
from __future__ import annotations

import json
from pathlib import Path

from ..model import lead
from .base import Adapter


class UserImportAdapter(Adapter):
    name = "user_import"
    acquisition = "user_import"

    def _files(self):
        return sorted(Path(self.config["inbox"]).glob("*.json"))

    def discover(self, ctx):
        for path in self._files():
            doc = json.loads(path.read_text(encoding="utf-8"))
            url = doc.get("url") or f"import://{path.stem}"
            item = lead(url, source=self.name, title=doc.get("title", ""), note=f"via={doc.get('via', '')}")
            item["platform"] = doc.get("platform") or item["platform"]
            item["_file"] = str(path)
            yield item

    def read(self, lead_, ctx):
        doc = json.loads(Path(lead_["_file"]).read_text(encoding="utf-8"))
        for i, post in enumerate(doc.get("posts", [])):
            if (post.get("text") or "").strip():
                yield {"post_id": str(post.get("id", i)), "published": post.get("published"),
                       "text": post["text"], "completeness": post.get("completeness", "full")}
