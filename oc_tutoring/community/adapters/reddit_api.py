"""Reddit's official Data API. Off until Reddit has approved the application.

Requires REDDIT_API_APPROVED=1 as well as credentials: having keys is not the
same as having permission. No request is made otherwise, and nothing here tries
to reach reddit.com without the API.
"""
from __future__ import annotations

import base64
import json
from urllib.parse import urlencode

from ..httpx import request_json
from .base import Adapter


class RedditAPIAdapter(Adapter):
    name = "reddit_api"
    acquisition = "official_api"
    storage_allowed = False   # keep excerpts out of a public repository until the terms are confirmed

    def availability(self):
        if not (self.env.get("REDDIT_CLIENT_ID") and self.env.get("REDDIT_CLIENT_SECRET")):
            return False, "no_credentials (REDDIT_CLIENT_ID / REDDIT_CLIENT_SECRET)"
        if self.env.get("REDDIT_API_APPROVED") != "1":
            return False, "not_approved (set REDDIT_API_APPROVED=1 only after Reddit approves the app)"
        return True, "ok"

    def _token(self):
        basic = base64.b64encode(f"{self.env['REDDIT_CLIENT_ID']}:{self.env['REDDIT_CLIENT_SECRET']}".encode()).decode()
        data = request_json("https://www.reddit.com/api/v1/access_token",
                            data=urlencode({"grant_type": "client_credentials"}).encode(),
                            headers={"Authorization": f"Basic {basic}", "User-Agent": self.config["user_agent"]},
                            transport=self.transport, sleep=self.sleep, retries=1)
        return data["access_token"]

    def read(self, lead_, ctx):
        if lead_["platform"] != "reddit":
            return
        post_id = lead_["key"].split(":", 1)[1]
        data = request_json(f"https://oauth.reddit.com/comments/{post_id}?limit=200&raw_json=1",
                            headers={"Authorization": f"Bearer {self._token()}",
                                     "User-Agent": self.config["user_agent"]},
                            transport=self.transport, sleep=self.sleep, retries=1)
        post = data[0]["data"]["children"][0]["data"]
        yield {"post_id": post["id"], "published": _day(post.get("created_utc")), "completeness": "full",
               "text": f"{post.get('title', '')}\n{post.get('selftext', '')}"}
        for child in data[1]["data"]["children"]:
            c = child.get("data", {})
            if child.get("kind") == "t1" and c.get("body"):
                yield {"post_id": c["id"], "published": _day(c.get("created_utc")), "text": c["body"],
                       "completeness": "full"}


def _day(ts):
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ts, timezone.utc).date().isoformat() if ts else None
