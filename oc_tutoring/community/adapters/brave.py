"""Brave Search API: discovery only.

Brave's API terms forbid storing or building a database of search results and
forbid redistribution, so nothing this adapter returns is written to disk by
default. A lead is only useful if another adapter can then open the URL itself.
Endpoints and parameters follow the documentation at api-dashboard.search.brave.com.
"""
from __future__ import annotations

from urllib.parse import urlencode

from ..httpx import request_json
from ..model import lead
from .base import Adapter

WEB = "https://api.search.brave.com/res/v1/web/search"
LLM = "https://api.search.brave.com/res/v1/llm/context"


class BraveAdapter(Adapter):
    name = "brave"
    acquisition = "search_index"
    storage_allowed = False
    persist_leads = False
    paid = True

    def availability(self):
        if not self.env.get("BRAVE_API_KEY"):
            return False, "no_credentials (set BRAVE_API_KEY)"
        if self.env.get("COMMUNITY_ALLOW_PAID") != "1":
            return False, "paid_calls_not_authorised (set COMMUNITY_ALLOW_PAID=1)"
        return True, "ok"

    def _call(self, endpoint, params):
        self.calls = getattr(self, "calls", 0) + 1
        return request_json(f"{endpoint}?{urlencode(params)}", timeout=self.config.get("timeout", 20),
                            headers={"X-Subscription-Token": self.env["BRAVE_API_KEY"], "Accept": "application/json"},
                            retries=self.config.get("retries", 1), transport=self.transport, sleep=self.sleep)

    def discover(self, ctx):
        for query in ctx["queries"][: self.config.get("max_queries", 5)]:
            if self.config.get("mode", "web") == "llm_context":
                data = self._call(LLM, {"q": query, "maximum_number_of_urls": 10})
                ages = data.get("sources", {})
                for item in data.get("grounding", {}).get("generic", []):
                    age = (ages.get(item["url"], {}).get("age") or [None, None])
                    yield lead(item["url"], source=self.name, title=item.get("title", ""), query=query,
                               published=age[1] if len(age) > 1 else None,
                               snippet=" ".join(item.get("snippets", []))[:300], persist=self.persist_leads)
            else:
                data = self._call(WEB, {"q": query, "count": 10, "country": "AU", "extra_snippets": "true"})
                for item in data.get("web", {}).get("results", []):
                    yield lead(item["url"], source=self.name, title=item.get("title", ""), query=query,
                               published=item.get("page_age"), snippet=item.get("description", ""),
                               persist=self.persist_leads)
            self.sleep(self.config.get("delay", 1.0))
