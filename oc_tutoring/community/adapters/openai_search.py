"""OpenAI Responses API with the web_search tool.

What comes back is a model-written answer plus cited URLs. It is a third-party
summary, not the post: it is stored as a lead with completeness "snippet" and
can never pass the quality gate on its own.
"""
from __future__ import annotations

import json

from ..httpx import request_json
from ..model import lead
from .base import Adapter

ENDPOINT = "https://api.openai.com/v1/responses"


class OpenAISearchAdapter(Adapter):
    name = "openai_web_search"
    acquisition = "llm_search"
    paid = True

    def availability(self):
        if not self.env.get("OPENAI_API_KEY"):
            return False, "no_credentials (set OPENAI_API_KEY; a ChatGPT subscription is not an API key)"
        if self.env.get("COMMUNITY_ALLOW_PAID") != "1":
            return False, "paid_calls_not_authorised (set COMMUNITY_ALLOW_PAID=1)"
        return True, "ok"

    def discover(self, ctx):
        self.usage = {"requests": 0, "input_tokens": 0, "output_tokens": 0}
        tool = {"type": "web_search", "search_context_size": self.config.get("search_context_size", "low")}
        if self.config.get("allowed_domains"):
            tool["filters"] = {"allowed_domains": self.config["allowed_domains"]}
        for query in ctx["queries"][: self.config.get("max_queries", 3)]:
            payload = {"model": self.config["model"], "tools": [tool],
                       "input": "Find public forum threads for this query and list each thread URL with its date "
                                "and a one-sentence note on what parents say. Query: " + query}
            data = request_json(ENDPOINT, data=json.dumps(payload).encode(), timeout=self.config.get("timeout", 90),
                                headers={"Authorization": f"Bearer {self.env['OPENAI_API_KEY']}",
                                         "Content-Type": "application/json"},
                                retries=self.config.get("retries", 1), transport=self.transport, sleep=self.sleep)
            self.usage["requests"] += 1
            for k in ("input_tokens", "output_tokens"):
                self.usage[k] += (data.get("usage") or {}).get(k, 0)
            for item in data.get("output", []):
                for part in item.get("content", []) if item.get("type") == "message" else []:
                    text = part.get("text", "")
                    for ann in part.get("annotations", []):
                        if ann.get("type") == "url_citation":
                            around = text[max(0, ann.get("start_index", 0) - 200): ann.get("end_index", 0)]
                            yield lead(ann["url"], source=self.name, title=ann.get("title", ""), query=query,
                                       snippet=around, note="model-written summary, not the original post")
            self.sleep(self.config.get("delay", 1.0))
