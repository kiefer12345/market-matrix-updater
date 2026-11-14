"""What every source adapter provides.

discover(ctx) yields leads (URLs worth a look).
read(lead, ctx) yields posts for leads this adapter can actually open.
A source that cannot be used returns a status instead of raising, so one
unavailable channel never stops the run.
"""
from __future__ import annotations


class Adapter:
    name = "base"
    acquisition = "direct_fetch"      # how text from this adapter was obtained
    storage_allowed = True            # may an excerpt be kept in the repository?
    persist_leads = True              # may discovered URLs be written to disk?
    paid = False                      # does a call cost money?

    def __init__(self, config: dict, env: dict, transport=None, sleep=None):
        self.config, self.env, self.transport = config, env, transport
        self.sleep = sleep or (lambda s: None if config.get("no_delay") else __import__("time").sleep(s))

    def availability(self) -> tuple[bool, str]:
        """(usable, reason). Reason is shown in the run report when not usable."""
        return True, "ok"

    def discover(self, ctx):
        return []

    def read(self, lead, ctx):
        """Yield dicts: post_id, text, published, completeness. Default: cannot read."""
        return []
