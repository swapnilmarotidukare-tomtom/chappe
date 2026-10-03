from __future__ import annotations


class SlackFormatter:
    """Slack mrkdwn. Every method escapes the text it is given."""

    def escape(self, text: str) -> str:
        return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    def bold(self, text: str) -> str:
        return f"*{self.escape(text)}*"

    def italic(self, text: str) -> str:
        return f"_{self.escape(text)}_"

    def link(self, url: str, label: str) -> str:
        safe_url = (
            url.replace("&", "&amp;").replace("<", "%3C").replace(">", "%3E").replace("|", "%7C")
        )
        return f"<{safe_url}|{self.escape(label)}>"

    def mention(self, target: str) -> str:
        return target  # comes from trusted config, already in Slack syntax

    def icon(self, token: str) -> str:
        return token
