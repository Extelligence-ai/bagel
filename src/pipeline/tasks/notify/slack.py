"""Post a pipeline notification to a Slack incoming webhook."""

import json
import logging
import urllib.request

from src.di import module
from src.pipeline import base


class _SafeDict(dict):
    """Leave unknown placeholders intact instead of raising KeyError."""

    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


class NotifySlack(base.Task):
    """Post a pipeline notification to a Slack incoming webhook.

    The message is a template: `{pipeline}`, `{site}`, `{asset}`, `{path}`, and
    `{asof_seconds}` are filled in at execution time, so an on-event pipeline can
    post messages like "hard brake on {asset} at t={asof_seconds}". Unknown
    placeholders are left as-is.

    The payload is `{"text": ...}`, which Slack-compatible webhooks (Mattermost,
    Rocket.Chat, etc.) also accept.
    """

    def __init__(
        self,
        webhook_url: str,
        message: str,
        include_context: bool = True,
        timeout_seconds: int = 10,
    ) -> None:
        """Initialize the task.

        Args:
            webhook_url (str): The Slack incoming-webhook URL
                (https://api.slack.com/messaging/webhooks).
            message (str): Message template. Placeholders `{pipeline}`, `{site}`,
                `{asset}`, `{path}`, and `{asof_seconds}` are substituted at
                execution time.
            include_context (bool, optional): Append a context line with the
                pipeline name, site, asset, and timestamp to the message.
            timeout_seconds (int, optional): HTTP timeout for the webhook call.

        """
        if not webhook_url.startswith(("https://", "http://")):
            # The webhook URL *is* the secret (Slack/Mattermost/Rocket.Chat webhook
            # URLs embed a bearer token in the path) -- never echo it back, even to
            # say it was rejected.
            raise ValueError("webhook_url must be http(s) (value withheld).")
        self._webhook_url = webhook_url
        self._message = message
        self._include_context = include_context
        self._timeout_seconds = timeout_seconds

    def setup(self, path: str, **kwargs) -> None:  # noqa: ANN003
        """Nothing to set up in this task."""

    def execute(self, asof_seconds: float, lookback: base.Lookback | None) -> None:
        """Post the rendered message to the webhook at the given time."""
        context = _SafeDict(
            pipeline=self.pipeline,
            site=self.site,
            asset=self.asset,
            path=self.path,
            asof_seconds=f"{asof_seconds:.3f}",
        )
        text = self._message.format_map(context)
        if self._include_context:
            text += (
                f"\n`pipeline={self.pipeline} site={self.site} "
                f"asset={self.asset} asof={asof_seconds:.3f}s`"
            )
        request = urllib.request.Request(  # noqa: S310 -- scheme enforced to http(s) in __init__
            self._webhook_url,
            data=json.dumps({"text": text}).encode(),
            headers={"Content-Type": "application/json"},
        )
        # urlopen raises on non-2xx, so a failed post fails the task (and the
        # pipeline's allow_failure setting decides what happens next).
        with urllib.request.urlopen(request, timeout=self._timeout_seconds):  # noqa: S310 -- scheme enforced in __init__
            pass
        logging.info("Posted pipeline notification for %s", self.pipeline)


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = NotifySlack
