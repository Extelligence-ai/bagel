"""Tests for the Slack notification task, against a real local HTTP server."""

import http.server
import json
import threading
import urllib.error
from typing import ClassVar

import pytest

from bagel import server as bagel_server
from bagel.pipeline.tasks.notify.slack import NotifySlack

SAMPLE = "./data/sample/pyarrow/csv/flight.csv"


class _Receiver(http.server.BaseHTTPRequestHandler):
    received: ClassVar[list[dict]] = []

    def do_POST(self) -> None:
        length = int(self.headers["Content-Length"])
        _Receiver.received.append(json.loads(self.rfile.read(length)))
        self.send_response(500 if self.path == "/fail" else 200)
        self.end_headers()

    def log_message(self, *args) -> None:  # noqa: ANN002 -- silence request logging
        pass


@pytest.fixture()
def webhook() -> tuple[str, list[dict]]:
    _Receiver.received = []
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Receiver)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{httpd.server_port}", _Receiver.received
    httpd.shutdown()


def _bare_task(url: str, message: str, **kwargs) -> NotifySlack:  # noqa: ANN003
    task = NotifySlack(webhook_url=url, message=message, **kwargs)
    # Pipeline.build sets these; unit tests set them directly.
    task._name = "notify"
    task._pipeline = "brake_alerts"
    task._site = "warehouse"
    task._asset = "forklift_7"
    task._path = SAMPLE
    task._log_id = "log"
    task._upload = False
    return task


def test_posts_rendered_template_with_context(webhook: tuple[str, list[dict]]) -> None:
    url, received = webhook
    task = _bare_task(url + "/hook", "🚨 hard brake on {asset} at t={asof_seconds}")

    task.execute(asof_seconds=42.5, lookback=None)

    assert len(received) == 1
    text = received[0]["text"]
    assert "🚨 hard brake on forklift_7 at t=42.500" in text
    assert "`pipeline=brake_alerts site=warehouse asset=forklift_7 asof=42.500s`" in text


def test_unknown_placeholder_is_preserved_and_context_optional(
    webhook: tuple[str, list[dict]],
) -> None:
    url, received = webhook
    task = _bare_task(url + "/hook", "value {not_a_field}", include_context=False)

    task.execute(asof_seconds=1.0, lookback=None)

    assert received[0]["text"] == "value {not_a_field}"


def test_http_error_fails_the_task(webhook: tuple[str, list[dict]]) -> None:
    url, _ = webhook
    task = _bare_task(url + "/fail", "boom")

    with pytest.raises(urllib.error.HTTPError):
        task.execute(asof_seconds=1.0, lookback=None)


def test_rejects_non_http_webhook_url() -> None:
    with pytest.raises(ValueError, match="http"):
        NotifySlack(webhook_url="file:///etc/passwd", message="x")


def test_listed_in_pipeline_capabilities() -> None:
    modules = {c["module"] for c in bagel_server.list_pipeline_capabilities()}
    assert "bagel.pipeline.tasks.notify.slack" in modules


def test_end_to_end_through_run_pipeline(webhook: tuple[str, list[dict]]) -> None:
    url, received = webhook
    config = {
        "name": "notify_smoke",
        "site": "test_site",
        "asset": "test_asset",
        "path": SAMPLE,
        "allow_failure": False,
        "cadence": {"topic": "message", "when": "once_at_end"},
        "tasks": [
            {
                "module": "bagel.pipeline.tasks.notify.slack",
                "setup": {"timestamp_column": "t", "timestamp_format": "seconds"},
                "args": {
                    "webhook_url": url + "/hook",
                    "message": "{pipeline} finished on {asset}",
                },
            }
        ],
    }

    result = bagel_server.run_pipeline(config)

    assert result["status"] == "completed"
    assert len(received) == 1
    assert "notify_smoke finished on test_asset" in received[0]["text"]
