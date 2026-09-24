"""A local HTTP stand-in for decision endpoints (TypeSafe Jev or a generic remote)."""

import http.server
import json
import threading
import time
from collections.abc import Callable, Iterator

import pytest

Reply = Callable[[dict], tuple[int, object]]


class DecisionServer:
    """Records requests and answers them with `reply(body) -> (status, json or text)`."""

    def __init__(self) -> None:
        self.requests: list[dict] = []
        self.reply: Reply = lambda body: (200, {})
        self.delay_seconds = 0.0
        self.port = 0

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}/v1/systemone"


def jev_reply(probabilities: dict[str, float], model: str = "jev-1.13.0") -> tuple[int, dict]:
    """A reply in the shape TypeSafe documents for /v1/systemone."""
    return (
        200,
        {
            "model": model,
            "answers": {
                "decision": {
                    "type": "choice",
                    "choice": max(probabilities, key=probabilities.__getitem__),
                    "probabilities": probabilities,
                    "confidence": 0.8,
                }
            },
            "usage": {"input_tokens": 312, "output_tokens": 48},
        },
    )


@pytest.fixture
def decision_server() -> Iterator[DecisionServer]:
    state = DecisionServer()

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            state.requests.append({"auth": self.headers.get("Authorization"), "path": self.path})
            self.send_response(200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"{}")

        def do_POST(self) -> None:
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            state.requests.append(
                {"body": body, "auth": self.headers.get("Authorization"), "path": self.path}
            )
            time.sleep(state.delay_seconds)
            status, reply, *extra = state.reply(body)
            data = reply.encode() if isinstance(reply, str) else json.dumps(reply).encode()
            self.send_response(status)
            for name, value in (extra[0] if extra else {}).items():
                self.send_header(name, value)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args: object) -> None:
            pass

    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    state.port = httpd.server_port
    threading.Thread(
        target=httpd.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True
    ).start()
    yield state
    httpd.shutdown()
