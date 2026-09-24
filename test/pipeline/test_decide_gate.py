"""Tests for the typed-decision gate (`src.pipeline.gates.decide`)."""

import http.server
import json
import threading
from collections.abc import Iterator

import duckdb
import pytest

from settings import settings
from src.pipeline import base
from src.pipeline.decide import backends
from src.pipeline.gates import decide

TS = settings.TIMESTAMP_SECONDS_COLUMN_NAME
CHOICES = ["upload", "keep_local", "discard"]


def _relation() -> duckdb.DuckDBPyRelation:
    return duckdb.sql(
        f"""
        SELECT * FROM (VALUES
            (10.0, {{'accel': {{'x': -2.0, 'y': 0.5}}, 'mode': 'auto'}}),
            (11.0, {{'accel': {{'x': -12.0, 'y': 0.0}}, 'mode': 'auto'}}),
            (12.0, {{'accel': {{'x': -4.0, 'y': 1.0}}, 'mode': 'manual'}})
        ) AS t({TS}, "/imu")
        """
    )


class _Fixed:
    """A backend that always returns the same probabilities."""

    def __init__(self, probabilities: dict[str, float]) -> None:
        self.probabilities = probabilities
        self.calls: list[dict] = []

    def decide(self, state: dict, question: str, choices: dict[str, str]) -> backends.Answer:
        self.calls.append({"state": state, "question": question, "choices": choices})
        return backends.Answer(self.probabilities, "fixed-model")


# --- decide_window -----------------------------------------------------------------------


def _decide(probabilities: dict[str, float], min_probability: float = 0.5) -> decide.Decision:
    return decide.decide_window(
        _relation(),
        backend=_Fixed(probabilities),
        question="Should this window leave the robot?",
        choices=CHOICES,
        accept=["upload"],
        min_probability=min_probability,
    )


def test_confident_accepted_choice_opens_the_gate() -> None:
    decision = _decide({"upload": 0.8, "keep_local": 0.15, "discard": 0.05})
    assert decision.choice == "upload"
    assert decision.probability == pytest.approx(0.8)
    assert decision.passed is True
    assert decision.abstained is False


def test_rejected_choice_keeps_the_gate_closed() -> None:
    decision = _decide({"upload": 0.1, "keep_local": 0.2, "discard": 0.7})
    assert decision.choice == "discard"
    assert decision.passed is False


def test_low_confidence_abstains_and_keeps_the_gate_closed() -> None:
    decision = _decide({"upload": 0.4, "keep_local": 0.35, "discard": 0.25})
    assert decision.choice == "upload"
    assert decision.abstained is True
    assert decision.passed is False


def test_probabilities_are_normalized() -> None:
    decision = _decide({"upload": 2.0, "keep_local": 1.0, "discard": 1.0})
    assert sum(decision.probabilities.values()) == pytest.approx(1.0)
    assert decision.probability == pytest.approx(0.5)


def test_backend_must_answer_every_choice() -> None:
    with pytest.raises(ValueError, match="keep_local"):
        _decide({"upload": 0.9, "discard": 0.1})


def test_backend_receives_the_window_summary() -> None:
    backend = _Fixed({"upload": 1.0, "keep_local": 0.0, "discard": 0.0})
    decide.decide_window(
        _relation(),
        backend=backend,
        question="q?",
        choices=CHOICES,
        accept=["upload"],
        min_probability=0.5,
    )
    (call,) = backend.calls
    assert call["question"] == "q?"
    assert list(call["choices"]) == CHOICES
    assert call["state"]["signals"]["/imu.accel.x"]["min"] == -12.0


def test_decision_serializes_to_json() -> None:
    decision = _decide({"upload": 0.8, "keep_local": 0.15, "discard": 0.05})
    payload = json.loads(decision.to_json())
    assert payload["choice"] == "upload"
    assert payload["passed"] is True
    assert payload["model"] == "fixed-model"
    assert set(payload["probabilities"]) == set(CHOICES)


def test_jev_backend_is_available_to_the_decide_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TYPESAFE_API_KEY", "k")
    gate = decide.Decide(question="q?", choices=CHOICES, accept=["upload"], backend="jev")
    assert isinstance(gate._backend, backends.JevBackend)


# --- configuration -----------------------------------------------------------------------


def test_accept_must_be_a_subset_of_choices() -> None:
    with pytest.raises(ValueError, match="bogus"):
        decide.Decide(question="q?", choices=CHOICES, accept=["bogus"], url="http://localhost:1")


def test_remote_backend_requires_a_url() -> None:
    with pytest.raises(ValueError, match="url"):
        decide.Decide(question="q?", choices=CHOICES, accept=["upload"], backend="remote")


def test_remote_backend_rejects_non_http_urls() -> None:
    with pytest.raises(ValueError, match="http"):
        decide.Decide(question="q?", choices=CHOICES, accept=["upload"], url="file:///etc/passwd")


def test_unknown_backend_is_rejected() -> None:
    with pytest.raises(ValueError, match="backend"):
        decide.Decide(question="q?", choices=CHOICES, accept=["upload"], backend="magic")


def test_gate_is_registered_and_importable_without_heavy_dependencies() -> None:
    from src.di import module

    decide.register()
    assert module.global_registry["src.pipeline.gates.decide"] is decide.Decide


# --- remote backend end to end -----------------------------------------------------------


@pytest.fixture
def decision_server() -> Iterator[tuple[str, list[dict]]]:
    received: list[dict] = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            received.append({"body": body, "auth": self.headers.get("Authorization")})
            minimum = body["state"]["signals"]["message.accel_x"]["min"]
            upload = 0.9 if minimum < -3 else 0.1
            reply = json.dumps(
                {"probabilities": {"upload": upload, "keep_local": 1 - upload, "discard": 0.0}}
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(reply)))
            self.end_headers()
            self.wfile.write(reply)

        def log_message(self, *args: object) -> None:
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}/decide", received
    server.shutdown()


def test_remote_gate_decides_over_a_real_source(
    decision_server: tuple[str, list[dict]], monkeypatch: pytest.MonkeyPatch
) -> None:
    url, received = decision_server
    monkeypatch.setenv("TEST_DECIDE_KEY", "secret")
    gate = base.Operator.build(
        pipeline="p",
        site="s",
        asset="a",
        path="./data/sample/pyarrow/csv",
        config={
            "module": "src.pipeline.gates.decide",
            "args": {
                "question": "Should this window leave the robot?",
                "choices": CHOICES,
                "accept": ["upload"],
                "url": url,
                "api_key_env": "TEST_DECIDE_KEY",
            },
        },
    )
    assert isinstance(gate, decide.Decide)

    passed = gate.evaluate(asof_seconds=1e12, lookback=None)

    (request,) = received
    assert request["auth"] == "Bearer secret"
    assert request["body"]["question"] == "Should this window leave the robot?"
    assert request["body"]["choices"] == CHOICES
    assert gate.last_decision is not None
    assert passed is gate.last_decision.passed
    assert gate.annotations()["choice"] == gate.last_decision.choice
    assert "state" not in gate.annotations()
