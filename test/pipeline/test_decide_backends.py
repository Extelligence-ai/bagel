"""Tests for typed-decision backends (`src.pipeline.decide.backends`)."""

import pytest

from src.pipeline.decide import backends
from test._fixtures.decision_server import DecisionServer, decision_server, jev_reply

CHOICES = {"stall": "wheels not moving", "other_unusual": "anything else", "normal": "fine"}
STATE = {"signals": {"/m.v": {"mean": 1.0}}}
server = decision_server


def _jev_reply(probabilities: dict[str, float]):  # noqa: ANN202
    return lambda body: jev_reply(probabilities)


@pytest.fixture
def jev(server: DecisionServer, monkeypatch: pytest.MonkeyPatch) -> backends.JevBackend:
    monkeypatch.setenv("TYPESAFE_API_KEY", "ts-key")
    return backends.JevBackend(url=server.url, timeout_seconds=0.5)


# --- Jev ---------------------------------------------------------------------------------


def test_jev_sends_one_choice_question_with_criteria(
    jev: backends.JevBackend, server: DecisionServer
) -> None:
    server.reply = _jev_reply({"stall": 0.7, "other_unusual": 0.2, "normal": 0.1})
    jev.decide(STATE, "What is happening?", CHOICES)
    (request,) = server.requests
    assert request["auth"] == "Bearer ts-key"
    assert request["body"] == {
        "model": "jev-latest",
        "state": STATE,
        "questions": {
            "decision": {
                "type": "choice",
                "instructions": "What is happening?",
                "criteria": CHOICES,
            }
        },
    }


def test_jev_returns_probabilities_and_the_answering_model(
    jev: backends.JevBackend, server: DecisionServer
) -> None:
    server.reply = _jev_reply({"stall": 0.7, "other_unusual": 0.2, "normal": 0.1})
    answer = jev.decide(STATE, "q?", CHOICES)
    assert answer.probabilities == {"stall": 0.7, "other_unusual": 0.2, "normal": 0.1}
    assert answer.model == "jev-1.13.0"


def test_jev_requires_its_api_key_up_front(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TYPESAFE_API_KEY", raising=False)
    with pytest.raises(ValueError, match="TYPESAFE_API_KEY"):
        backends.JevBackend()


def test_jev_rejects_non_http_urls(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TYPESAFE_API_KEY", "k")
    with pytest.raises(ValueError, match="http"):
        backends.JevBackend(url="file:///etc/passwd")


@pytest.mark.parametrize("status", [429, 500, 503, 401])
def test_jev_http_errors_are_unavailable(
    jev: backends.JevBackend, server: DecisionServer, status: int
) -> None:
    server.reply = lambda body: (status, {"error": "nope"})
    with pytest.raises(backends.BackendUnavailable, match=str(status)):
        jev.decide(STATE, "q?", CHOICES)


def test_jev_timeout_is_unavailable(jev: backends.JevBackend, server: DecisionServer) -> None:
    server.reply = _jev_reply({"stall": 1.0, "other_unusual": 0.0, "normal": 0.0})
    server.delay_seconds = 1.0
    with pytest.raises(backends.BackendUnavailable, match="timed out"):
        jev.decide(STATE, "q?", CHOICES)


def test_jev_connection_refused_is_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TYPESAFE_API_KEY", "k")
    backend = backends.JevBackend(url="http://127.0.0.1:9/v1/systemone", timeout_seconds=0.5)
    with pytest.raises(backends.BackendUnavailable):
        backend.decide(STATE, "q?", CHOICES)


def test_jev_reply_missing_a_choice_is_unavailable(
    jev: backends.JevBackend, server: DecisionServer
) -> None:
    server.reply = _jev_reply({"stall": 0.9, "normal": 0.1})
    with pytest.raises(backends.BackendUnavailable, match="other_unusual"):
        jev.decide(STATE, "q?", CHOICES)


def test_jev_reply_that_is_not_json_is_unavailable(
    jev: backends.JevBackend, server: DecisionServer
) -> None:
    server.reply = lambda body: (200, "<html>gateway</html>")
    with pytest.raises(backends.BackendUnavailable):
        jev.decide(STATE, "q?", CHOICES)


# --- generic remote ----------------------------------------------------------------------


def test_remote_posts_state_question_and_choice_names(server: DecisionServer) -> None:
    server.reply = lambda body: (200, {"probabilities": {c: 1.0 for c in body["choices"]}})
    answer = backends.RemoteBackend(server.url, timeout_seconds=1, api_key_env=None).decide(
        STATE, "q?", CHOICES
    )
    assert server.requests[0]["body"] == {
        "state": STATE,
        "question": "q?",
        "choices": list(CHOICES),
    }
    assert set(answer.probabilities) == set(CHOICES)
    assert answer.model is None


def test_remote_http_error_is_unavailable(server: DecisionServer) -> None:
    server.reply = lambda body: (502, {})
    backend = backends.RemoteBackend(server.url, timeout_seconds=1, api_key_env=None)
    with pytest.raises(backends.BackendUnavailable):
        backend.decide(STATE, "q?", CHOICES)


# --- local -------------------------------------------------------------------------------


def test_local_backend_without_the_jev_group_explains_the_build_flag() -> None:
    # Host CI does not install the `jev` group: this is the CPU-only-image experience.
    with pytest.raises(ImportError, match="JEV_MODE=true"):
        backends.LocalBackend("x/y")


# --- build -------------------------------------------------------------------------------


def test_build_selects_a_backend_by_name(
    server: DecisionServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TYPESAFE_API_KEY", "k")
    assert isinstance(backends.build("jev", url=server.url), backends.JevBackend)
    assert isinstance(backends.build("remote", url=server.url), backends.RemoteBackend)


def test_build_rejects_unknown_backends() -> None:
    with pytest.raises(ValueError, match="magic"):
        backends.build("magic")


def test_build_remote_requires_a_url() -> None:
    with pytest.raises(ValueError, match="url"):
        backends.build("remote")
