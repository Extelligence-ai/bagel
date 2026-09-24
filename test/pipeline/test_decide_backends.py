"""Tests for typed-decision backends (`src.pipeline.decide.backends`)."""

import importlib.util
from collections.abc import Iterator

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
    if importlib.util.find_spec("torch") is not None:
        pytest.skip("torch is installed here; the CPU-only-image error path cannot be exercised")
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


# --- robustness (from pre-release review) ------------------------------------------------


@pytest.fixture
def hangup_url() -> Iterator[str]:
    """A server that starts a reply and drops the connection mid-body."""
    import socket
    import threading

    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(4)
    stop = threading.Event()

    def serve() -> None:
        listener.settimeout(0.1)
        while not stop.is_set():
            try:
                conn, _ = listener.accept()
            except TimeoutError:
                continue
            # Headers arrive, then the body is cut short: urllib raises
            # http.client.IncompleteRead from read(), not a URLError.
            conn.recv(4096)
            conn.sendall(b'HTTP/1.1 200 OK\r\nContent-Length: 500\r\n\r\n{"answers": {')
            conn.close()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{listener.getsockname()[1]}/v1/systemone"
    stop.set()
    thread.join()
    listener.close()


def test_a_dropped_connection_is_unavailable(
    hangup_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # RemoteDisconnected / ConnectionResetError are not URLError subclasses; they used to
    # escape the fallback and crash the pipeline instead of yielding screen_only.
    monkeypatch.setenv("TYPESAFE_API_KEY", "k")
    backend = backends.JevBackend(url=hangup_url, timeout_seconds=1)
    with pytest.raises(backends.BackendUnavailable):
        backend.decide(STATE, "q?", CHOICES)


def test_remote_backend_requires_its_api_key_env_up_front(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MY_KEY", raising=False)
    with pytest.raises(ValueError, match="MY_KEY"):
        backends.RemoteBackend("http://127.0.0.1:9/", timeout_seconds=1, api_key_env="MY_KEY")


@pytest.mark.parametrize("bad", [-1.0, float("nan"), float("inf")])
def test_non_probability_scores_are_unavailable(
    jev: backends.JevBackend, server: DecisionServer, bad: float
) -> None:
    server.reply = _jev_reply({"stall": bad, "other_unusual": 0.1, "normal": 0.1})
    with pytest.raises(backends.BackendUnavailable):
        jev.decide(STATE, "q?", CHOICES)


def test_jev_url_override_must_be_https_unless_local(monkeypatch: pytest.MonkeyPatch) -> None:
    # The bearer key would otherwise travel in clear text.
    monkeypatch.setenv("TYPESAFE_API_KEY", "k")
    with pytest.raises(ValueError, match="https"):
        backends.JevBackend(url="http://proxy.example.com/v1/systemone")
    backends.JevBackend(url="http://127.0.0.1:9/v1/systemone")  # loopback is fine
    backends.JevBackend(url="http://localhost:9/v1/systemone")


def test_an_all_zero_reply_is_unavailable(jev: backends.JevBackend, server: DecisionServer) -> None:
    # Every choice at 0 is not a distribution; accepting it made the anomaly gate pick the
    # first choice with zero confidence and drop a flagged slice (Codex P1).
    server.reply = _jev_reply({"stall": 0.0, "other_unusual": 0.0, "normal": 0.0})
    with pytest.raises(backends.BackendUnavailable, match="positive"):
        jev.decide(STATE, "q?", CHOICES)


def test_local_choices_are_tokenized_without_sequence_start_tokens() -> None:
    # Llama-style tokenizers prepend BOS by default; a BOS mid-sequence would score the
    # choice as a fresh sequence instead of the prompt's continuation (Codex P2).
    calls: list[dict] = []

    class Tokenizer:
        def __call__(self, text: str, **kwargs: object) -> object:
            calls.append({"text": text, **kwargs})
            return type("Encoded", (), {"input_ids": [[1, 2]]})()

    backends.choice_ids(Tokenizer(), "stall")
    assert calls == [{"text": " stall", "return_tensors": "pt", "add_special_tokens": False}]
