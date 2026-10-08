"""Typed-decision backends: ask a model to put a probability on each choice.

- ``jev``: TypeSafe's hosted System One model (``POST /v1/systemone``). Stdlib only, so
  it works in every image, including CPU-only robots. Point `url` at a proxy (e.g. a
  LiteLLM pass-through) if the robot should not call TypeSafe directly.
- ``remote``: any HTTP endpoint that accepts ``{state, question, choices}`` and
  answers ``{"probabilities": {choice: p}}``.
- ``local``: a Hugging Face causal LM on the robot, scoring each choice by its
  log-likelihood. Needs the ``jev`` dependency group (images built with
  ``--build-arg JEV_MODE=true``).

Every network or format failure raises `BackendUnavailable`, so callers can fall back
instead of crashing the pipeline.
"""

import dataclasses
import http.client
import json
import math
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Protocol

JEV_URL = "https://api.typesafe.ai/v1/systemone"
JEV_MODEL = "jev-latest"
JEV_API_KEY_ENV = "TYPESAFE_API_KEY"
_QUESTION = "decision"


class BackendUnavailable(Exception):  # noqa: N818 -- reads as a state, not an error class
    """The backend could not produce a usable answer (network, HTTP or format error)."""


@dataclasses.dataclass(frozen=True)
class Answer:
    """Scores per choice (not necessarily normalized) and the model that answered."""

    probabilities: dict[str, float]
    model: str | None


class Backend(Protocol):
    """Put a score on every choice for a question about a window."""

    def decide(self, state: dict, question: str, choices: dict[str, str]) -> Answer:
        """Answer `question` given `state`; `choices` maps each choice to a description."""
        ...


_LOOPBACK_HOSTS = {"localhost", "127.0.0.1", "::1"}


def _require_http(url: str, secret: bool = False) -> str:
    parts = urllib.parse.urlparse(url)
    if parts.scheme not in {"http", "https"}:
        raise ValueError(f"Decision backend url must be http(s), got {url!r}")
    if secret and parts.scheme == "http" and parts.hostname not in _LOOPBACK_HOSTS:
        raise ValueError(
            f"Refusing to send the API key over plain http to {parts.hostname!r}; use https "
            "(plain http is allowed for localhost proxies only)"
        )
    return url


def _redacted(url: str) -> str:
    """Return the url without any userinfo, for log and error messages."""
    parts = urllib.parse.urlparse(url)
    return f"{parts.scheme}://{parts.hostname or ''}{parts.path}"


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Never follow redirects: urllib would re-send every header, the bearer key included."""

    def redirect_request(  # noqa: PLR0913 -- urllib's signature
        self,
        req: Any,  # noqa: ANN401
        fp: Any,  # noqa: ANN401
        code: int,
        msg: str,
        headers: Any,  # noqa: ANN401
        newurl: str,
    ) -> None:
        return None


_OPENER = urllib.request.build_opener(_NoRedirect)


def _post_json(url: str, body: dict, headers: dict[str, str], timeout_seconds: float) -> Any:  # noqa: ANN401
    request = urllib.request.Request(  # noqa: S310 -- scheme checked by _require_http
        url,
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", **headers},
        method="POST",
    )
    where = _redacted(url)
    try:
        with _OPENER.open(request, timeout=timeout_seconds) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        if 300 <= error.code < 400:  # noqa: PLR2004
            raise BackendUnavailable(
                f"Refusing to follow a redirect ({error.code}) from {where}"
            ) from error
        raise BackendUnavailable(f"HTTP {error.code} from {where}") from error
    except TimeoutError as error:
        raise BackendUnavailable(f"Request to {where} timed out") from error
    except urllib.error.URLError as error:
        if isinstance(error.reason, TimeoutError):
            raise BackendUnavailable(f"Request to {where} timed out") from error
        raise BackendUnavailable(f"Cannot reach {where}: {error.reason}") from error
    except (OSError, http.client.HTTPException) as error:
        # Resets, RemoteDisconnected and IncompleteRead surface raw from read().
        raise BackendUnavailable(f"Connection to {where} failed: {error!r}") from error
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise BackendUnavailable(f"Non-JSON reply from {where}") from error


def _probabilities(raw: Any, choices: dict[str, str]) -> dict[str, float]:  # noqa: ANN401
    try:
        scores = {str(k): float(v) for k, v in raw.items()}
    except (AttributeError, TypeError, ValueError) as error:
        raise BackendUnavailable(f"Malformed probabilities: {raw!r}") from error
    if missing := [choice for choice in choices if choice not in scores]:
        raise BackendUnavailable(f"Reply did not score choices {missing}")
    if bad := [c for c in choices if not math.isfinite(scores[c]) or scores[c] < 0]:
        raise BackendUnavailable(f"Reply scored choices with non-probabilities: {bad}")
    if not any(scores[choice] > 0 for choice in choices):
        raise BackendUnavailable("Reply gave no choice a positive score")
    return {choice: scores[choice] for choice in choices}


def normalize_log_likelihoods(
    log_likelihoods: dict[str, float], lengths: dict[str, int]
) -> dict[str, float]:
    """Turn summed token log-probs into comparable scores in (0, 1].

    Summed log-probs penalize longer names: a choice whose tokens extend another's can
    never outrank it. Compare the mean log-prob per token instead, then shift so the
    best choice scores 1.
    """
    per_token = {c: log_likelihoods[c] / max(lengths[c], 1) for c in log_likelihoods}
    if not all(math.isfinite(v) for v in per_token.values()):
        raise BackendUnavailable("Local model produced non-finite scores")
    best = max(per_token.values())
    return {choice: math.exp(value - best) for choice, value in per_token.items()}


def choice_ids(tokenizer: Any, choice: str) -> Any:  # noqa: ANN401
    """Tokenize a choice as a continuation of the prompt, never as a new sequence.

    Llama-style tokenizers prepend BOS by default; concatenated after the prompt that
    would score the choice as a fresh sequence instead of the prompt's continuation.
    """
    return tokenizer(" " + choice, return_tensors="pt", add_special_tokens=False).input_ids


class JevBackend:
    """TypeSafe Jev: one `choice` question whose criteria are the choice descriptions."""

    def __init__(
        self,
        model: str = JEV_MODEL,
        url: str = JEV_URL,
        api_key_env: str = JEV_API_KEY_ENV,
        timeout_seconds: float = 10.0,
    ) -> None:
        """Initialize the backend; fails now if the API key is not configured."""
        self._api_key = os.environ.get(api_key_env)
        if not self._api_key:
            raise ValueError(f"The jev backend needs an API key in ${api_key_env}")
        self._model = model
        self._url = _require_http(url, secret=True)
        self._timeout_seconds = timeout_seconds

    def decide(self, state: dict, question: str, choices: dict[str, str]) -> Answer:
        """Implement `Backend.decide`."""
        body = {
            "model": self._model,
            "state": state,
            "questions": {
                _QUESTION: {"type": "choice", "instructions": question, "criteria": choices}
            },
        }
        reply = _post_json(
            self._url, body, {"Authorization": f"Bearer {self._api_key}"}, self._timeout_seconds
        )
        try:
            raw = reply["answers"][_QUESTION]["probabilities"]
        except (KeyError, TypeError) as error:
            raise BackendUnavailable(f"Reply has no answer for {_QUESTION!r}") from error
        model = reply.get("model") if isinstance(reply, dict) else None
        return Answer(_probabilities(raw, choices), model)


class RemoteBackend:
    """A generic endpoint: POST ``{state, question, choices}``, get ``{"probabilities"}``."""

    def __init__(self, url: str, timeout_seconds: float, api_key_env: str | None) -> None:
        """Initialize the backend; fails now if a named API key variable is unset."""
        self._api_key = os.environ.get(api_key_env) if api_key_env else None
        if api_key_env and not self._api_key:
            raise ValueError(f"The remote decision backend needs an API key in ${api_key_env}")
        self._url = _require_http(url, secret=bool(self._api_key))
        self._timeout_seconds = timeout_seconds

    def decide(self, state: dict, question: str, choices: dict[str, str]) -> Answer:
        """Implement `Backend.decide`."""
        headers = {}
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"
        body = {"state": state, "question": question, "choices": list(choices)}
        reply = _post_json(self._url, body, headers, self._timeout_seconds)
        try:
            raw = reply["probabilities"]
        except (KeyError, TypeError) as error:
            raise BackendUnavailable("Reply has no 'probabilities'") from error
        return Answer(_probabilities(raw, choices), reply.get("model"))


class LocalBackend:
    """Score each choice by its log-likelihood under a causal LM, on the robot."""

    def __init__(self, model: str) -> None:
        """Load the model; requires the `jev` dependency group."""
        try:
            import torch
            import transformers
        except ImportError as error:
            raise ImportError(
                "The local decision backend needs the 'jev' dependency group. Build the "
                "image with --build-arg JEV_MODE=true, or use the jev/remote backend on "
                "CPU-only robots."
            ) from error
        self._torch = torch
        self._name = model
        self._device = "cuda" if torch.cuda.is_available() else "cpu"
        self._tokenizer = transformers.AutoTokenizer.from_pretrained(model)
        self._model = transformers.AutoModelForCausalLM.from_pretrained(model).to(self._device)
        self._model.eval()

    def decide(self, state: dict, question: str, choices: dict[str, str]) -> Answer:
        """Implement `Backend.decide`."""
        options = "; ".join(f"{name}: {text}" for name, text in choices.items())
        prompt = (
            f"State: {json.dumps(state, sort_keys=True)}\n"
            f"Question: {question}\n"
            f"Choices: {options}\n"
            "Answer:"
        )
        try:
            return self._score(prompt, choices)
        except (RuntimeError, ValueError, IndexError, OSError) as error:
            # CUDA OOM, a prompt beyond the model's context, a tokenizer error.
            raise BackendUnavailable(f"Local model failed: {error}") from error

    def _score(self, prompt: str, choices: dict[str, str]) -> Answer:
        prompt_ids = self._tokenizer(prompt, return_tensors="pt").input_ids.to(self._device)
        log_likelihoods = {}
        lengths = {}
        with self._torch.no_grad():
            for choice in choices:
                encoded = choice_ids(self._tokenizer, choice).to(self._device)
                input_ids = self._torch.cat([prompt_ids, encoded], dim=1)
                log_probs = self._torch.log_softmax(self._model(input_ids).logits[0, :-1], dim=-1)
                targets = input_ids[0, 1:]
                token_log_probs = log_probs.gather(1, targets.unsqueeze(1)).squeeze(1)
                log_likelihoods[choice] = token_log_probs[-encoded.shape[1] :].sum().item()
                lengths[choice] = int(encoded.shape[1])
        return Answer(normalize_log_likelihoods(log_likelihoods, lengths), self._name)


def build(
    backend: str,
    model: str | None = None,
    url: str | None = None,
    api_key_env: str | None = None,
    timeout_seconds: float = 10.0,
) -> Backend:
    """Construct a backend from pipeline arguments.

    Raises:
        ValueError: If the backend is unknown or its required arguments are missing.
        ImportError: If `local` is requested in an image without the `jev` group.

    """
    match backend:
        case "jev":
            return JevBackend(
                model=model or JEV_MODEL,
                url=url or JEV_URL,
                api_key_env=api_key_env or JEV_API_KEY_ENV,
                timeout_seconds=timeout_seconds,
            )
        case "remote":
            if not url:
                raise ValueError("The remote decision backend requires a url")
            return RemoteBackend(url, timeout_seconds, api_key_env)
        case "local":
            if not model:
                raise ValueError("The local decision backend requires a model")
            return LocalBackend(model)
        case _:
            raise ValueError(f"Unknown decision backend {backend!r}; use jev, remote or local")
