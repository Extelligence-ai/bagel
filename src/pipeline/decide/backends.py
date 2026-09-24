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


def _require_http(url: str) -> str:
    if urllib.parse.urlparse(url).scheme not in {"http", "https"}:
        raise ValueError(f"Decision backend url must be http(s), got {url!r}")
    return url


def _post_json(url: str, body: dict, headers: dict[str, str], timeout_seconds: float) -> Any:  # noqa: ANN401
    request = urllib.request.Request(  # noqa: S310 -- scheme checked by _require_http
        url,
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", **headers},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310
            return json.load(response)
    except urllib.error.HTTPError as error:
        raise BackendUnavailable(f"HTTP {error.code} from {url}") from error
    except TimeoutError as error:
        raise BackendUnavailable(f"Request to {url} timed out") from error
    except urllib.error.URLError as error:
        if isinstance(error.reason, TimeoutError):
            raise BackendUnavailable(f"Request to {url} timed out") from error
        raise BackendUnavailable(f"Cannot reach {url}: {error.reason}") from error
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise BackendUnavailable(f"Non-JSON reply from {url}") from error


def _probabilities(raw: Any, choices: dict[str, str]) -> dict[str, float]:  # noqa: ANN401
    try:
        scores = {str(k): float(v) for k, v in raw.items()}
    except (AttributeError, TypeError, ValueError) as error:
        raise BackendUnavailable(f"Malformed probabilities: {raw!r}") from error
    if missing := [choice for choice in choices if choice not in scores]:
        raise BackendUnavailable(f"Reply did not score choices {missing}")
    return {choice: scores[choice] for choice in choices}


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
        self._url = _require_http(url)
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
        """Initialize the backend."""
        self._url = _require_http(url)
        self._timeout_seconds = timeout_seconds
        self._api_key_env = api_key_env

    def decide(self, state: dict, question: str, choices: dict[str, str]) -> Answer:
        """Implement `Backend.decide`."""
        headers = {}
        if self._api_key_env:
            headers["Authorization"] = f"Bearer {os.environ[self._api_key_env]}"
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
        self._tokenizer = transformers.AutoTokenizer.from_pretrained(model)
        self._model = transformers.AutoModelForCausalLM.from_pretrained(model)
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
        prompt_ids = self._tokenizer(prompt, return_tensors="pt").input_ids
        log_likelihoods = {}
        with self._torch.no_grad():
            for choice in choices:
                choice_ids = self._tokenizer(" " + choice, return_tensors="pt").input_ids
                input_ids = self._torch.cat([prompt_ids, choice_ids], dim=1)
                log_probs = self._torch.log_softmax(self._model(input_ids).logits[0, :-1], dim=-1)
                targets = input_ids[0, 1:]
                token_log_probs = log_probs.gather(1, targets.unsqueeze(1)).squeeze(1)
                log_likelihoods[choice] = token_log_probs[-choice_ids.shape[1] :].sum().item()
        best = max(log_likelihoods.values())
        return Answer(
            {choice: math.exp(value - best) for choice, value in log_likelihoods.items()},
            self._name,
        )


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
