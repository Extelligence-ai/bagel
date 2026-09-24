"""Ask a typed-decision model (Jev-style) a multiple-choice question about a time window.

The window is summarized into compact per-signal statistics, the model returns a
probability for each choice, and the gate passes when the most likely choice is one of
the accepted ones with enough confidence. Below that confidence the gate abstains
(stays closed), so downstream tasks only run on decisions the model is sure of.

Two backends:

- ``remote`` (default): POST the summary to an HTTP decision endpoint. Needs no extra
  dependencies, so it works in every image, including CPU-only robots.
- ``local``: run a Hugging Face causal LM on the robot and score each choice by its
  log-likelihood. Needs the ``jev`` dependency group, which images only include when
  built with ``--build-arg JEV_MODE=true``.

"""

import dataclasses
import json
import logging
import math
import os
import urllib.parse
import urllib.request
from typing import Any, Protocol

import duckdb
import duckdb.typing

from settings import settings
from src.di import module
from src.pipeline import base, messages

_NUMERIC_TYPES = {
    "tinyint",
    "smallint",
    "integer",
    "bigint",
    "hugeint",
    "utinyint",
    "usmallint",
    "uinteger",
    "ubigint",
    "uhugeint",
    "float",
    "double",
    "decimal",
}


_MIN_CHOICES = 2


class Backend(Protocol):
    """Return a probability (or unnormalized score >= 0) for every choice."""

    def decide(self, state: dict, question: str, choices: list[str]) -> dict[str, float]:
        """Score each choice for the question given the window summary."""
        ...


@dataclasses.dataclass(frozen=True)
class Decision:
    """The outcome of one typed decision over a window."""

    choice: str
    probability: float
    probabilities: dict[str, float]
    abstained: bool
    passed: bool
    state: dict

    def to_json(self) -> str:
        """Serialize the decision, e.g. for logs, events or an audit record."""
        return json.dumps(dataclasses.asdict(self), sort_keys=True)


def _numeric_leaves(name: str, dtype: duckdb.typing.DuckDBPyType) -> list[tuple[str, str]]:
    """Return (dotted label, SQL expression) for every numeric leaf under a column."""
    if dtype.id == "struct":
        leaves = []
        for child, child_type in dtype.children:
            for label, expression in _numeric_leaves(f"{name}.{child}", child_type):
                leaves.append((label, expression))
        return leaves
    if dtype.id in _NUMERIC_TYPES:
        column, *path = name.split(".")
        expression = f'"{column}"' + "".join(f"['{field}']" for field in path)
        return [(name, expression)]
    return []


def summarize(relation: duckdb.DuckDBPyRelation) -> dict:
    """Summarize a window of messages into per-signal count/min/max/mean statistics."""
    ts = settings.TIMESTAMP_SECONDS_COLUMN_NAME
    leaves = [
        leaf
        for column, dtype in zip(relation.columns, relation.types, strict=True)
        if column != ts
        for leaf in _numeric_leaves(column, dtype)
    ]
    aggregates = ["count(*)", f"min({ts})::DOUBLE", f"max({ts})::DOUBLE"]
    for _, expression in leaves:
        aggregates += [
            f"count({expression})",
            f"min({expression})::DOUBLE",
            f"max({expression})::DOUBLE",
            f"avg({expression})::DOUBLE",
        ]
    messages_, start, end, *stats = relation.aggregate(", ".join(aggregates)).fetchall()[0]
    signals = {}
    for index, (label, _) in enumerate(leaves):
        count, minimum, maximum, mean = stats[4 * index : 4 * index + 4]
        signals[label] = {"count": count, "min": minimum, "max": maximum, "mean": mean}
    return {
        "window": {"start_seconds": start, "end_seconds": end, "messages": messages_},
        "signals": signals,
    }


def decide_window(  # noqa: PLR0913
    relation: duckdb.DuckDBPyRelation,
    backend: Backend,
    question: str,
    choices: list[str],
    accept: list[str],
    min_probability: float,
) -> Decision:
    """Summarize a window, ask the backend, and apply the acceptance rule."""
    state = summarize(relation)
    scores = backend.decide(state, question, choices)
    missing = [choice for choice in choices if choice not in scores]
    if missing:
        raise ValueError(f"Decision backend did not score choices: {missing}")
    total = sum(scores[choice] for choice in choices)
    if total <= 0:
        raise ValueError(f"Decision backend returned no positive scores: {scores}")
    probabilities = {choice: scores[choice] / total for choice in choices}
    choice = max(choices, key=probabilities.__getitem__)
    probability = probabilities[choice]
    abstained = probability < min_probability
    return Decision(
        choice=choice,
        probability=probability,
        probabilities=probabilities,
        abstained=abstained,
        passed=not abstained and choice in accept,
        state=state,
    )


class RemoteBackend:
    """POST ``{state, question, choices}``; expect ``{"probabilities": {choice: p}}``."""

    def __init__(self, url: str, timeout_seconds: float, api_key_env: str | None) -> None:
        """Initialize the backend."""
        self._url = url
        self._timeout_seconds = timeout_seconds
        self._api_key_env = api_key_env

    def decide(self, state: dict, question: str, choices: list[str]) -> dict[str, float]:
        """Implement `Backend.decide`."""
        headers = {"Content-Type": "application/json"}
        if self._api_key_env:
            headers["Authorization"] = f"Bearer {os.environ[self._api_key_env]}"
        body = json.dumps({"state": state, "question": question, "choices": choices})
        request = urllib.request.Request(  # noqa: S310 -- scheme enforced to http(s) in Decide
            self._url, data=body.encode(), headers=headers, method="POST"
        )
        with urllib.request.urlopen(request, timeout=self._timeout_seconds) as response:  # noqa: S310
            payload = json.load(response)
        return {str(k): float(v) for k, v in payload["probabilities"].items()}


class LocalBackend:
    """Score each choice by its log-likelihood under a causal LM, on the robot."""

    def __init__(self, model: str) -> None:
        """Load the model; requires the `jev` dependency group."""
        try:
            import torch
            import transformers
        except ImportError as error:
            raise ImportError(
                "The local decide backend needs the 'jev' dependency group. Build the "
                "image with --build-arg JEV_MODE=true, or use backend: remote on "
                "CPU-only robots."
            ) from error
        self._torch = torch
        self._tokenizer = transformers.AutoTokenizer.from_pretrained(model)
        self._model = transformers.AutoModelForCausalLM.from_pretrained(model)
        self._model.eval()

    def decide(self, state: dict, question: str, choices: list[str]) -> dict[str, float]:
        """Implement `Backend.decide`."""
        prompt = (
            f"State: {json.dumps(state, sort_keys=True)}\n"
            f"Question: {question}\n"
            f"Choices: {', '.join(choices)}\n"
            "Answer:"
        )
        prompt_ids = self._tokenizer(prompt, return_tensors="pt").input_ids
        log_likelihoods = {}
        with self._torch.no_grad():
            for choice in choices:
                choice_ids = self._tokenizer(" " + choice, return_tensors="pt").input_ids
                input_ids = self._torch.cat([prompt_ids, choice_ids], dim=1)
                logits = self._model(input_ids).logits
                log_probs = self._torch.log_softmax(logits[0, :-1], dim=-1)
                targets = input_ids[0, 1:]
                token_log_probs = log_probs.gather(1, targets.unsqueeze(1)).squeeze(1)
                log_likelihoods[choice] = token_log_probs[-choice_ids.shape[1] :].sum().item()
        best = max(log_likelihoods.values())
        return {choice: math.exp(value - best) for choice, value in log_likelihoods.items()}


class Decide(messages.TopicMessageMixin, base.Gate):
    """Ask a typed-decision model a multiple-choice question about the lookback window."""

    def __init__(  # noqa: PLR0913
        self,
        question: str,
        choices: list[str],
        accept: list[str],
        topics: list[str] | None = None,
        min_probability: float = 0.5,
        backend: str = "remote",
        url: str | None = None,
        model: str | None = None,
        timeout_seconds: float = 10.0,
        api_key_env: str | None = None,
    ) -> None:
        """Initialize the gate.

        Args:
            question (str): The question to ask about each window.
            choices (list[str]): The possible answers, e.g. ["upload", "keep_local", "discard"].
            accept (list[str]): The choices that open the gate.
            topics (list[str] | None, optional): The topics to summarize. If None, all topics.
            min_probability (float, optional): Below this probability for the top choice the
                gate abstains and stays closed. Defaults to 0.5.
            backend (str, optional): "remote" (HTTP endpoint) or "local" (on-robot model;
                needs an image built with JEV_MODE=true). Defaults to "remote".
            url (str | None, optional): The decision endpoint for the remote backend.
            model (str | None, optional): Hugging Face model id or path for the local backend.
            timeout_seconds (float, optional): Remote request timeout. Defaults to 10.
            api_key_env (str | None, optional): Environment variable holding a bearer token
                for the remote backend.

        Raises:
            ValueError: If the choices, accept list, probability or backend are invalid.
            ImportError: If the local backend is requested without the `jev` group.

        """
        if len(choices) < _MIN_CHOICES or len(set(choices)) != len(choices):
            raise ValueError(f"Choices must be at least two distinct values, got {choices}")
        if not accept or any(choice not in choices for choice in accept):
            raise ValueError(f"Accept must be a non-empty subset of choices: {accept}")
        if not (0.0 <= min_probability <= 1.0):
            raise ValueError("min_probability must be between 0 and 1")
        self._question = question
        self._choices = choices
        self._accept = accept
        self._topics = topics
        self._min_probability = min_probability
        self._backend: Backend
        if backend == "remote":
            if not url:
                raise ValueError("The remote decide backend requires a url")
            if urllib.parse.urlparse(url).scheme not in {"http", "https"}:
                raise ValueError(f"The remote decide backend url must be http(s), got {url!r}")
            self._backend = RemoteBackend(url, timeout_seconds, api_key_env)
        elif backend == "local":
            if not model:
                raise ValueError("The local decide backend requires a model")
            self._backend = LocalBackend(model)
        else:
            raise ValueError(f"Unknown decide backend {backend!r}; use 'remote' or 'local'")
        self.last_decision: Decision | None = None

    def evaluate(self, asof_seconds: float, lookback: base.Lookback | None) -> bool:
        """Implement `base.Gate.evaluate`."""
        relation = self.to_duckdb(topics=self._topics, asof_seconds=asof_seconds, lookback=lookback)
        self.last_decision = decide_window(
            relation,
            backend=self._backend,
            question=self._question,
            choices=self._choices,
            accept=self._accept,
            min_probability=self._min_probability,
        )
        summary: dict[str, Any] = dataclasses.asdict(self.last_decision)
        summary.pop("state")
        logging.info("decide gate %s at %s: %s", self.name, asof_seconds, json.dumps(summary))
        return self.last_decision.passed


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = Decide
