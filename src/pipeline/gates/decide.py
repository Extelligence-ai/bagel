"""Ask a typed-decision model (Jev-style) a multiple-choice question about a time window.

The window is summarized into compact per-signal statistics, the model returns a
probability for each choice, and the gate passes when the most likely choice is one of
the accepted ones with enough confidence. Below that confidence the gate abstains
(stays closed), so downstream tasks only run on decisions the model is sure of.

Backends (see `src.pipeline.decide.backends`): ``remote`` (default, any HTTP decision
endpoint), ``jev`` (TypeSafe's hosted model) and ``local`` (on the robot; needs an image
built with ``--build-arg JEV_MODE=true``). For anomaly detection with a baseline, use
the `anomaly` gate instead.
"""

import dataclasses
import json
import logging
from typing import Any

import duckdb

from src.di import module
from src.pipeline import base, messages
from src.pipeline.decide import backends, summary

_MIN_CHOICES = 2


@dataclasses.dataclass(frozen=True)
class Decision:
    """The outcome of one typed decision over a window."""

    choice: str
    probability: float
    probabilities: dict[str, float]
    abstained: bool
    passed: bool
    model: str | None
    state: dict

    def to_json(self) -> str:
        """Serialize the decision, e.g. for logs, events or an audit record."""
        return json.dumps(dataclasses.asdict(self), sort_keys=True)


def decide_window(  # noqa: PLR0913
    relation: duckdb.DuckDBPyRelation,
    backend: backends.Backend,
    question: str,
    choices: list[str],
    accept: list[str],
    min_probability: float,
    signals: summary.Signals | None = None,
) -> Decision:
    """Summarize a window, ask the backend, and apply the acceptance rule."""
    state = summary.summarize(relation, signals)
    answer = backend.decide(state, question, {choice: choice for choice in choices})
    scores = answer.probabilities
    if missing := [choice for choice in choices if choice not in scores]:
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
        model=answer.model,
        state=state,
    )


class Decide(messages.TopicMessageMixin, base.Gate):
    """Ask a typed-decision model a multiple-choice question about the lookback window."""

    def __init__(  # noqa: PLR0913
        self,
        question: str,
        choices: list[str],
        accept: list[str],
        topics: list[str] | None = None,
        signals: list[str] | None = None,
        max_signals: int = 64,
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
            signals (list[str] | None, optional): Dotted numeric signals to summarize. If
                None, every numeric field of the topics except `header`/`stamp` fields.
            max_signals (int, optional): Refuse to summarize more signals than this; each
                one adds to the query and to the request. Defaults to 64.
            min_probability (float, optional): Below this probability for the top choice the
                gate abstains and stays closed. Defaults to 0.5.
            backend (str, optional): "remote" (HTTP endpoint), "jev" (TypeSafe) or "local"
                (on-robot model; needs an image built with JEV_MODE=true). Defaults to
                "remote".
            url (str | None, optional): The decision endpoint (required for remote).
            model (str | None, optional): Model id or path (required for local).
            timeout_seconds (float, optional): Request timeout. Defaults to 10.
            api_key_env (str | None, optional): Environment variable holding a bearer token.

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
        self._signals = signals
        self._max_signals = max_signals
        self._resolved: summary.Signals | None = None
        self._min_probability = min_probability
        self._backend = backends.build(
            backend, model=model, url=url, api_key_env=api_key_env, timeout_seconds=timeout_seconds
        )
        self.last_decision: Decision | None = None

    def evaluate(self, asof_seconds: float, lookback: base.Lookback | None) -> bool:
        """Implement `base.Gate.evaluate`."""
        relation = self.to_duckdb(topics=self._topics, asof_seconds=asof_seconds, lookback=lookback)
        if self._resolved is None:
            if self._signals:
                resolved = summary.resolve_signals(relation, self._signals)
            else:
                resolved = summary.numeric_signals(relation)
            if len(resolved) > self._max_signals:
                raise ValueError(
                    f"{len(resolved)} numeric signals exceed max_signals={self._max_signals}. "
                    "Set 'topics' or 'signals' to the ones worth asking about, or raise "
                    "max_signals (every signal adds to the request)."
                )
            self._resolved = resolved
        try:
            self.last_decision = decide_window(
                relation,
                backend=self._backend,
                question=self._question,
                choices=self._choices,
                accept=self._accept,
                min_probability=self._min_probability,
                signals=self._resolved,
            )
        except backends.BackendUnavailable as error:
            # An outage closes the gate for this window instead of crashing the pipeline.
            logging.warning(
                "decide gate %s: backend unavailable (%s); abstaining", self.name, error
            )
            self.last_decision = None
            return False
        logging.info(
            "decide gate %s at %s: %s", self.name, asof_seconds, json.dumps(self.annotations())
        )
        return self.last_decision.passed

    def annotations(self) -> dict[str, Any]:
        """Implement `base.Gate.annotations`: the latest decision without its state."""
        if self.last_decision is None:
            return {}
        decision = dataclasses.asdict(self.last_decision)
        decision.pop("state")
        return decision


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = Decide
