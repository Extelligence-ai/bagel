"""Detect anomalies on the robot and ask Jev to label them. BETA.

Each window is summarized and compared against a rolling baseline learned on the
robot. In ``screen`` mode (default) a cheap check -- a window mean or single sample far
outside the baseline spread, or an expected topic that stops publishing -- decides
whether to ask Jev; in ``always`` mode Jev is asked about every window. Jev picks one of
the named anomaly types, ``other_unusual`` or ``normal``. The gate passes for anything
but a confident ``normal``, so downstream tasks (snippet, write_annotations, upload)
keep only anomalous log segments, each with a JSON label from `annotations()`.

If Jev cannot be reached, windows the screen flagged still pass, labelled
``screen_only`` (``verified: false``), so no anomaly is lost while offline.

BETA limits: batch (recorded) sources only -- the Jev call is synchronous and would
block a live ingest thread. The Jev backend has been exercised against live Jev via
Vercel AI Gateway; a direct TypeSafe key has not been used yet.
"""

import logging
import math
from typing import Any

from src.di import module
from src.pipeline import base, messages
from src.pipeline.decide import backends, baseline, screen, summary

NORMAL = "normal"
OTHER = "other_unusual"
SCREEN_ONLY = "screen_only"
MODES = ("screen", "always")
DEFAULT_QUESTION = (
    "Compared with this robot's baseline, does this window of sensor data show one of "
    "these anomalies, something else unusual, or normal operation?"
)


def _present_topics(
    window: dict, last_seen: dict[str, float], asof_seconds: float, dropout_seconds: float
) -> set[str]:
    """Topics that published in the window, or are silent but still within their grace."""
    return {
        topic
        for topic, stats in window["topics"].items()
        if stats["messages"]
        or (topic in last_seen and asof_seconds - last_seen[topic] <= dropout_seconds)
    }


def _validate_names(anomalies: dict[str, str], mode: str) -> None:
    """Reject anomaly names, descriptions and modes the gate cannot run with."""
    if not anomalies:
        raise ValueError("The anomaly gate needs at least one named type in 'anomalies'")
    if any(not isinstance(k, str) or not isinstance(v, str) for k, v in anomalies.items()):
        raise ValueError(
            "Anomaly names and descriptions must be strings. YAML reads bare yes/no/on/off "
            "as booleans: quote them."
        )
    if reserved := sorted({NORMAL, OTHER, SCREEN_ONLY} & set(anomalies)):
        raise ValueError(f"Anomaly names {reserved} are reserved labels")
    if mode not in MODES:
        raise ValueError(f"Unknown mode {mode!r}; use one of {MODES}")


def _validate_numbers(  # noqa: PLR0913
    z_threshold: float,
    dropout_seconds: float,
    timeout_seconds: float,
    baseline_window_minutes: float,
    warmup_minutes: float,
    min_probability: float,
    max_signals: int,
) -> None:
    """Reject numeric settings the gate cannot run with."""
    numbers = (
        z_threshold,
        dropout_seconds,
        timeout_seconds,
        baseline_window_minutes,
        warmup_minutes,
    )
    if not all(math.isfinite(n) for n in numbers):
        raise ValueError("Numeric settings must be finite (YAML `.nan`/`.inf` are not)")
    if z_threshold <= 0 or dropout_seconds <= 0 or timeout_seconds <= 0:
        raise ValueError("z_threshold, dropout_seconds and timeout_seconds must be positive")
    if baseline_window_minutes <= 0 or warmup_minutes < 0:
        raise ValueError("baseline_window_minutes must be positive and warmup_minutes non-negative")
    if not (0.0 <= min_probability <= 1.0):
        raise ValueError("min_probability must be between 0 and 1")
    if not isinstance(max_signals, int) or isinstance(max_signals, bool) or max_signals < 1:
        raise ValueError("max_signals must be a whole number of at least 1")
    if warmup_minutes > baseline_window_minutes:
        raise ValueError(
            "warmup_minutes must not exceed baseline_window_minutes: the baseline forgets "
            "history faster than warm-up needs it and would never become ready"
        )


class Anomaly(messages.TopicMessageMixin, base.Gate):
    """Detect anomalies on the robot and ask Jev to label them. BETA."""

    live_safe = False  # synchronous backend call: recorded sources only

    def __init__(  # noqa: PLR0913
        self,
        anomalies: dict[str, str],
        topics: list[str] | None = None,
        signals: list[str] | None = None,
        max_signals: int = 64,
        mode: str = "screen",
        z_threshold: float = 3.0,
        dropout_seconds: float = 2.0,
        baseline_window_minutes: float = 30.0,
        warmup_minutes: float = 5.0,
        min_probability: float = 0.6,
        question: str = DEFAULT_QUESTION,
        backend: str = "jev",
        model: str | None = None,
        url: str | None = None,
        api_key_env: str | None = None,
        timeout_seconds: float = 10.0,
    ) -> None:
        """Initialize the gate.

        Args:
            anomalies (dict[str, str]): Named anomaly types and a plain-language
                description of each, e.g. {"stall": "velocity commanded but no motion"}.
            topics (list[str] | None, optional): Topics to watch. If None, all topics.
            signals (list[str] | None, optional): Dotted numeric signals to watch, e.g.
                "/imu.linear_acceleration.x". If None, every numeric field of the watched
                topics except `header`/`stamp` fields; `[]` watches no signals, so only
                dropouts can trip the screen.
            max_signals (int, optional): Refuse to watch more signals than this. Every
                signal adds to the summary query and to the request sent to Jev (64k
                token context); PX4 logs expose ~2000. Defaults to 64.
            mode (str, optional): "screen" asks Jev only about windows the on-robot
                check flags; "always" asks about every window. Defaults to "screen".
            z_threshold (float, optional): Flag a window whose mean sits this many
                baseline standard deviations from the baseline mean; single samples
                must clear this plus the expected extreme for the sample count.
            dropout_seconds (float, optional): Flag a topic silent for longer than this.
                Only topics that publish in most baseline windows count, and never the
                cadence topic (the pipeline only runs when it publishes).
            baseline_window_minutes (float, optional): Span of the rolling baseline.
            warmup_minutes (float, optional): History needed before screening starts.
            min_probability (float, optional): Confidence Jev needs for its label to
                count. A less confident answer counts as normal. Defaults to 0.6.
            question (str, optional): The instructions given to Jev.
            backend (str, optional): "jev" (TypeSafe, default), "remote" or "local".
            model (str | None, optional): Model id; defaults to "jev-latest" for jev.
            url (str | None, optional): Endpoint override, e.g. a LiteLLM proxy.
            api_key_env (str | None, optional): Env var with the API key; defaults to
                TYPESAFE_API_KEY for jev.
            timeout_seconds (float, optional): Request timeout. Defaults to 10.

        Raises:
            ValueError: On invalid arguments, or a missing API key for the jev backend.

        """
        logging.warning("The anomaly gate is BETA: recorded (batch) sources only.")
        _validate_names(anomalies, mode)
        _validate_numbers(
            z_threshold,
            dropout_seconds,
            timeout_seconds,
            baseline_window_minutes,
            warmup_minutes,
            min_probability,
            max_signals,
        )
        self._choices = {
            **anomalies,
            OTHER: "an anomaly that is none of the named types",
            NORMAL: "normal operation for this robot",
        }
        self._topics = topics
        self._signals = signals
        self._max_signals = max_signals
        self._resolved: summary.Signals | None = None
        self._mode = mode
        self._z_threshold = z_threshold
        self._dropout_seconds = dropout_seconds
        self._min_probability = min_probability
        self._question = question
        self._backend = backends.build(
            backend, model=model, url=url, api_key_env=api_key_env, timeout_seconds=timeout_seconds
        )
        self.baseline: baseline.Baseline = baseline.RollingBaseline(
            baseline_window_minutes, warmup_minutes
        )
        self._annotations: dict[str, Any] = {}
        self._last_seen: dict[str, float] = {}
        self._first_seen: dict[str, float] = {}

    def _watched(self, relation: Any) -> summary.Signals:  # noqa: ANN401
        """Resolve the watched signals once; the schema does not change between windows."""
        if self._resolved is None:
            if self._signals is not None:  # [] is a choice: dropouts only
                resolved = summary.resolve_signals(relation, self._signals)
            else:
                resolved = summary.numeric_signals(relation)
            if len(resolved) > self._max_signals:
                raise ValueError(
                    f"{len(resolved)} numeric signals exceed max_signals={self._max_signals}. "
                    "Set 'topics' or 'signals' to the ones worth watching, or raise "
                    "max_signals (every signal adds to the request sent to Jev)."
                )
            self._resolved = resolved
        return self._resolved

    def evaluate(self, asof_seconds: float, lookback: base.Lookback | None) -> bool:
        """Implement `base.Gate.evaluate`."""
        if lookback is None or lookback.unit == base.Unit.FRAME or lookback.last <= 0:
            raise ValueError(
                "The anomaly gate needs a positive time-based lookback, e.g. "
                "{last: 10, unit: second}: its baseline is built from windows of data time."
            )
        relation = self.to_duckdb(topics=self._topics, asof_seconds=asof_seconds, lookback=lookback)
        window = summary.summarize(relation, self._watched(relation))
        normal = self.baseline.stats(asof_seconds) if self.baseline.ready(asof_seconds) else None
        reasons = (
            screen.screen(
                window,
                normal,
                asof_seconds,
                self._z_threshold,
                self._dropout_seconds,
                last_seen=self._last_seen,
                first_seen=self._first_seen,
            )
            if normal
            else []
        )
        for topic, stats in window["topics"].items():
            if stats["last_seconds"] is not None:
                self._last_seen[topic] = stats["last_seconds"]
                self._first_seen.setdefault(topic, stats["last_seconds"])
        present = _present_topics(window, self._last_seen, asof_seconds, self._dropout_seconds)
        self._annotations = {}
        if self._mode == "screen" and not reasons:
            self.baseline.add(window, present)
            return False

        state = {"window": window, "baseline": normal, "screen_reasons": reasons}
        try:
            answer = self._backend.decide(state, self._question, self._choices)
        except backends.BackendUnavailable as error:
            logging.warning("Anomaly gate %s: Jev unavailable (%s)", self.name, error)
            if not reasons:
                self.baseline.add(window, present)
                return False
            self._record(SCREEN_ONLY, {}, None, False, window, normal, reasons)
            return True

        total = sum(answer.probabilities.values())
        probabilities = {
            choice: (score / total if total > 0 else 0.0)
            for choice, score in answer.probabilities.items()
        }
        label = max(probabilities, key=probabilities.__getitem__)
        flagged = label != NORMAL and probabilities[label] >= self._min_probability
        if flagged:
            self._record(label, probabilities, answer.model, True, window, normal, reasons)
        elif not reasons or (label == NORMAL and probabilities[NORMAL] >= self._min_probability):
            # Only a confident `normal` verdict teaches the baseline that a screened window
            # was fine; a weak one keeps it out, as an unreachable backend would.
            self.baseline.add(window, present)
        return flagged

    def _record(  # noqa: PLR0913
        self,
        label: str,
        probabilities: dict[str, float],
        model: str | None,
        verified: bool,
        window: dict,
        normal: dict | None,
        reasons: list[dict],
    ) -> None:
        self._annotations = {
            "label": label,
            "probabilities": probabilities,
            "verified": verified,
            "model": model,
            "mode": self._mode,
            "screen_reasons": reasons,
            "window": window["window"],
            "baseline": normal,
            "beta": True,
        }
        logging.info("Anomaly gate %s flagged %s (verified=%s)", self.name, label, verified)

    def annotations(self) -> dict[str, Any]:
        """Implement `base.Gate.annotations`: the label record of the latest pass."""
        return dict(self._annotations)


def register() -> None:
    """Register module for dependency injection."""
    module.global_registry[__name__] = Anomaly
