"""What "normal" looks like for a robot, as per-signal mean/std over recent windows."""

import collections
import math
from typing import Protocol


class Baseline(Protocol):
    """A source of normal per-signal statistics.

    `RollingBaseline` learns on the robot. Reference-log and fleet-pushed baselines
    (Matcha roadmap) implement the same three methods.
    """

    def add(self, window: dict) -> None:
        """Learn from a window summary judged normal."""
        ...

    def ready(self, asof_seconds: float) -> bool:
        """Whether there is enough history to screen against."""
        ...

    def stats(self, asof_seconds: float) -> dict:
        """Return ``{"span_seconds", "signals": {label: {count, mean, std}}, "topics"}``."""
        ...


class RollingBaseline:
    """Statistics over the windows added during the last `window_minutes` of data time.

    Callers add only windows they did not flag, so an ongoing fault does not become
    the new normal.
    """

    def __init__(self, window_minutes: float, warmup_minutes: float) -> None:
        """Initialize an empty baseline."""
        self._span_seconds = window_minutes * 60
        self._warmup_seconds = warmup_minutes * 60
        # (start_seconds, end_seconds, {label: (count, sum, sum_sq)}, {topics that published})
        self._windows: collections.deque[
            tuple[float, float, dict[str, tuple[int, float, float]], set[str]]
        ] = collections.deque()

    def add(self, window: dict) -> None:
        """Implement `Baseline.add`."""
        bounds = window["window"]
        if bounds["end_seconds"] is None:
            return
        moments = {}
        for label, signal in window["signals"].items():
            count = signal["count"]
            if not count:
                continue
            mean, std = signal["mean"], signal["std"] or 0.0
            moments[label] = (count, mean * count, (std**2 + mean**2) * count)
        topics = {topic for topic, stats in window["topics"].items() if stats["messages"]}
        self._windows.append((bounds["start_seconds"], bounds["end_seconds"], moments, topics))

    def _prune(self, asof_seconds: float) -> None:
        while self._windows and self._windows[0][1] < asof_seconds - self._span_seconds:
            self._windows.popleft()

    def _covered_seconds(self, asof_seconds: float) -> float:
        return asof_seconds - self._windows[0][0] if self._windows else 0.0

    def ready(self, asof_seconds: float) -> bool:
        """Implement `Baseline.ready`."""
        self._prune(asof_seconds)
        return bool(self._windows) and self._covered_seconds(asof_seconds) >= self._warmup_seconds

    def stats(self, asof_seconds: float) -> dict:
        """Implement `Baseline.stats`."""
        self._prune(asof_seconds)
        totals: dict[str, list[float]] = {}
        topics: set[str] = set()
        for _, _, moments, window_topics in self._windows:
            topics |= window_topics
            for label, (count, total, total_sq) in moments.items():
                running = totals.setdefault(label, [0.0, 0.0, 0.0])
                running[0] += count
                running[1] += total
                running[2] += total_sq
        signals = {}
        for label, (samples, total, total_sq) in totals.items():
            mean = total / samples
            signals[label] = {
                "count": int(samples),
                "mean": mean,
                "std": math.sqrt(max(total_sq / samples - mean**2, 0.0)),
            }
        return {
            "span_seconds": self._covered_seconds(asof_seconds),
            "signals": signals,
            "topics": sorted(topics),
        }
