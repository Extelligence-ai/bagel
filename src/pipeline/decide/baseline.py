"""What "normal" looks like for a robot, as per-signal mean/std over recent windows."""

import collections
import logging
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
        """Return ``{"span_seconds", "signals": {label: {count, mean, std}}, "topics"}``.

        `topics` lists the topics expected to publish in every window.
        """
        ...


class RollingBaseline:
    """Statistics over the last `window_minutes` of normal windows.

    Callers add only windows they did not flag, and age is measured from the newest
    normal window, so an ongoing fault -- however long -- never becomes the new normal.
    """

    def __init__(self, window_minutes: float, warmup_minutes: float) -> None:
        """Initialize an empty baseline."""
        self._span_seconds = window_minutes * 60
        self._warmup_seconds = warmup_minutes * 60
        # (start_seconds, end_seconds, {label: (count, sum, sum_sq)}, {topics that published})
        self._windows: collections.deque[
            tuple[float, float, dict[str, tuple[float, float, float]], set[str]]
        ] = collections.deque()

    def add(self, window: dict) -> None:
        """Implement `Baseline.add`."""
        bounds = window["window"]
        if bounds["end_seconds"] is None:
            return
        if self._windows and bounds["end_seconds"] < self._windows[-1][1]:
            # Data time went backwards (a looped bag, a sim reset, a rewound timestamp
            # field): the old windows would never prune, so start over. Windows that
            # merely overlap (a cadence shorter than the lookback) end later each time
            # and are fine.
            logging.warning("Baseline reset: window ends before the previous one did")
            self._windows.clear()
        # Overlapping windows (a cadence shorter than the lookback) would count each
        # sample once per window; weight a window by the share of its span that is new.
        share = 1.0
        if self._windows and bounds["end_seconds"] > bounds["start_seconds"]:
            previous_end = self._windows[-1][1]
            span = bounds["end_seconds"] - bounds["start_seconds"]
            share = min(1.0, max(0.0, (bounds["end_seconds"] - previous_end) / span))
        if share == 0.0:
            return
        moments = {}
        for label, signal in window["signals"].items():
            count = signal["count"] * share
            if not count:
                continue
            mean, std = signal["mean"], signal["std"] or 0.0
            moments[label] = (count, mean * count, (std**2 + mean**2) * count)
        topics = {topic for topic, stats in window["topics"].items() if stats["messages"]}
        self._windows.append((bounds["start_seconds"], bounds["end_seconds"], moments, topics))
        self._prune()

    def _prune(self) -> None:
        # Age is measured from the newest normal window, not from the clock: during a
        # fault nothing normal arrives, and the known-good history must survive it
        # rather than be pruned until the fault reads as the new normal.
        if not self._windows:
            return
        newest_end = self._windows[-1][1]
        while self._windows and self._windows[0][1] < newest_end - self._span_seconds:
            self._windows.popleft()

    def _covered_seconds(self, asof_seconds: float) -> float:
        return asof_seconds - self._windows[0][0] if self._windows else 0.0

    def ready(self, asof_seconds: float) -> bool:
        """Implement `Baseline.ready`."""
        return bool(self._windows) and self._covered_seconds(asof_seconds) >= self._warmup_seconds

    def stats(self, asof_seconds: float) -> dict:
        """Implement `Baseline.stats`."""
        totals: dict[str, list[float]] = {}
        seen: collections.Counter[str] = collections.Counter()
        for _, _, moments, window_topics in self._windows:
            seen.update(window_topics)
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
        # Only topics that publish in most windows are expected to keep publishing;
        # event-driven ones (/cmd_vel while driving) would otherwise read as dropouts.
        expected = [topic for topic, count in seen.items() if 2 * count > len(self._windows)]
        return {
            "span_seconds": self._covered_seconds(asof_seconds),
            "signals": signals,
            "topics": sorted(expected),
        }
