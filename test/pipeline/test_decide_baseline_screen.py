"""Tests for the rolling baseline and the on-robot screen (`src.pipeline.decide`)."""

import pytest

from src.pipeline.decide import baseline, screen


def _window(end: float, values: list[float], topic_last: float | None = None) -> dict:
    """A window summary with one signal `/m.v` holding `values`, ending at `end`."""
    count = len(values)
    mean = sum(values) / count if count else None
    std = (sum((v - mean) ** 2 for v in values) / count) ** 0.5 if count else None
    return {
        "window": {"start_seconds": end - 10, "end_seconds": end, "messages": count},
        "signals": {
            "/m.v": {
                "count": count,
                "min": min(values) if values else None,
                "max": max(values) if values else None,
                "mean": mean,
                "std": std,
            }
        },
        "topics": {"/m": {"messages": count, "last_seconds": topic_last}},
    }


# --- RollingBaseline ---------------------------------------------------------------------


def test_combines_windows_into_one_mean_and_std() -> None:
    rolling = baseline.RollingBaseline(window_minutes=10, warmup_minutes=0)
    rolling.add(_window(10, [1.0, 3.0]))
    rolling.add(_window(20, [5.0, 7.0]))
    stats = rolling.stats(asof_seconds=20)["signals"]["/m.v"]
    assert stats["count"] == 4
    assert stats["mean"] == pytest.approx(4.0)
    assert stats["std"] == pytest.approx(5**0.5)  # population std of 1, 3, 5, 7


def test_windows_older_than_the_rolling_span_are_dropped() -> None:
    rolling = baseline.RollingBaseline(window_minutes=1, warmup_minutes=0)
    rolling.add(_window(10, [100.0]))
    rolling.add(_window(80, [2.0]))
    assert rolling.stats(asof_seconds=80)["signals"]["/m.v"]["mean"] == pytest.approx(2.0)


def test_not_ready_until_the_warmup_span_is_covered() -> None:
    rolling = baseline.RollingBaseline(window_minutes=30, warmup_minutes=1)
    assert rolling.ready(asof_seconds=0) is False
    rolling.add(_window(10, [1.0]))  # covers 0..10 s
    assert rolling.ready(asof_seconds=10) is False
    rolling.add(_window(60, [1.0]))  # covers 0..60 s
    assert rolling.ready(asof_seconds=60) is True


def test_reports_topics_that_have_published_and_its_span() -> None:
    rolling = baseline.RollingBaseline(window_minutes=10, warmup_minutes=0)
    rolling.add(_window(10, [1.0], topic_last=9.0))
    stats = rolling.stats(asof_seconds=10)
    assert stats["topics"] == ["/m"]
    assert stats["span_seconds"] == pytest.approx(10.0)


def test_empty_windows_do_not_poison_statistics() -> None:
    rolling = baseline.RollingBaseline(window_minutes=10, warmup_minutes=0)
    rolling.add(_window(10, []))
    rolling.add(_window(20, [2.0, 4.0]))
    stats = rolling.stats(asof_seconds=20)
    assert stats["signals"]["/m.v"]["mean"] == pytest.approx(3.0)


# --- screen ------------------------------------------------------------------------------


def _baseline(mean: float = 0.0, std: float = 1.0) -> dict:
    return {
        "span_seconds": 600.0,
        "signals": {"/m.v": {"count": 100, "mean": mean, "std": std}},
        "topics": ["/m"],
    }


def test_extreme_value_beyond_threshold_is_flagged() -> None:
    reasons = screen.screen(
        _window(100, [0.1, -4.5, 0.2], topic_last=100),
        _baseline(),
        asof_seconds=100,
        z_threshold=3.0,
        dropout_seconds=2.0,
    )
    assert reasons == [{"kind": "z_score", "signal": "/m.v", "value": -4.5, "z": 4.5}]


def test_values_within_threshold_are_not_flagged() -> None:
    reasons = screen.screen(
        _window(100, [0.5, -1.0], topic_last=100),
        _baseline(),
        asof_seconds=100,
        z_threshold=3.0,
        dropout_seconds=2.0,
    )
    assert reasons == []


def test_constant_baseline_still_flags_a_change() -> None:
    reasons = screen.screen(
        _window(100, [5.0, 9.0], topic_last=100),
        _baseline(mean=5.0, std=0.0),
        asof_seconds=100,
        z_threshold=3.0,
        dropout_seconds=2.0,
    )
    assert [r["kind"] for r in reasons] == ["z_score"]


def test_topic_that_stops_publishing_is_a_dropout() -> None:
    reasons = screen.screen(
        _window(100, [0.0], topic_last=95.0),
        _baseline(),
        asof_seconds=100,
        z_threshold=3.0,
        dropout_seconds=2.0,
    )
    assert reasons == [{"kind": "dropout", "topic": "/m", "silent_seconds": 5.0}]


def test_topic_silent_for_the_whole_window_is_a_dropout() -> None:
    reasons = screen.screen(
        _window(100, [], topic_last=None),
        _baseline(),
        asof_seconds=100,
        z_threshold=3.0,
        dropout_seconds=2.0,
    )
    assert reasons == [{"kind": "dropout", "topic": "/m", "silent_seconds": None}]


def test_signals_unknown_to_the_baseline_are_ignored() -> None:
    empty = {"span_seconds": 600.0, "signals": {}, "topics": []}
    reasons = screen.screen(
        _window(100, [1e9], topic_last=None),
        empty,
        asof_seconds=100,
        z_threshold=3.0,
        dropout_seconds=2.0,
    )
    assert reasons == []
