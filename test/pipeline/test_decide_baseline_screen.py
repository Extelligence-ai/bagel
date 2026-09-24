"""Tests for the rolling baseline and the on-robot screen (`src.pipeline.decide`)."""

import random

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
    assert {r["kind"] for r in reasons} == {"mean_shift", "z_score"}


def test_topic_that_stops_publishing_is_a_dropout() -> None:
    reasons = screen.screen(
        _window(100, [0.0], topic_last=95.0),
        _baseline(),
        asof_seconds=100,
        z_threshold=3.0,
        dropout_seconds=2.0,
    )
    assert reasons == [{"kind": "dropout", "topic": "/m", "silent_seconds": 5.0}]


def test_topic_silent_for_the_whole_window_is_measured_from_its_last_message() -> None:
    reasons = screen.screen(
        _window(100, [], topic_last=None),
        _baseline(),
        asof_seconds=100,
        z_threshold=3.0,
        dropout_seconds=2.0,
        last_seen={"/m": 85.0},
    )
    assert reasons == [{"kind": "dropout", "topic": "/m", "silent_seconds": 15.0}]


def test_an_empty_window_shorter_than_the_threshold_is_not_yet_a_dropout() -> None:
    # 10 s window, 30 s threshold: silence so far is 10 s (Codex P2).
    reasons = screen.screen(
        _window(100, [], topic_last=None),
        _baseline(),
        asof_seconds=100,
        z_threshold=3.0,
        dropout_seconds=30.0,
        last_seen={"/m": 90.0},
    )
    assert reasons == []


def test_a_topic_never_seen_at_all_is_reported_with_unknown_silence() -> None:
    reasons = screen.screen(
        _window(100, [], topic_last=None),
        _baseline(),
        asof_seconds=100,
        z_threshold=3.0,
        dropout_seconds=2.0,
    )
    assert reasons == [{"kind": "dropout", "topic": "/m", "silent_seconds": None}]


def test_a_baseline_with_too_few_samples_is_not_screened_against() -> None:
    # One message in the first window gives std 0; the constant-baseline floor would then
    # call ordinary noise a 50 sigma event.
    thin = {
        "span_seconds": 60.0,
        "signals": {"/m.v": {"count": 1, "mean": 1.0, "std": 0.0}},
        "topics": [],
    }
    reasons = screen.screen(_window(100, [1.05, 0.95], topic_last=100), thin, 100, 3.0, 2.0)
    assert reasons == []


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


# --- false-positive rate and sensitivity (from pre-release review) ------------------------


def _gauss_window(end: float, hz: int, rng: random.Random, shift: float = 0.0) -> dict:
    return _window(end, [rng.gauss(shift, 1.0) for _ in range(10 * hz)], topic_last=end)


def _screened(hz: int, spike_at: int | None = None, shift_at: int | None = None) -> list[int]:
    """Indices of flagged windows over 300 windows of N(0,1) noise at `hz` samples/s."""
    rng = random.Random(0)  # noqa: S311 -- deterministic test data, not crypto
    rolling = baseline.RollingBaseline(window_minutes=30, warmup_minutes=1)
    flagged = []
    for index in range(300):
        end = (index + 1) * 10.0
        window = _gauss_window(end, hz, rng, shift=4.0 if index == shift_at else 0.0)
        if index == spike_at:
            window["signals"]["/m.v"]["max"] = 12.0
        if rolling.ready(end) and screen.screen(window, rolling.stats(end), end, 3.0, 2.0):
            flagged.append(index)
            continue
        rolling.add(window)
    return flagged


@pytest.mark.parametrize("hz", [10, 50, 200])
def test_pure_noise_is_rarely_flagged(hz: int) -> None:
    # The old screen compared the window's most extreme sample against the per-sample
    # std: the max of N normals exceeds 3 std with probability 1 - 0.9973**N, so at
    # 50 Hz most normal windows were flagged and the baseline starved.
    assert len(_screened(hz)) <= 6  # <= 2% of ~294 screened windows


def test_a_sustained_shift_is_flagged_as_a_mean_shift() -> None:
    assert 150 in _screened(hz=50, shift_at=150)


def test_a_single_extreme_sample_is_still_flagged() -> None:
    assert 150 in _screened(hz=50, spike_at=150)


def test_extreme_threshold_grows_with_the_sample_count() -> None:
    # 12 sigma stands out at any rate; 3.5 sigma is an ordinary max among 2000 samples.
    tiny = _window(100, [0.0] * 1999 + [3.5], topic_last=100)
    assert screen.screen(tiny, _baseline(), 100, 3.0, 2.0) == []


def test_mean_shift_reason_names_the_signal() -> None:
    reasons = screen.screen(
        _window(100, [4.0, 4.2, 3.8], topic_last=100), _baseline(), 100, 3.0, 2.0
    )
    assert [(r["kind"], r["signal"]) for r in reasons] == [("mean_shift", "/m.v")]


def test_the_spread_floor_only_applies_to_a_constant_baseline() -> None:
    # Barometric pressure: mean 101325, std 5. A 30 Pa mean shift is 6 sigma and must
    # be flagged; the old floor of 1e-3 * |mean| = 101 Pa hid it.
    reasons = screen.screen(
        _window(100, [101355.0, 101355.0], topic_last=100),
        _baseline(mean=101325.0, std=5.0),
        100,
        3.0,
        2.0,
    )
    assert "mean_shift" in {r["kind"] for r in reasons}


def test_only_topics_seen_in_most_baseline_windows_can_drop_out() -> None:
    # An event-driven topic (/cmd only while driving) must not read as a dropout in
    # every quiet window.
    rolling = baseline.RollingBaseline(window_minutes=10, warmup_minutes=0)
    for end in (10, 20, 30, 40):
        window = _window(end, [1.0], topic_last=end)
        window["topics"]["/cmd"] = {"messages": 1 if end == 10 else 0, "last_seconds": None}
        rolling.add(window)
    assert rolling.stats(40)["topics"] == ["/m"]


def test_time_running_backwards_resets_the_baseline() -> None:
    # `ros2 bag play --loop`, a sim reset, or MQTT data with a rewound timestamp field.
    rolling = baseline.RollingBaseline(window_minutes=30, warmup_minutes=0)
    rolling.add(_window(1000, [1.0]))
    rolling.add(_window(1010, [1.0]))
    rolling.add(_window(20, [5.0]))
    assert rolling.stats(20)["signals"]["/m.v"]["mean"] == 5.0
    assert rolling.stats(20)["span_seconds"] == pytest.approx(10.0)


def test_overlapping_windows_do_not_reset_the_baseline() -> None:
    # A 10 s lookback evaluated every second: each window starts before the previous
    # one ended. That is overlap, not time running backwards (Codex P1).
    rolling = baseline.RollingBaseline(window_minutes=30, warmup_minutes=0)
    for end in (10.0, 11.0, 12.0):
        rolling.add(_window(end, [1.0, 3.0]))
    stats = rolling.stats(asof_seconds=12.0)
    assert stats["signals"]["/m.v"]["count"] == 6
    assert stats["span_seconds"] == pytest.approx(12.0)
