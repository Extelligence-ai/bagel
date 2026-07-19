"""Tests for the live edge-recorder trigger and predicate evaluation."""

import pyarrow as pa
import pytest

from bagel.pipeline import live


def _events(
    stream: list[tuple[float, bool]], forward: float = 0.0, debounce: float = 0.0
) -> tuple[list[float], list[float]]:
    """Feed a stream through a trigger and return (fired_now, all_via_flush)."""
    trigger = live.LiveEventTrigger(forward_seconds=forward, debounce_seconds=debounce)
    fired: list[float] = []
    for timestamp, hit in stream:
        fired.extend(trigger.feed(timestamp, hit))
    return fired, trigger.flush()


def test_fires_on_rising_edge_immediately_with_no_forward_window() -> None:
    stream = [(0.0, False), (1.0, True), (2.0, True), (3.0, False), (4.0, True)]
    fired, pending = _events(stream)
    assert fired == [1.0, 4.0]
    assert pending == []


def test_forward_window_delays_firing_until_post_elapsed() -> None:
    # Event at t=1; with a 3s forward window it fires only once t>=4 arrives.
    stream = [(0.0, False), (1.0, True), (2.0, True), (3.0, False), (4.0, False), (5.0, False)]
    fired, pending = _events(stream, forward=3.0)
    assert fired == [1.0]  # fired at t=4 (1 + 3)
    assert pending == []


def test_forward_window_event_still_pending_at_stream_end_is_flushed() -> None:
    # Event at t=5 with a 10s forward window never reaches t=15 -> flushed at close.
    stream = [(0.0, False), (5.0, True), (6.0, False)]
    fired, pending = _events(stream, forward=10.0)
    assert fired == []
    assert pending == [5.0]


def test_debounce_coalesces_bursty_events() -> None:
    stream = [(0.0, True), (1.0, False), (2.0, True), (10.0, False), (11.0, True)]
    fired, _ = _events(stream, debounce=5.0)
    assert fired == [0.0, 11.0]


def test_negative_windows_rejected() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        live.LiveEventTrigger(forward_seconds=-1.0)


def test_evaluate_predicate_true_and_false() -> None:
    struct = pa.struct([("accel_x", pa.float64()), ("vel", pa.float64())])
    predicate = "imu['accel_x'] < -10"
    assert live.evaluate_predicate("imu", struct, {"accel_x": -13.0, "vel": 4.0}, predicate)
    assert not live.evaluate_predicate("imu", struct, {"accel_x": -2.0, "vel": 4.0}, predicate)


def test_evaluate_predicate_nested_struct_with_slashed_topic() -> None:
    struct = pa.struct([("linear_acceleration", pa.struct([("x", pa.float64())]))])
    message = {"linear_acceleration": {"x": -12.0}}
    predicate = "\"/imu\"['linear_acceleration']['x'] < -10"
    assert live.evaluate_predicate("/imu", struct, message, predicate)
