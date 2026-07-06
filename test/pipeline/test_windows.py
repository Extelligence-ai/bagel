"""Tests for the event-window reduction utilities."""

import pytest

from src.pipeline import windows


def test_rising_edges_fires_once_per_sustained_event() -> None:
    samples = [(0.0, False), (1.0, True), (2.0, True), (3.0, False), (4.0, True)]
    assert windows.rising_edges(samples) == [1.0, 4.0]


def test_rising_edges_sorts_unordered_input() -> None:
    samples = [(4.0, True), (0.0, False), (2.0, True), (1.0, False)]
    assert windows.rising_edges(samples) == [2.0]


def test_rising_edges_coerces_none_to_false() -> None:
    samples = [(0.0, None), (1.0, True), (2.0, None), (3.0, True)]
    assert windows.rising_edges(samples) == [1.0, 3.0]


def test_rising_edges_debounce_coalesces() -> None:
    samples = [(0.0, True), (1.0, False), (2.0, True), (10.0, False), (11.0, True)]
    # Edges at 0, 2, 11. With a 5s gap the edge at 2 is dropped (within 5s of 0).
    assert windows.rising_edges(samples, min_gap_seconds=5.0) == [0.0, 11.0]


def test_event_windows_builds_symmetric_intervals() -> None:
    assert windows.event_windows([100.0], pre_seconds=10, post_seconds=5) == [(90.0, 105.0)]


def test_event_windows_rejects_negative() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        windows.event_windows([1.0], pre_seconds=-1, post_seconds=0)


def test_merge_intervals_unions_overlaps() -> None:
    assert windows.merge_intervals([(0.0, 10.0), (5.0, 15.0), (20.0, 25.0)]) == [
        (0.0, 15.0),
        (20.0, 25.0),
    ]


def test_merge_intervals_merges_touching() -> None:
    assert windows.merge_intervals([(0.0, 10.0), (10.0, 20.0)]) == [(0.0, 20.0)]


def test_merge_intervals_drops_invalid_and_sorts() -> None:
    assert windows.merge_intervals([(30.0, 40.0), (5.0, 3.0), (0.0, 10.0)]) == [
        (0.0, 10.0),
        (30.0, 40.0),
    ]


def test_merge_intervals_empty() -> None:
    assert windows.merge_intervals([]) == []


def test_total_duration() -> None:
    assert windows.total_duration([(0.0, 10.0), (20.0, 25.0)]) == 15.0


def test_plan_reduction_end_to_end() -> None:
    # Two decel events at t=2 and t=7; +/-1.5s windows do not overlap.
    samples = [
        (0.0, False),
        (1.0, False),
        (2.0, True),
        (3.0, True),
        (4.0, False),
        (7.0, True),
        (8.0, False),
    ]
    plan = windows.plan_reduction(samples, pre_seconds=1.5, post_seconds=1.5, span_seconds=8.0)
    assert plan["events"] == [2.0, 7.0]
    # event 2 -> [0.5, 3.5], event 7 -> [5.5, 8.5]; disjoint, so both kept.
    assert plan["intervals"] == [(0.5, 3.5), (5.5, 8.5)]
    assert plan["kept_seconds"] == 6.0
    assert plan["kept_fraction"] == 0.75


def test_plan_reduction_overlapping_windows_merge() -> None:
    # Events at t=2 and t=3 with +/-2s windows overlap -> a single kept interval.
    samples = [(0.0, False), (2.0, True), (2.5, False), (3.0, True), (4.0, False)]
    plan = windows.plan_reduction(samples, pre_seconds=2, post_seconds=2, span_seconds=100.0)
    assert plan["events"] == [2.0, 3.0]
    assert plan["intervals"] == [(0.0, 5.0)]
    assert plan["kept_seconds"] == 5.0
    assert plan["kept_fraction"] == pytest.approx(0.05)


def test_plan_reduction_zero_span() -> None:
    plan = windows.plan_reduction([(0.0, True)], pre_seconds=1, post_seconds=1, span_seconds=0.0)
    assert plan["kept_fraction"] == 0.0
