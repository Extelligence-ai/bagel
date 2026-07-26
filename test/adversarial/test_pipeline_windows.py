"""Property-based tests for window utilities."""

from hypothesis import given, strategies as st

from src.pipeline import windows

_ts = st.floats(min_value=-1e6, max_value=1e6, allow_nan=False, allow_infinity=False)


@given(st.lists(st.tuples(_ts, st.booleans())))
def test_rising_edges_never_exceeds_sample_count(samples: list) -> None:
    edges = windows.rising_edges(samples)
    assert len(edges) <= len(samples)
    assert edges == sorted(edges)


_iv = st.tuples(_ts, _ts).map(lambda t: (min(t), max(t)))


@given(st.lists(_iv))
def test_merge_intervals_are_disjoint_and_sorted(intervals: list) -> None:
    merged = windows.merge_intervals(intervals)
    for (a_start, a_end), (b_start, b_end) in zip(merged, merged[1:]):
        assert a_end <= b_start  # disjoint, ordered


@given(st.lists(_iv))
def test_total_duration_is_nonnegative_and_bounded(intervals: list) -> None:
    merged = windows.merge_intervals(intervals)
    dur = windows.total_duration(merged)
    assert dur >= 0.0
    naive = sum(max(0.0, e - s) for s, e in intervals)
    assert dur <= naive + 1e-6
