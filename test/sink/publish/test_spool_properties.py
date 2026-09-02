"""Spool invariants under arbitrary operation sequences (spec §8 property tests)."""

import json

import pytest
from hypothesis import given
from hypothesis import settings as hyp_settings
from hypothesis import strategies as st

from src.sink.publish import spool as spool_mod
from src.sink.publish.spool import Spool


@given(
    n_records=st.integers(min_value=1, max_value=60),
    ack_at=st.integers(min_value=0, max_value=60),
    restart_at=st.integers(min_value=0, max_value=60),
)
@hyp_settings(max_examples=50, deadline=None)
def test_replay_is_exactly_the_unacked_suffix(
    tmp_path_factory: pytest.TempPathFactory,
    n_records: int,
    ack_at: int,
    restart_at: int,
) -> None:
    root = tmp_path_factory.mktemp("spool")
    s = Spool(root)
    for i in range(1, n_records + 1):
        s.append("events", i, {"n": i})
        if i == restart_at:
            s = Spool(root)  # simulated restart mid-stream
    effective_ack = min(ack_at, n_records)
    if effective_ack:
        s.ack("events", effective_ack)
    s = Spool(root)  # restart after ack
    replayed = [seq for seq, _ in s.pending("events")]
    assert replayed == list(range(effective_ack + 1, n_records + 1))
    assert s.next_seq("events") == n_records + 1


@given(
    record_pad=st.integers(min_value=10, max_value=200),
    n_records=st.integers(min_value=5, max_value=80),
    cap=st.integers(min_value=200, max_value=1200),
)
@hyp_settings(max_examples=40, deadline=None)
def test_capped_lane_bounded_and_ordered(
    tmp_path_factory: pytest.TempPathFactory,
    record_pad: int,
    n_records: int,
    cap: int,
) -> None:
    root = tmp_path_factory.mktemp("spool")
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(spool_mod, "SEGMENT_MAX_BYTES", 128)
        s = Spool(root, capped_lanes={"channels": cap})
        for i in range(1, n_records + 1):
            s.append("channels", i, {"pad": "x" * record_pad, "n": i})
        lane_bytes = sum(p.stat().st_size for p in (root / "channels").glob("*.jsonl"))
        assert lane_bytes <= cap + spool_mod.SEGMENT_MAX_BYTES
        pending = [seq for seq, _ in s.pending("channels")]
        assert pending == sorted(pending)
        assert pending[-1] == n_records  # newest always survives
        # No parse damage anywhere.
        for seg in (root / "channels").glob("*.jsonl"):
            for line in seg.read_text().splitlines():
                json.loads(line)
