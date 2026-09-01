"""Disk spool: segments, watermark, lanes (fleet streaming spec §4)."""

import json
import pathlib

import pytest

from src.sink.publish import spool as spool_mod
from src.sink.publish.spool import Spool


@pytest.fixture()
def root(tmp_path: pathlib.Path) -> pathlib.Path:
    return tmp_path / "publish" / "robot-1"


class TestAppendAndReplay:
    def test_seq_allocation_starts_at_one_and_is_monotonic(self, root: pathlib.Path) -> None:
        s = Spool(root)
        assert s.next_seq("channels") == 1
        s.append("channels", 1, {"v": 1})
        assert s.next_seq("channels") == 2

    def test_append_rejects_non_monotonic_seq(self, root: pathlib.Path) -> None:
        s = Spool(root)
        s.append("channels", 1, {"v": 1})
        with pytest.raises(ValueError, match="monotonic"):
            s.append("channels", 1, {"v": 1})

    def test_pending_replays_in_order(self, root: pathlib.Path) -> None:
        s = Spool(root)
        for i in range(1, 6):
            s.append("channels", i, {"n": i})
        assert [(seq, p["n"]) for seq, p in s.pending("channels")] == [(i, i) for i in range(1, 6)]

    def test_lanes_are_independent(self, root: pathlib.Path) -> None:
        s = Spool(root)
        s.append("channels", 1, {"c": 1})
        s.append("events", 1, {"e": 1})
        assert [p for _, p in s.pending("events")] == [{"e": 1}]

    def test_restart_resumes_seq_and_replay(self, root: pathlib.Path) -> None:
        s = Spool(root)
        for i in range(1, 4):
            s.append("channels", i, {"n": i})
        del s
        s2 = Spool(root)
        assert s2.next_seq("channels") == 4
        assert [seq for seq, _ in s2.pending("channels")] == [1, 2, 3]


class TestSegments:
    def test_segments_roll_at_size_cap(
        self, root: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(spool_mod, "SEGMENT_MAX_BYTES", 200)
        s = Spool(root)
        big = {"pad": "x" * 80}
        for i in range(1, 7):
            s.append("channels", i, big)
        segments = sorted((root / "channels").glob("segment-*.jsonl"))
        assert len(segments) >= 3
        # Named by first seq: the first segment starts at 1.
        assert segments[0].name == f"segment-{1:016d}.jsonl"
        # Every line in a segment parses and seqs ascend across segment order.
        seqs = []
        for seg in segments:
            for line in seg.read_text().splitlines():
                seqs.append(json.loads(line)["seq"])
        assert seqs == sorted(seqs) == list(range(1, 7))

    def test_active_segment_appends_dont_rewrite(self, root: pathlib.Path) -> None:
        s = Spool(root)
        s.append("channels", 1, {"n": 1})
        seg = next((root / "channels").glob("segment-*.jsonl"))
        size_before = seg.stat().st_size
        s.append("channels", 2, {"n": 2})
        assert seg.stat().st_size > size_before  # appended, not rewritten


class TestCrashTolerance:
    def test_truncated_final_line_skipped_on_restart(self, root: pathlib.Path) -> None:
        """Crash-truncated final line is skipped; restart recovers preceding records."""
        s = Spool(root)
        s.append("channels", 1, {"n": 1})
        s.append("channels", 2, {"n": 2})
        s.append("channels", 3, {"n": 3})
        del s

        # Truncate the final line of the segment mid-record (simulate crash).
        seg = next((root / "channels").glob("segment-*.jsonl"))
        text = seg.read_text()
        # Remove last 15 characters to tear the final JSON record.
        seg.write_text(text[:-15])

        # Restart should not crash; next_seq skips torn line and resumes from seq 3.
        s2 = Spool(root)
        assert s2.next_seq("channels") == 3
        # Pending yields only intact records, skipping the torn one.
        assert [seq for seq, _ in s2.pending("channels")] == [1, 2]

    def test_corrupted_middle_line_raises(self, root: pathlib.Path) -> None:
        """Corruption in a middle line (real data corruption) raises on parse."""
        s = Spool(root)
        s.append("channels", 1, {"n": 1})
        s.append("channels", 2, {"n": 2})
        s.append("channels", 3, {"n": 3})
        del s

        # Corrupt a middle line by replacing it with garbage.
        seg = next((root / "channels").glob("segment-*.jsonl"))
        lines = seg.read_text().splitlines()
        lines[1] = "corrupted garbage line"  # Corrupt the second line.
        seg.write_text("\n".join(lines) + "\n")

        # Restart fails because pending encounters corrupted line in the middle.
        s2 = Spool(root)
        with pytest.raises(json.JSONDecodeError):
            list(s2.pending("channels"))
