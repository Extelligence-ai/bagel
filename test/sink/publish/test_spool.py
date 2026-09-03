"""Disk spool: segments, watermark, lanes (fleet streaming spec §4)."""

import json
import pathlib
import threading
import time

import pytest

from src.sink.publish import spool as spool_mod
from src.sink.publish.spool import Spool, SpoolCorruptError, SpoolLockedError


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
        with pytest.raises(SpoolCorruptError, match="channels") as exc_info:
            list(s2.pending("channels"))
        # Typed contract: message names the lane and the segment file, and chains
        # the underlying parse failure.
        assert seg.name in str(exc_info.value)
        assert isinstance(exc_info.value.__cause__, json.JSONDecodeError)


class TestRollBoundaryCrashRecovery:
    """Final-review Critical 1: a crash exactly at a segment roll must not reset seq.

    With SEGMENT_MAX_BYTES forced to 32 -- the exact byte length of one of
    this test's `{"seq": i, "payload": {"n": i}}` JSONL lines, chosen so a
    single record is never itself "oversized" (Codex review: append() now
    drops a record whose own size alone exceeds SEGMENT_MAX_BYTES, which a
    cap of 1 would trip on every record here) -- every append still rolls
    into its own segment (one record's size plus the next exceeds the cap),
    so each segment holds exactly one record and its name's first_seq equals
    that record's seq. This makes "the last segment lost its only record"
    deterministic to set up: whatever is left in segments[:-1] proves seqs up
    to first_seq_of(segments[-1]) - 1 were durably written, even though the
    last segment itself now looks empty (or wholly torn).
    """

    def test_empty_last_segment_floors_seq_at_segment_name_not_watermark(
        self, root: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(spool_mod, "SEGMENT_MAX_BYTES", 32)
        s = Spool(root)
        for i in range(1, 4):
            s.append("channels", i, {"n": i})
        segments = sorted((root / "channels").glob("segment-*.jsonl"))
        assert len(segments) == 3  # one record per segment, as designed above
        last_first_seq = spool_mod._first_seq_of(segments[-1])
        del s

        # Crash landed after the roll created the file but before any bytes landed.
        segments[-1].write_bytes(b"")

        s2 = Spool(root)
        assert s2.next_seq("channels") == last_first_seq  # floor, not a reset to 1
        s2.append("channels", last_first_seq, {"n": last_first_seq})
        assert [seq for seq, _ in s2.pending("channels")] == list(range(1, last_first_seq + 1))

    def test_torn_only_line_of_last_segment_floors_seq_at_segment_name_not_watermark(
        self, root: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(spool_mod, "SEGMENT_MAX_BYTES", 32)
        s = Spool(root)
        for i in range(1, 4):
            s.append("channels", i, {"n": i})
        segments = sorted((root / "channels").glob("segment-*.jsonl"))
        assert len(segments) == 3
        last_first_seq = spool_mod._first_seq_of(segments[-1])
        del s

        # Tear the segment's only line mid-record (simulate a crash mid-write).
        text = segments[-1].read_text()
        assert text
        segments[-1].write_text(text[:-5])

        s2 = Spool(root)
        assert s2.next_seq("channels") == last_first_seq  # floor from the segment name
        s2.append("channels", last_first_seq, {"n": last_first_seq})
        assert [seq for seq, _ in s2.pending("channels")] == list(range(1, last_first_seq + 1))


class TestTornTailSealedOnRestart:
    """Final-review Critical 2: a torn tail must be repaired before the next append,
    or the next append grafts onto the partial line and corrupts the segment.
    """

    def test_torn_tail_then_append_yields_prefix_plus_new_record_no_graft(
        self, root: pathlib.Path
    ) -> None:
        s = Spool(root)
        s.append("channels", 1, {"n": 1})
        s.append("channels", 2, {"n": 2})
        s.append("channels", 3, {"n": 3})
        del s

        seg = next((root / "channels").glob("segment-*.jsonl"))
        text = seg.read_text()
        seg.write_text(text[:-15])  # tear the final record mid-write

        s2 = Spool(root)
        # The torn tail is sealed (truncated away) before this first append lands,
        # so the new record starts on a clean line boundary.
        s2.append("channels", 3, {"n": 3, "resent": True})
        assert [seq for seq, _ in s2.pending("channels")] == [1, 2, 3]

        # A second append must still parse cleanly (no garbage mid-file to trip on).
        s2.append("channels", 4, {"n": 4})
        assert [seq for seq, _ in s2.pending("channels")] == [1, 2, 3, 4]

        # Nothing grafted: every line in the segment is valid, standalone JSON.
        for line in seg.read_text().splitlines():
            json.loads(line)

    def test_never_drop_lane_torn_tail_repaired_on_restart(self, root: pathlib.Path) -> None:
        """Same repair, exercised explicitly on a never-drop lane ("events")."""
        s = Spool(root)
        s.append("events", 1, {"n": 1})
        s.append("events", 2, {"n": 2})
        s.append("events", 3, {"n": 3})
        del s

        seg = next((root / "events").glob("segment-*.jsonl"))
        text = seg.read_text()
        seg.write_text(text[:-15])

        s2 = Spool(root)
        s2.append("events", 3, {"n": 3, "resent": True})
        assert [seq for seq, _ in s2.pending("events")] == [1, 2, 3]
        s2.append("events", 4, {"n": 4})
        assert [seq for seq, _ in s2.pending("events")] == [1, 2, 3, 4]
        for line in seg.read_text().splitlines():
            json.loads(line)


class TestOversizedRecord:
    def test_oversized_record_is_dropped_not_written(
        self, root: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Codex review (3905367602): _evict() only unlinks while
        # len(segments) > 1, so a single record whose own serialized size
        # exceeds SEGMENT_MAX_BYTES becomes its own sole/newest segment and
        # can never be evicted -- unboundedly blowing past a capped lane's
        # byte cap. Ruling: drop-oldest semantics extend to drop-oversized --
        # append() must refuse to write a record that alone exceeds the cap.
        monkeypatch.setattr(spool_mod, "SEGMENT_MAX_BYTES", 200)
        s = Spool(root)
        s.append("channels", 1, {"n": 1})  # a normal record first

        giant = {"pad": "x" * 1000}
        s.append("channels", 2, giant)  # oversized: must be dropped, not written

        assert [seq for seq, _ in s.pending("channels")] == [1]  # seq 2 never landed
        assert s.stats()["channels"].evicted == 1

        # The seq is still consumed (not reissued) and normal appends continue.
        assert s.next_seq("channels") == 3
        s.append("channels", 3, {"n": 3})
        assert [seq for seq, _ in s.pending("channels")] == [1, 3]

    def test_oversized_record_logs_one_warning(
        self, root: pathlib.Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        import logging

        monkeypatch.setattr(spool_mod, "SEGMENT_MAX_BYTES", 200)
        s = Spool(root)
        giant = {"pad": "x" * 1000}
        with caplog.at_level(logging.WARNING):
            s.append("channels", 1, giant)
        warnings_ = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings_) == 1

    def test_oversized_record_on_heartbeat_lane_is_still_written(
        self, root: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Ruling exception: heartbeat is the never-drop lane -- even an
        # (in-practice-unreachable) oversized heartbeat payload must still
        # be written, not dropped.
        monkeypatch.setattr(spool_mod, "SEGMENT_MAX_BYTES", 200)
        s = Spool(root)
        giant = {"pad": "x" * 1000}
        s.append("heartbeat", 1, giant)
        assert [seq for seq, _ in s.pending("heartbeat")] == [1]
        assert s.stats()["heartbeat"].evicted == 0

    def test_oversized_drop_on_a_never_touched_lane_survives_a_rescan(
        self, root: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Codex round 3 follow-up (PR #214, P2, comment 3925663134): an
        oversized-drop consumes its seq in the cache ONLY -- no segment is
        ever written for a dropped record, so when a lane's FIRST-EVER
        record is oversized, disk shows nothing for it at all (no
        directory). A later call on the SAME (now warm-cached) instance
        that forces a rescan (`_current_last_seq`'s reseal branch, hit
        because the lane has no segments -- a `None` tail) must not
        REGRESS the cache back down to the watermark (0): the
        already-consumed seq must survive, or the next oversized record
        would reuse it instead of advancing past it."""
        monkeypatch.setattr(spool_mod, "SEGMENT_MAX_BYTES", 200)
        s = Spool(root)
        giant = {"pad": "x" * 1000}

        first_seq = s.append_next("channels", lambda seq: giant)  # oversized: dropped, no write
        assert first_seq == 1
        assert list((root / "channels").glob("segment-*.jsonl")) == []  # nothing on disk
        assert s.stats()["channels"].evicted == 1

        # Force the rescan path: stats()'s last_seq (itself disk-
        # authoritative, Codex round 3 P2 comment 3925391258) routes
        # through _current_last_seq, whose reseal branch is exactly what
        # regressed the cache before this fix.
        assert s.stats()["channels"].last_seq == 1  # must NOT regress to 0

        second_seq = s.append_next("channels", lambda seq: giant)  # another oversized drop
        assert second_seq == 2  # consumes seq 2 -- never reuses the already-consumed 1
        assert s.stats()["channels"].evicted == 2


class TestPendingToleratesConcurrentEviction:
    def test_segment_evicted_mid_iteration_is_skipped_not_raised(
        self, root: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Codex review (3909414307): pending()'s segment list is a
        # point-in-time snapshot; a concurrent _evict() unlinking one of the
        # captured paths mid-iteration used to raise an uncaught
        # FileNotFoundError, aborting replay entirely instead of tolerating
        # the race (skip the evicted segment, keep going).
        monkeypatch.setattr(spool_mod, "SEGMENT_MAX_BYTES", 40)
        s = Spool(root)
        for i in range(1, 4):
            s.append("channels", i, {"n": i})
        segments = sorted((root / "channels").glob("segment-*.jsonl"))
        assert len(segments) == 3  # one record per (small-capped) segment

        real_scan_segment = spool_mod._scan_segment
        call_count = {"n": 0}

        def scan_and_evict_first(segment: pathlib.Path, **kwargs: object) -> object:
            call_count["n"] += 1
            if call_count["n"] == 1:
                segments[1].unlink()  # simulate a concurrent _evict() mid-iteration
            return real_scan_segment(segment, **kwargs)

        monkeypatch.setattr(spool_mod, "_scan_segment", scan_and_evict_first)

        result = list(s.pending("channels"))  # must not raise

        seqs = [seq for seq, _ in result]
        assert seqs == [1, 3]  # the evicted segment's record (seq 2) is skipped


class TestReadPathPurity:
    def test_pending_and_stats_do_not_create_dirs_for_unknown_lane(
        self, root: pathlib.Path
    ) -> None:
        s = Spool(root)
        assert list(s.pending("ghost")) == []
        assert not (root / "ghost").exists()

        stats = s.stats()
        assert "ghost" not in stats
        assert not (root / "ghost").exists()

        s.ack("ghost", 5)  # acking an unwritten lane must not conjure a dir either
        assert not (root / "ghost").exists()


class TestAckAndWatermark:
    def test_ack_prunes_fully_acked_segments(
        self, root: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(spool_mod, "SEGMENT_MAX_BYTES", 120)
        s = Spool(root)
        for i in range(1, 10):
            s.append("channels", i, {"pad": "x" * 40, "n": i})
        segments_before = len(list((root / "channels").glob("segment-*.jsonl")))
        assert segments_before >= 3
        s.ack("channels", 8)
        remaining = sorted((root / "channels").glob("segment-*.jsonl"))
        assert len(remaining) < segments_before
        assert [seq for seq, _ in s.pending("channels")] == [9]

    def test_ack_is_idempotent_and_ignores_stale(self, root: pathlib.Path) -> None:
        s = Spool(root)
        for i in range(1, 4):
            s.append("channels", i, {"n": i})
        s.ack("channels", 2)
        s.ack("channels", 2)
        s.ack("channels", 1)  # stale: no-op
        assert [seq for seq, _ in s.pending("channels")] == [3]

    def test_idempotent_reack_still_prunes_after_crash_between_watermark_and_prune(
        self, root: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Codex review (3905367589): ack()'s idempotent early-return
        # (seq <= watermark) used to skip retrying _prune() entirely. If the
        # process died after _write_watermarks() committed but before
        # _prune() ran, the fully-acked segments were left un-pruned
        # forever -- a repeated (idempotent) ack for the same seq never
        # cleaned them up. Simulate that crash directly: write the
        # watermark, but never call _prune, then re-ack the same seq.
        monkeypatch.setattr(spool_mod, "SEGMENT_MAX_BYTES", 120)
        s = Spool(root)
        for i in range(1, 10):
            s.append("channels", i, {"pad": "x" * 40, "n": i})
        segments_before = len(list((root / "channels").glob("segment-*.jsonl")))
        assert segments_before >= 3

        # Simulate "crashed after watermark write, before prune": write the
        # watermark directly, bypassing ack()'s own _prune call.
        s._write_watermarks({"channels": 8})
        assert len(list((root / "channels").glob("segment-*.jsonl"))) == segments_before

        # A re-ack for the SAME (already-watermarked) seq is the idempotent
        # early-return path -- it must still prune against the persisted
        # watermark.
        s.ack("channels", 8)

        remaining = sorted((root / "channels").glob("segment-*.jsonl"))
        assert len(remaining) < segments_before
        assert [seq for seq, _ in s.pending("channels")] == [9]

    def test_outage_backlog_survives_early_ack_and_idempotent_reack(
        self, root: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Chaos-e2e regression suspect, pinned as an invariant (2026-09-01
        # CI failure "delivered [1, 38, 39]"): one batch acked pre-outage,
        # then a backlog spooled while the broker is down. pending() must
        # return the ENTIRE backlog in order, and an idempotent re-ack of
        # the pre-outage seq (which now retries _prune) must never unlink a
        # segment still holding unacked records. Multi-segment on purpose:
        # _prune's only delete path is non-final segments, so the
        # single-segment case can't even exercise the suspected bug.
        monkeypatch.setattr(spool_mod, "SEGMENT_MAX_BYTES", 120)
        s = Spool(root)
        s.append("channels", 1, {"pad": "x" * 40, "n": 1})
        s.ack("channels", 1)
        for i in range(2, 40):
            s.append("channels", i, {"pad": "x" * 40, "n": i})
        assert len(list((root / "channels").glob("segment-*.jsonl"))) >= 3
        backlog = list(range(2, 40))
        assert [seq for seq, _ in s.pending("channels")] == backlog
        s.ack("channels", 1)  # idempotent re-ack: prune retry must spare live segments
        assert [seq for seq, _ in s.pending("channels")] == backlog

    def test_watermark_survives_restart_exactly(self, root: pathlib.Path) -> None:
        s = Spool(root)
        for i in range(1, 6):
            s.append("channels", i, {"n": i})
        s.ack("channels", 3)
        del s
        s2 = Spool(root)
        assert [seq for seq, _ in s2.pending("channels")] == [4, 5]
        assert s2.next_seq("channels") == 6

    def test_watermark_file_is_valid_json_after_ack(self, root: pathlib.Path) -> None:
        s = Spool(root)
        s.append("channels", 1, {"n": 1})
        s.ack("channels", 1)
        data = json.loads((root / "watermark.json").read_text())
        assert data == {"channels": 1}


class TestEvictionAndCaps:
    def test_capped_lane_evicts_oldest_segments(
        self, root: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(spool_mod, "SEGMENT_MAX_BYTES", 120)
        s = Spool(root, capped_lanes={"channels": 300})
        for i in range(1, 30):
            s.append("channels", i, {"pad": "x" * 40, "n": i})
        lane_bytes = sum(p.stat().st_size for p in (root / "channels").glob("*.jsonl"))
        assert lane_bytes <= 300 + 120  # cap + one-segment slack
        pending = [seq for seq, _ in s.pending("channels")]
        assert pending == sorted(pending)
        assert pending[-1] == 29  # newest survives
        assert s.stats()["channels"].evicted > 0

    def test_never_drop_lane_ignores_caps_and_grows(
        self, root: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(spool_mod, "SEGMENT_MAX_BYTES", 120)
        s = Spool(root, capped_lanes={"channels": 300})
        for i in range(1, 30):
            s.append("events", i, {"pad": "x" * 40, "n": i})
        assert len([seq for seq, _ in s.pending("events")]) == 29

    def test_write_failure_on_never_drop_lane_raises_spool_full(
        self, root: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        s = Spool(root)
        s.append("events", 1, {"n": 1})

        real_open = open

        def boom(file: object, mode: str = "r", *a: object, **k: object) -> object:
            # A real "disk full" only breaks writes -- reads (including the
            # disk-authoritative tail peek append() now does on every call,
            # Codex round 3 follow-up) keep working. Only fail append/write
            # modes, so this stays a faithful simulation.
            if "a" in mode or "w" in mode:
                raise OSError(28, "No space left on device")
            return real_open(file, mode, *a, **k)

        monkeypatch.setattr("builtins.open", boom)
        with pytest.raises(spool_mod.SpoolFullError, match="events"):
            s.append("events", 2, {"n": 2})


class TestStats:
    def test_stats_reports_bytes_pending_and_seqs(self, root: pathlib.Path) -> None:
        s = Spool(root)
        for i in range(1, 5):
            s.append("channels", i, {"n": i})
        s.ack("channels", 1)
        st = s.stats()["channels"]
        assert st.pending == 3
        assert st.last_seq == 4
        assert st.acked_seq == 1
        assert st.bytes > 0

    def test_stats_last_seq_is_disk_authoritative_on_a_warm_cache(
        self, root: pathlib.Path
    ) -> None:
        """Codex round 3 follow-up (PR #214, P2, comment 3925391258):
        `stats()`'s `last_seq` must reflect disk, not this instance's own
        possibly-stale cache -- `acked_seq` (`_watermarks()`) and `pending`
        (`self.pending()`) already re-read fresh from disk on every call;
        `last_seq` must too, or a heartbeat calling `stats()` on a warm
        instance reports a `last_seq` (and the backlog-depth picture that
        implies) that lags behind what another writer already put on disk."""
        a = Spool(root)
        a.append_next("channels", lambda seq: {"n": seq})  # seq=1
        a.ack("channels", 1)

        b = Spool(root)
        assert b.next_seq("channels") == 2  # warms b's cache at the OLD value (1)

        a.append_next("channels", lambda seq: {"n": seq})  # seq=2, on disk now
        a.ack("channels", 2)

        # b never performs any mutator itself -- stats() alone must catch up.
        st = b.stats()["channels"]
        assert st.last_seq == 2
        assert st.acked_seq == 2
        assert st.pending == 0


class TestNeverCappedLanes:
    def test_init_rejects_heartbeat_in_capped_lanes(self, root: pathlib.Path) -> None:
        """Spool.__init__ must reject heartbeat in capped_lanes."""
        with pytest.raises(ValueError, match="heartbeat") as exc_info:
            Spool(root, capped_lanes={"heartbeat": 1024})
        assert "never-drop" in str(exc_info.value)

    def test_init_accepts_channels_in_capped_lanes(self, root: pathlib.Path) -> None:
        """Spool.__init__ must accept channels in capped_lanes."""
        s = Spool(root, capped_lanes={"channels": 1024})
        assert s._capped == {"channels": 1024}

    def test_init_rejects_mixed_heartbeat_and_channels_in_capped_lanes(
        self,
        root: pathlib.Path,
    ) -> None:
        """Spool.__init__ must reject when heartbeat is mixed with other capped lanes."""
        with pytest.raises(ValueError, match="heartbeat") as exc_info:
            Spool(root, capped_lanes={"channels": 1024, "heartbeat": 1})
        assert "never-drop" in str(exc_info.value)

    def test_oversized_heartbeat_record_is_written_not_dropped(
        self, root: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Oversized record on heartbeat lane must be written, not dropped."""
        monkeypatch.setattr(spool_mod, "SEGMENT_MAX_BYTES", 200)
        s = Spool(root)
        giant = {"pad": "x" * 1000}
        s.append("heartbeat", 1, giant)
        assert [seq for seq, _ in s.pending("heartbeat")] == [1]
        assert s.stats()["heartbeat"].evicted == 0

    def test_oversized_capped_lane_record_is_dropped(
        self, root: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Oversized record on capped lane must be dropped (existing behavior)."""
        monkeypatch.setattr(spool_mod, "SEGMENT_MAX_BYTES", 200)
        s = Spool(root, capped_lanes={"channels": 1000})
        giant = {"pad": "x" * 1000}
        s.append("channels", 1, giant)
        assert [seq for seq, _ in s.pending("channels")] == []
        assert s.stats()["channels"].evicted == 1

    def test_heartbeat_lane_not_evicted_when_other_lanes_capped(
        self, root: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Heartbeat records must never be evicted, even when other lanes are capped."""
        monkeypatch.setattr(spool_mod, "SEGMENT_MAX_BYTES", 120)
        s = Spool(root, capped_lanes={"channels": 300})
        # Write heartbeat records far exceeding the channels cap
        for i in range(1, 30):
            s.append("heartbeat", i, {"pad": "x" * 40, "n": i})
        # Every heartbeat record should still be pending
        assert len([seq for seq, _ in s.pending("heartbeat")]) == 29
        # No eviction should have occurred
        assert s.stats()["heartbeat"].evicted == 0

    def test_for_robot_caps_only_channels(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """for_robot must cap only channels, never heartbeat or events."""
        monkeypatch.setattr("settings.settings.CACHE_DIRECTORY", str(tmp_path))
        monkeypatch.setattr("settings.settings.FLEET_SPOOL_MAX_BYTES", 500)
        s = Spool.for_robot("r7")
        assert s._capped == {"channels": 500}
        assert "heartbeat" not in s._capped
        assert "events" not in s._capped


class TestForRobot:
    def test_for_robot_single_segment_nests_correctly(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """for_robot('r7') creates spool at CACHE_DIRECTORY/publish/r7 with channels cap."""
        monkeypatch.setattr("settings.settings.CACHE_DIRECTORY", str(tmp_path))
        monkeypatch.setattr("settings.settings.FLEET_SPOOL_MAX_BYTES", 500)
        s = Spool.for_robot("r7")
        s.append("channels", 1, {"n": 1})
        assert (tmp_path / "publish" / "r7" / "channels").exists()
        assert s._capped["channels"] == 500

    def test_for_robot_two_segment_nests_correctly(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """for_robot('acme/r7') creates spool at CACHE_DIRECTORY/publish/acme/r7."""
        monkeypatch.setattr("settings.settings.CACHE_DIRECTORY", str(tmp_path))
        s = Spool.for_robot("acme/r7")
        s.append("channels", 1, {"n": 1})
        assert (tmp_path / "publish" / "acme" / "r7" / "channels").exists()

    def test_for_robot_rejects_leading_slash(self) -> None:
        with pytest.raises(ValueError, match="must not start with /"):
            Spool.for_robot("/etc/passwd")

    def test_for_robot_rejects_empty_string(self) -> None:
        with pytest.raises(ValueError, match="must be non-empty"):
            Spool.for_robot("")

    def test_for_robot_rejects_dot_dot(self) -> None:
        with pytest.raises(ValueError, match="at most one /"):
            Spool.for_robot("../../evil")

    def test_for_robot_rejects_dot_dot_in_path(self) -> None:
        with pytest.raises(ValueError, match="at most one /"):
            Spool.for_robot("a/../b")

    def test_for_robot_rejects_too_many_segments(self) -> None:
        with pytest.raises(ValueError, match="at most one /"):
            Spool.for_robot("a/b/c")

    def test_for_robot_rejects_empty_segment(self) -> None:
        with pytest.raises(ValueError, match="must be non-empty"):
            Spool.for_robot("acme/")

    def test_for_robot_rejects_dot_segment(self) -> None:
        with pytest.raises(ValueError, match="must be non-empty and not"):
            Spool.for_robot(".")


class TestDiskAuthoritativeMonotonicity:
    """Codex round 3 follow-up (PR #214, P1): `append()`'s monotonicity check
    must be disk-authoritative ACROSS `Spool` instances, not just within one.

    `exclusive()` (P1b) only serializes concurrent DISK ACCESS -- it does
    nothing to refresh a DIFFERENT already-open `Spool` instance's
    in-process `_last_seq` cache. A long-lived instance (e.g. a
    `FleetService`) whose cache predates an intervening writer on the same
    real spool (e.g. a selftest run, or another process entirely) must not
    accept/allocate a seq that collides with what's already on disk -- that
    would silently duplicate a seq instead of raising the clean `ValueError`
    the single-writer invariant promises.
    """

    def test_stale_cached_writer_raises_instead_of_duplicating(self, root: pathlib.Path) -> None:
        a = Spool(root)
        a.append("channels", 1, {"n": 1})
        a.append("channels", 2, {"n": 2})
        a.append("channels", 3, {"n": 3})

        # B is a fresh instance on the same root: first touch, scans disk,
        # continues the sequence correctly.
        b = Spool(root)
        b.append("channels", 4, {"n": 4})
        before = list(b.pending("channels"))

        # A's in-process cache still says last_seq=3 -- it never saw B's
        # write. A disk-authoritative check must catch this collision.
        with pytest.raises(ValueError, match="monotonic"):
            a.append("channels", 4, {"n": "duplicate-attempt"})

        # Nothing was written by the rejected call -- disk state intact.
        after = list(b.pending("channels"))
        assert after == before
        assert [seq for seq, _ in after] == [1, 2, 3, 4]

    def test_stale_cached_writer_can_still_append_the_true_next_seq(
        self, root: pathlib.Path
    ) -> None:
        """After the collision above, the same stale instance must recover
        cleanly once given the actually-correct next seq -- not get stuck."""
        a = Spool(root)
        a.append("channels", 1, {"n": 1})
        b = Spool(root)
        b.append("channels", 2, {"n": 2})

        with pytest.raises(ValueError, match="monotonic"):
            a.append("channels", 2, {"n": "stale"})

        a.append("channels", 3, {"n": 3})  # the true next seq -- must succeed
        assert [seq for seq, _ in a.pending("channels")] == [1, 2, 3]

    def test_stale_next_seq_also_reflects_disk_not_the_cache(self, root: pathlib.Path) -> None:
        """`next_seq()` must see a concurrent writer's advance too, not just
        `append()`'s rejection path -- so next_seq() -> append() never
        re-collides on the second try."""
        a = Spool(root)
        a.append("channels", 1, {"n": 1})
        b = Spool(root)
        b.append("channels", 2, {"n": 2})

        assert a.next_seq("channels") == 3

    def test_full_scan_last_seq_only_runs_on_first_touch_not_every_append(
        self, root: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Perf guard: the disk-authoritative check must not degrade every
        `append()` into an O(whole active segment) full re-parse -- only the
        lane's first touch in this instance's lifetime may call the
        expensive `_scan_last_seq` (crash-tail recovery); every later call
        must use the cheap tail-only read."""
        s = Spool(root)
        calls: list[str] = []
        original = spool_mod.Spool._scan_last_seq

        def spy(self: Spool, lane: str) -> int:
            calls.append(lane)
            return original(self, lane)

        monkeypatch.setattr(spool_mod.Spool, "_scan_last_seq", spy)

        for i in range(1, 6):
            s.append("channels", i, {"n": i})

        assert calls == ["channels"]  # only the very first append's first-touch scan


class TestTailLastSeqWindowGrowth:
    """Codex round 3 follow-up (PR #214, P1): `_tail_last_seq`'s backward-read
    loop must grow its window GEOMETRICALLY (double each retry), not by a
    fixed 8KiB step re-reading the whole (growing) window every iteration --
    the fixed step is quadratic total bytes read for a final record bigger
    than one chunk (worst case a segment-max ~4MB single-line record)."""

    def test_correct_for_a_final_record_well_over_one_chunk(self, root: pathlib.Path) -> None:
        s = Spool(root)
        s.append("channels", 1, {"n": 1})
        # ~100KiB payload -- comfortably more than one 8KiB read chunk, so
        # the loop must retry (grow the window) to reach the newline that
        # separates it from record 1.
        s.append("channels", 2, {"pad": "x" * 100_000})

        assert s._tail_last_seq("channels") == (2, True)

    def test_read_call_count_is_geometric_not_linear(
        self, root: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        s = Spool(root)
        s.append("channels", 1, {"n": 1})
        s.append("channels", 2, {"pad": "x" * 100_000})

        real_open = open
        read_calls: list[int] = []

        class _CountingHandle:
            """Wraps a real binary file handle, counting `.read()` calls."""

            def __init__(self, real: object) -> None:
                self._real = real

            def seek(self, *a: object, **k: object) -> object:
                return self._real.seek(*a, **k)

            def read(self, *a: object, **k: object) -> bytes:
                read_calls.append(1)
                return self._real.read(*a, **k)

            def __enter__(self) -> "_CountingHandle":
                return self

            def __exit__(self, *exc: object) -> object:
                return self._real.__exit__(*exc)

        def fake_open(file: object, mode: str = "r", *a: object, **k: object) -> object:
            handle = real_open(file, mode, *a, **k)
            return _CountingHandle(handle) if mode == "rb" else handle

        monkeypatch.setattr("builtins.open", fake_open)

        result = s._tail_last_seq("channels")

        assert result == (2, True)
        # +1 read for the O(1) last-byte terminator check, then geometric
        # doubling from 8KiB needs ~5 reads to cover ~100KiB (8, 16, 32, 64,
        # then the size-capped final read); a fixed 8KiB linear step would
        # need ~13+ on top of that. 9 is a robust dividing line between the
        # two.
        assert len(read_calls) <= 9, f"expected geometric (<=9) reads, got {len(read_calls)}"


class TestUnterminatedTailReseal:
    """Codex round 3 follow-up (PR #214, P1, comment 3923824688): a torn
    (non-newline-terminated) active-segment tail must be sealed (truncated)
    BEFORE the next append, on a WARM cache too -- not just on a lane's
    first touch. Before this fix, `_tail_last_seq` inferred terminatedness
    from whether the tail happened to parse, which is wrong two ways: (a) a
    crash landing right after a complete JSON payload but before its
    trailing newline parses FINE without the newline, so it was reported as
    a trustworthy seq; (b) a genuinely partial/mid-write tail failed to
    parse, but nothing then truncated it before the next write landed
    directly on top of it. Either way, `append()`'s next `write()` (append
    mode) grafted new bytes onto the untermianted tail with no separating
    newline -- permanent mid-file corruption, discovered only later when
    `pending()` raises `SpoolCorruptError`.
    """

    def _active_segment(self, root: pathlib.Path, lane: str) -> pathlib.Path:
        return sorted((root / lane).glob("segment-*.jsonl"))[-1]

    def test_complete_json_without_trailing_newline_is_resealed_before_append(
        self, root: pathlib.Path
    ) -> None:
        s = Spool(root)
        s.append("channels", 1, {"n": 1})  # warms this instance's cache

        # Simulate a crash landing exactly after a COMPLETE JSON payload but
        # before its trailing newline -- parses fine on its own, which is
        # exactly what made the old parse-based check trust it.
        segment = self._active_segment(root, "channels")
        with open(segment, "ab") as handle:
            handle.write(json.dumps({"seq": 2, "payload": {"n": "never-committed"}}).encode())
        assert not segment.read_bytes().endswith(b"\n")

        # The cache is warm (this instance already appended once) -- the
        # torn tail must still be caught here, not just on first touch.
        seq = s.next_seq("channels")
        assert seq == 2  # the torn seq=2 bytes were never durably committed
        s.append("channels", seq, {"n": 2})

        assert segment.read_bytes().endswith(b"\n")  # resealed to a clean boundary
        replayed = list(s.pending("channels"))  # must not raise SpoolCorruptError
        assert [seq for seq, _ in replayed] == [1, 2]
        assert replayed[1][1] == {"n": 2}

    def test_partial_json_tail_is_resealed_before_append(self, root: pathlib.Path) -> None:
        s = Spool(root)
        s.append("channels", 1, {"n": 1})  # warms this instance's cache

        # Simulate a crash mid-write: not even valid JSON, no newline.
        segment = self._active_segment(root, "channels")
        with open(segment, "ab") as handle:
            handle.write(b'{"seq": 2, "payload": {"n": ')
        assert not segment.read_bytes().endswith(b"\n")

        seq = s.next_seq("channels")
        assert seq == 2
        s.append("channels", seq, {"n": 2})

        assert segment.read_bytes().endswith(b"\n")
        replayed = list(s.pending("channels"))
        assert [seq for seq, _ in replayed] == [1, 2]
        assert replayed[1][1] == {"n": 2}

    def test_normal_terminated_tail_never_triggers_a_reseal(
        self, root: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The healthy path must stay cheap: a properly newline-terminated
        tail must never fall back to the full `_scan_last_seq` reseal --
        the warm cache stays effective, pinned by a call-counting probe."""
        s = Spool(root)
        s.append("channels", 1, {"n": 1})  # first touch: its own _scan_last_seq call

        calls: list[str] = []
        original = spool_mod.Spool._scan_last_seq

        def spy(self: Spool, lane: str) -> int:
            calls.append(lane)
            return original(self, lane)

        monkeypatch.setattr(spool_mod.Spool, "_scan_last_seq", spy)

        for i in range(2, 6):
            s.append("channels", i, {"n": i})

        assert calls == []  # no reseal once every tail stays properly terminated


class TestEmptyOrUnparsableActiveSegmentFloorsAtFilename:
    """Codex round 3 follow-up (PR #214, P1/P2, comment 3924387659).

    P1: a `None` tail on an EXISTING active segment (empty -- a roll that
    crashed before its first record landed -- or unparsable) must be
    treated exactly like an unterminated tail: reseal via the full
    `_scan_last_seq` path, which derives the FILENAME floor
    (`_first_seq_of(segment) - 1`). The old fast path fell back to the
    watermark ALONE in this case, ignoring both the filename floor and any
    earlier segments' still-live (unacked) records -- a floor REGRESSION
    that let a warm cache's `append_next()` allocate a seq that collides
    with data already durably on disk (not merely a stale-but-safe
    undercount).

    P2: the disk-derived floor on the NORMAL (non-reseal) read path must be
    stored back into `self._last_seq[lane]` -- previously computed fresh
    every call but discarded, letting the cache silently lag behind what
    this instance's own reads had already established. Safe because it's
    folded through `max()`, so it can only advance.
    """

    def test_empty_active_segment_on_a_warm_cache_floors_at_filename_not_watermark(
        self, root: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(spool_mod, "SEGMENT_MAX_BYTES", 32)
        a = Spool(root)
        a.append("channels", 1, {"n": 1})  # segment-1

        b = Spool(root)
        assert b.next_seq("channels") == 2  # warms b's cache at the OLD value (last=1)

        a.append("channels", 2, {"n": 2})  # segment-2 (cap forces a roll every append)
        a.append("channels", 3, {"n": 3})  # segment-3
        segments = sorted((root / "channels").glob("segment-*.jsonl"))
        assert len(segments) == 3

        # Simulate the crashed roller: a 4th append started a roll (created
        # the next segment, named for the seq it was about to write) but
        # crashed before any bytes landed -- b never saw any of this, its
        # cache still says 1.
        next_segment = root / "channels" / spool_mod._segment_name(4)
        next_segment.write_bytes(b"")

        allocated = b.append_next("channels", lambda seq: {"n": seq})

        # The filename floor (segment-4 => floor 3) plus the live records
        # 1-3 already on disk means the correct next seq is 4 -- never
        # watermark(0)+1 == 1, which would collide with record 1, or worse
        # get silently written INTO segment-4 under a mismatched name.
        assert allocated == 4
        assert [seq for seq, _ in b.pending("channels")] == [1, 2, 3, 4]

    def test_unparsable_terminated_tail_on_a_warm_cache_also_reseals(
        self, root: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Same ruling, the other `None`-tail sub-case: a properly
        newline-terminated segment whose content still doesn't parse as a
        usable record (e.g. a stray newline-only segment) must reseal too --
        not silently trust the watermark alone, which here would ignore
        both the filename floor and an earlier segment's still-live
        (unacked) record."""
        monkeypatch.setattr(spool_mod, "SEGMENT_MAX_BYTES", 32)
        a = Spool(root)
        a.append("channels", 1, {"n": 1})  # segment-1

        b = Spool(root)
        assert b.next_seq("channels") == 2  # warms b's cache at 1

        a.append("channels", 2, {"n": 2})  # segment-2 (cap forces a roll)
        segments = sorted((root / "channels").glob("segment-*.jsonl"))
        assert len(segments) == 2

        # A stray newline-only segment named for seq 3 -- terminated
        # (`\n`), but nothing parses out of it: the "unparsable" sub-case
        # of a `None` tail.
        stray_segment = root / "channels" / spool_mod._segment_name(3)
        stray_segment.write_bytes(b"\n")

        allocated = b.append_next("channels", lambda seq: {"n": seq})

        assert allocated == 3  # filename floor (segment-3 => floor 2), never watermark(0)+1 == 1
        assert [seq for seq, _ in b.pending("channels")] == [1, 2, 3]

    def test_disk_derived_floor_is_stored_back_into_the_cache(self, root: pathlib.Path) -> None:
        """P2: a pure peek (`next_seq()`) that derives a fresher floor than
        this instance's own stale cache must store that fresher value back,
        not just return it and discard it."""
        a = Spool(root)
        a.append_next("channels", lambda seq: {"n": seq})  # a's cache: last_seq=1

        b = Spool(root)
        b.append_next("channels", lambda seq: {"n": seq})  # disk now at seq=2; a's cache still 1

        peeked = a.next_seq("channels")  # a pure read -- must reflect disk (2), not stale (1)
        assert peeked == 3

        # The freshly-derived floor (2) must be stored back into a's own
        # cache, not merely returned -- observable by reading it directly.
        assert a._last_seq["channels"] == 2

    def test_stored_back_floor_does_not_cause_extra_full_scans(
        self, root: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The store-back must not regress the existing cheap-path
        guarantee: `_scan_last_seq` (the expensive full-segment parse)
        still only ever runs on a lane's first touch, never on a repeated
        read/peek."""
        a = Spool(root)
        a.append_next("channels", lambda seq: {"n": seq})  # first touch

        calls: list[str] = []
        original = spool_mod.Spool._scan_last_seq

        def spy(self: Spool, lane: str) -> int:
            calls.append(lane)
            return original(self, lane)

        monkeypatch.setattr(spool_mod.Spool, "_scan_last_seq", spy)

        for _ in range(5):
            a.next_seq("channels")

        assert calls == []  # no re-derivation via the expensive path


class TestAppendNext:
    """Codex round 3 follow-up (PR #214, P1, comment 3924082774): `append_next()`
    closes the allocate-then-append race structurally. The old shape --
    `next_seq()` then a separate `append()` call -- was TWO lock
    acquisitions with a gap between them; a competing writer (another
    `Spool` instance's own call, or a `spool.exclusive()`-held run, e.g. the
    selftest) could land in that gap and advance the lane first, making the
    original caller's `append()` raise `ValueError` against a seq it
    allocated against a now-stale floor. `append_next()` derives the floor,
    assigns the seq, and writes all under ONE lock acquisition, so there is
    no gap left for a competitor to land in.
    """

    def test_old_two_call_windows_collision_is_now_impossible(self, root: pathlib.Path) -> None:
        """Directly reproduces what used to be exploitable: A peeks via the
        (still-legitimate, read-only) `next_seq()`, then B interleaves in
        exactly the window that peek would have left open in the old
        `next_seq()` + `append()` shape, via an `exclusive()`-held run
        (mirroring the selftest's own pattern). A's ACTUAL write, routed
        through `append_next()` instead of the old `append(peeked, ...)`,
        must succeed with a freshly re-derived (not stale) seq -- zero
        `ValueError`."""
        a = Spool(root)
        b = Spool(root)

        peeked = a.next_seq("channels")
        assert peeked == 1  # what A would have (incorrectly) tried to write at, pre-fix

        with b.exclusive(timeout=1.0):
            b.append_next("channels", lambda seq: {"seq": seq, "who": "b"})

        actual_seq = a.append_next("channels", lambda seq: {"seq": seq, "who": "a"})

        assert actual_seq == 2  # correctly re-derived, not the stale peeked=1
        assert [seq for seq, _ in a.pending("channels")] == [1, 2]

    def test_competitor_cannot_land_between_allocation_and_write(
        self, root: pathlib.Path
    ) -> None:
        """Direct proof of atomicity: while A's `append_next` is mid-flight
        (its `build` callable deliberately blocked, so we can observe it
        running -- `build` runs INSIDE the lock), B's `append_next` must be
        BLOCKED until A's single critical section completes. In the old
        `next_seq()` + `append()` shape, the lock was already released by
        this point (right after `next_seq()` returned), leaving exactly
        this window open for B to land in."""
        a = Spool(root)
        b = Spool(root)
        a_entered_build = threading.Event()
        release_a = threading.Event()
        order: list[str] = []

        def slow_build(seq: int) -> dict:
            order.append("a-build-entered")
            a_entered_build.set()
            release_a.wait(timeout=2.0)
            order.append("a-build-exit")
            return {"seq": seq}

        t_a = threading.Thread(target=lambda: a.append_next("channels", slow_build))
        t_a.start()
        a_entered_build.wait(timeout=2.0)

        def run_b() -> None:
            b.append_next("channels", lambda seq: {"seq": seq})
            order.append("b-append_next-done")

        t_b = threading.Thread(target=run_b)
        t_b.start()
        time.sleep(0.1)  # give B a chance to attempt -- it must still be blocked
        assert "b-append_next-done" not in order

        release_a.set()
        t_a.join()
        t_b.join()

        assert order.index("a-build-exit") < order.index("b-append_next-done")

    def test_two_instances_stress_interleaved_yield_unique_contiguous_seqs(
        self, root: pathlib.Path
    ) -> None:
        """Broader stress complement to the two directed tests above: many
        concurrent `append_next` calls from two separate `Spool` instances,
        zero `ValueError`, every seq 1..N allocated exactly once."""
        a = Spool(root)
        b = Spool(root)
        errors: list[BaseException] = []
        seqs: list[int] = []
        seqs_lock = threading.Lock()
        per_writer = 25

        def writer(spool: Spool, tag: str) -> None:
            for i in range(per_writer):
                try:
                    seq = spool.append_next("channels", lambda seq, i=i: {"seq": seq, "i": i})
                except BaseException as exc:  # collected, asserted below (not swallowed)
                    errors.append(exc)
                else:
                    with seqs_lock:
                        seqs.append(seq)

        t_a = threading.Thread(target=writer, args=(a, "a"))
        t_b = threading.Thread(target=writer, args=(b, "b"))
        t_a.start()
        t_b.start()
        t_a.join()
        t_b.join()

        assert errors == []
        assert sorted(seqs) == list(range(1, 2 * per_writer + 1))
        assert len(set(seqs)) == 2 * per_writer


class TestExclusiveLock:
    """`Spool.exclusive()` (Codex round 3, P1b): hold the spool's real lock
    across a multi-call critical section, cross-process-safe, bounded wait.
    """

    def test_reentrant_with_mutators_on_the_same_instance(self, root: pathlib.Path) -> None:
        """Calls made inside the `with` block must not deadlock on their own lock."""
        s = Spool(root)
        with s.exclusive(timeout=1.0):
            seq = s.next_seq("channels")
            s.append("channels", seq, {"n": 1})
            s.ack("channels", seq)
            s.stats()
        assert list(s.pending("channels")) == []

    def test_second_instance_blocks_until_released_then_proceeds(
        self, root: pathlib.Path
    ) -> None:
        """A second `Spool` on the same root waits for the first to release,
        then succeeds -- this is a real OS-level lock, not merely advisory
        within one instance."""
        s1 = Spool(root)
        s2 = Spool(root)
        released = threading.Event()
        acquired_order: list[str] = []

        def hold_then_release() -> None:
            with s1.exclusive(timeout=1.0):
                acquired_order.append("s1")
                time.sleep(0.2)
            released.set()

        t = threading.Thread(target=hold_then_release)
        t.start()
        time.sleep(0.05)  # let s1 grab the lock first

        with s2.exclusive(timeout=2.0):
            acquired_order.append("s2")
            assert released.is_set()  # s2 only got in after s1 let go

        t.join()
        assert acquired_order == ["s1", "s2"]

    def test_second_instance_times_out_with_typed_error_while_first_holds(
        self, root: pathlib.Path
    ) -> None:
        """The bounded-wait refusal: a lock held elsewhere for longer than the
        timeout raises `SpoolLockedError`, not an indefinite hang."""
        s1 = Spool(root)
        s2 = Spool(root)
        holding = threading.Event()
        release = threading.Event()

        def hold_until_told() -> None:
            with s1.exclusive(timeout=1.0):
                holding.set()
                release.wait(timeout=2.0)

        t = threading.Thread(target=hold_until_told)
        t.start()
        holding.wait(timeout=2.0)

        with pytest.raises(SpoolLockedError, match="locked by another writer"):
            s2.exclusive(timeout=0.1)

        release.set()
        t.join()

    def test_a_plain_mutator_still_waits_indefinitely_not_a_typed_refusal(
        self, root: pathlib.Path
    ) -> None:
        """Only `exclusive()`'s bounded wait times out -- the ordinary per-call
        mutators keep their existing block-forever behavior (unchanged)."""
        s1 = Spool(root)
        s2 = Spool(root)
        holding = threading.Event()

        def hold_briefly() -> None:
            with s1.exclusive(timeout=1.0):
                holding.set()
                time.sleep(0.15)

        t = threading.Thread(target=hold_briefly)
        t.start()
        holding.wait(timeout=2.0)

        # s2.next_seq blocks on the same lock but has no timeout -- it must
        # simply wait for s1 to finish, not raise.
        assert s2.next_seq("channels") == 1
        t.join()


def test_spool_module_does_not_import_paho_eagerly(monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib
    import sys

    for name in [m for m in sys.modules if m == "paho" or m.startswith("paho.")]:
        monkeypatch.delitem(sys.modules, name)
    monkeypatch.delitem(sys.modules, "src.sink.publish.spool", raising=False)
    importlib.import_module("src.sink.publish.spool")
    assert not any(m == "paho" or m.startswith("paho.") for m in sys.modules)
