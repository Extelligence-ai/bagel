"""Resource-limit behavior (#134): memory scales with the query, not the file.

Before hardening, parse_file() slurped the whole log via read_text().splitlines()
(~3x file size peak: raw string + line list + records). After: it iterates the
open handle, so peak is ~the records themselves plus one line. The test uses
long messages so records ≈ file size; the old implementation peaks ≥3x and the
new one ~1.2x, so a 2x threshold cleanly separates them.
"""

import pathlib
import tracemalloc
from typing import Any, NoReturn

import numpy as np
import pytest

from src.source.ros.parse import parse_file


def test_parse_file_streams_instead_of_slurping(tmp_path: pathlib.Path) -> None:
    message = "x" * 10_000
    line = f"[INFO] [1662400000.100000000] [talker]: {message}\n"
    log = tmp_path / "big.log"
    with open(log, "w", encoding="utf-8") as f:
        for _ in range(2_000):
            f.write(line)
    size = log.stat().st_size  # ~20 MB

    tracemalloc.start()
    records = parse_file(log)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert len(records) == 2_000
    assert records[0].message == message
    assert peak < 2 * size, f"peak {peak} vs file {size}: parse_file is not streaming"


# ---------------------------------------------------------------------------
# CAN: one-pass metadata stats and windowed, uncached records (#134).
#
# ``valid_asc_and_dbc`` reuses ``_write_valid_dbc`` from test_can_parse.py
# (a module-level function, imported directly). test_can_parse.py has no
# reusable *valid* ASC-writing helper (only inline garbage/malformed ASC text
# for its failure-path tests), so the ASC body here is written locally,
# following the same frame-line format used there
# (test_asc_with_non_hex_frame_data_raises_clean_error): hex-base timestamps,
# arbitration id "64" (hex) = 100 (decimal) to match the DBC's ``BO_ 100
# EngineData``.
# ---------------------------------------------------------------------------


@pytest.fixture
def valid_asc_and_dbc(tmp_path: pathlib.Path) -> tuple[pathlib.Path, pathlib.Path]:
    pytest.importorskip("can")
    pytest.importorskip("cantools")
    from test.adversarial.test_can_parse import _write_valid_dbc

    dbc = _write_valid_dbc(tmp_path)
    asc = tmp_path / "valid.asc"
    lines = [
        "date Mon Jan 1 00:00:00 2024",
        "base hex  timestamps absolute",
        "no internal events logged",
        "",
    ]
    for index, timestamp in enumerate((1.0, 2.0, 3.0, 4.0, 5.0)):
        data = " ".join(f"{(byte + index) % 256:02X}" for byte in range(8))
        lines.append(f"   {timestamp:.6f} 1  64             Rx   d 8 {data}")
    asc.write_text("\n".join(lines) + "\n")
    return asc, dbc


def test_can_stats_matches_records(valid_asc_and_dbc: tuple[pathlib.Path, pathlib.Path]) -> None:
    """stats must equal what a full decode reports, without storing frames."""
    from src.source.automotive import can as can_source

    capture, dbc = valid_asc_and_dbc
    log = can_source.CanLog(path=str(capture), dbc=str(dbc))
    records = log.records()
    count, start, end = log.stats
    assert count == len(records)
    assert start == records[0][0]
    assert end == records[-1][0]


def test_can_metadata_does_not_materialize_records(
    valid_asc_and_dbc: tuple[pathlib.Path, pathlib.Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """describe-path properties must never build the full decoded list."""
    from src.source.automotive import can as can_source

    capture, dbc = valid_asc_and_dbc
    factory = can_source.SourceFactory(path=str(capture), dbc=str(dbc))

    def _boom(*args: object, **kwargs: object) -> None:
        raise AssertionError("records() must not be called by metadata paths")

    monkeypatch.setattr(factory._log, "records", _boom)
    assert factory.total_message_count > 0
    assert factory.start_seconds <= factory.end_seconds
    assert "dbc_messages" in factory.metadata


def test_can_records_window_filters_before_sort(
    valid_asc_and_dbc: tuple[pathlib.Path, pathlib.Path],
) -> None:
    from src.source.automotive import can as can_source

    capture, dbc = valid_asc_and_dbc
    log = can_source.CanLog(path=str(capture), dbc=str(dbc))
    everything = log.records()
    mid = everything[len(everything) // 2][0]
    windowed = log.records(start_seconds=mid)
    assert windowed == [r for r in everything if r[0] >= mid]


def test_can_topic_counts_come_from_one_pass_not_records(
    valid_asc_and_dbc: tuple[pathlib.Path, pathlib.Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Enumerating topics/counts must not re-decode via records() per topic (#134)."""
    from src.source.automotive import can as can_source
    from src.topic.automotive.can import TopicRegistry

    capture, dbc = valid_asc_and_dbc
    log = can_source.CanLog(path=str(capture), dbc=str(dbc))

    # Ground truth, independently derived from a full decode.
    expected_counts: dict[str, int] = {}
    for _, name, _ in log.records():
        expected_counts[name] = expected_counts.get(name, 0) + 1

    topic_counts = log.topic_message_counts
    assert topic_counts == expected_counts
    assert sum(topic_counts.values()) == log.stats[0]

    registry = TopicRegistry()

    def _boom(*args: object, **kwargs: object) -> None:
        raise AssertionError("records() must not be called by topic enumeration")

    monkeypatch.setattr(log, "records", _boom)

    assert sorted(registry.available_topics(log)) == sorted(expected_counts)
    for name, count in expected_counts.items():
        assert registry.message_count(name, log) == count


# ---------------------------------------------------------------------------
# MDF4: window-first channel slicing in the message stream (#134).
# ---------------------------------------------------------------------------


@pytest.fixture
def small_mf4(tmp_path: pathlib.Path) -> pathlib.Path:
    asammdf = pytest.importorskip("asammdf")
    timestamps = np.arange(0.0, 10.0, 0.5)  # 20 samples, relative seconds
    signal = asammdf.Signal(
        samples=np.arange(len(timestamps), dtype=np.float64),
        timestamps=timestamps,
        name="speed",
    )
    mdf = asammdf.MDF()
    mdf.append([signal], acq_name="Engine")
    path = tmp_path / "small.mf4"
    mdf.save(path, overwrite=True)
    mdf.close()
    return path


def _mf4_messages(
    path: pathlib.Path, start: float | None = None, end: float | None = None
) -> list[tuple[str, float, dict[str, Any]]]:
    from asammdf import MDF

    from src.message.automotive.mf4 import MessageDataset

    mdf = MDF(str(path))
    try:
        return list(MessageDataset()._messages(mdf, ["Engine"], start, end))
    finally:
        mdf.close()


def test_mf4_window_equals_filtered_full_read(small_mf4: pathlib.Path) -> None:
    from asammdf import MDF

    start_epoch = MDF(str(small_mf4)).header.start_time.timestamp()
    everything = _mf4_messages(small_mf4)
    windowed = _mf4_messages(small_mf4, start_epoch + 2.0, start_epoch + 7.0)
    assert windowed == [m for m in everything if start_epoch + 2.0 <= m[1] <= start_epoch + 7.0]
    assert 0 < len(windowed) < len(everything)


def test_mf4_window_loads_only_the_slice(
    small_mf4: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Channel loads must pass record_offset/record_count for the window."""
    from asammdf import MDF

    mdf = MDF(str(small_mf4))
    start_epoch = mdf.header.start_time.timestamp()
    calls: list[dict[str, Any]] = []
    original_get = mdf.get

    def spying_get(*args: object, **kwargs: object) -> object:
        calls.append(kwargs)
        return original_get(*args, **kwargs)

    monkeypatch.setattr(mdf, "get", spying_get)
    from src.message.automotive.mf4 import MessageDataset

    list(MessageDataset()._messages(mdf, ["Engine"], start_epoch + 2.0, start_epoch + 7.0))
    mdf.close()
    assert calls, "expected channel loads"
    for kwargs in calls:
        assert kwargs.get("record_count") is not None
        assert kwargs["record_count"] < 20, "full-channel load defeats window slicing"


def test_mf4_struct_and_end_seconds_read_one_sample(
    small_mf4: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from src.source.automotive.mf4 import SourceFactory
    from src.topic.automotive.mf4 import TopicRegistry

    factory = SourceFactory(str(small_mf4))
    mdf = factory.build()

    get_counts = []
    original_get = mdf.get

    def spying_get(*args: object, **kwargs: object) -> object:
        get_counts.append(kwargs.get("record_count"))
        return original_get(*args, **kwargs)

    monkeypatch.setattr(mdf, "get", spying_get)
    struct = TopicRegistry().struct("Engine", mdf)
    assert struct.field("speed").type is not None
    assert all(count == 1 for count in get_counts), "struct() must load one sample per channel"

    end = factory.end_seconds
    assert end == factory.start_seconds + 9.5  # last timestamp in the fixture


def test_record_batches_row_count_is_clamped() -> None:
    """#134 addendum: the byte-target batch estimate must be clamped by row count.

    Small rows (e.g. IMU) make the 1 GB Arrow-byte target resolve to millions of
    rows per batch, which accumulate as Python objects at 10-20x overhead before
    each flush: a 416 MB / 1.2M-message bag OOM-killed an 8 GB server (matrix,
    2026-08-14). Batches must flush at MAX_ARROW_RECORD_BATCH_SIZE_COUNT rows
    regardless of the byte estimate.
    """
    import pyarrow as pa

    from settings import settings
    from src.message.base import MessageDataset

    class _Dataset(MessageDataset):
        def _messages(self, *args: object, **kwargs: object) -> NoReturn:  # pragma: no cover
            raise NotImplementedError

        def _to_json(self, message: object, struct: object) -> object:
            return message

    schema = pa.schema(
        [
            pa.field(settings.TIMESTAMP_SECONDS_COLUMN_NAME, pa.float64()),
            pa.field("/imu", pa.struct([pa.field("x", pa.float64())])),
        ]
    )
    total = 300_000
    messages = (("/imu", float(i), {"x": 1.0}) for i in range(total))
    batches = list(_Dataset()._record_batches(messages, schema, ffill=False))

    assert sum(batch.num_rows for batch in batches) == total
    cap = settings.MAX_ARROW_RECORD_BATCH_SIZE_COUNT
    oversized = [batch.num_rows for batch in batches if batch.num_rows > cap]
    assert not oversized, f"batches exceed the row cap ({cap}): {oversized}"


def test_can_iter_records_streams_in_order(valid_asc_and_dbc: tuple) -> None:
    """Codex review on #156: the CAN path must stream into Arrow batching.

    records() materializes the full decoded list, so the record-batch row
    clamp cannot bound CAN captures. iter_records() streams with a bounded
    reorder heap instead; on (near-)chronological captures its output equals
    the fully sorted list.
    """
    import inspect

    from src.source.automotive import can as can_source

    capture, dbc = valid_asc_and_dbc
    log = can_source.CanLog(path=str(capture), dbc=str(dbc))
    assert inspect.isgenerator(log.iter_records())
    assert list(log.iter_records()) == log.records()
    everything = log.records()
    mid = everything[len(everything) // 2][0]
    assert list(log.iter_records(start_seconds=mid)) == log.records(start_seconds=mid)


def test_can_message_layer_streams_not_materializes(
    valid_asc_and_dbc: tuple, monkeypatch: pytest.MonkeyPatch
) -> None:
    """_messages must consume the streaming iterator, never the full list."""
    from src.message.automotive.can import MessageDataset as CanMessages
    from src.source.automotive import can as can_source

    capture, dbc = valid_asc_and_dbc
    log = can_source.CanLog(path=str(capture), dbc=str(dbc))

    def _boom(*args: object, **kwargs: object) -> None:
        raise AssertionError("_messages must not materialize via records()")

    monkeypatch.setattr(log, "records", _boom)
    messages = list(CanMessages()._messages(log, ["EngineData"], None, None))
    assert messages, "expected streamed messages"
