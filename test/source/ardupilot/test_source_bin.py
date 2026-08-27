import pytest

from src.source.ardupilot import bin as ardubin


def test_source_factory() -> None:
    # GIVEN
    factory = ardubin.SourceFactory("data/sample/ardupilot/sample.bin")

    # WHEN / THEN
    assert factory.total_message_count == 69769
    assert factory.start_seconds == 1754307092.5810509
    assert factory.end_seconds == 1754307201.7701719


def test_validate_path_should_raise() -> None:
    # WHEN / THEN
    with pytest.raises(FileNotFoundError):
        ardubin.SourceFactory("data/sample/ardupilot/non_exist.bin")


class _FakeMsg:
    def __init__(self, timestamp: float) -> None:
        self._timestamp = timestamp


class _FakeReader:
    """Mimics DFReader_binary's per-type offsets: the highest file offset holds a
    trailing FMT-style record that inherits the *first* timestamp."""

    def __init__(self) -> None:
        self.counts = [-1] * 256
        self.offsets: list[list[int]] = [[] for _ in range(256)]
        self.offset = 0
        self._by_offset: dict[int, float] = {}
        self.counts[0], self.offsets[0], self._by_offset[10] = 1, [10], 100.0  # FMT at start
        self.counts[1], self.offsets[1], self._by_offset[500] = 3, [20, 300, 500], 160.5
        self.counts[2], self.offsets[2], self._by_offset[900] = (
            2,
            [40, 900],
            100.0,
        )  # trailing, no TimeUS
        self.counts[3], self.offsets[3], self._by_offset[700] = 5, [60, 700], 172.25

    def recv_msg(self) -> _FakeMsg | None:
        timestamp = self._by_offset.get(self.offset)
        return _FakeMsg(timestamp) if timestamp is not None else None


def test_end_seconds_is_max_over_last_message_of_each_type() -> None:
    """pymavlink's last_timestamp() decodes only the highest-offset record, which on
    real logs can be a trailing FMT with no TimeUS (issue #198)."""
    from src.source.ardupilot.bin import last_timestamp_seconds

    assert last_timestamp_seconds(_FakeReader()) == 172.25
