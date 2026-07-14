"""Wire-level tests: an OnEvent pipeline attached to the live sink buffer.

Exercises the path `TopicBufferWriter.append -> evaluate_predicate -> LiveEventTrigger
-> Pipeline.run_at` without a ROS bridge, proving the live edge-recorder wiring that
`TopicSink.subscribe(topic, pipeline=...)` uses.
"""

import pathlib
from unittest.mock import MagicMock

import pyarrow as pa

from src.pipeline.base import Cadence, Frequency, Lookback, OnEvent, Unit
from src.sink.buffer import TopicBufferWriter

STRUCT = pa.struct([("ax", pa.float64()), ("t", pa.float64())])


def _writer(tmp_path: pathlib.Path, when: object) -> tuple[TopicBufferWriter, MagicMock]:
    pipeline = MagicMock()
    pipeline.cadence = Cadence(topic="imu", when=when)
    writer = TopicBufferWriter(
        path=tmp_path,
        topic="imu",
        type_name="test/Imu",
        definition="float64 ax",
        struct=STRUCT,
        buffer_size_bytes=None,
        overwrite=True,
        pipeline=pipeline,
        extract_timestamp=lambda message: message["t"],
    )
    return writer, pipeline


def _feed(writer: TopicBufferWriter, samples: list[tuple[float, float]]) -> None:
    for t, ax in samples:
        writer.append({"ax": ax, "t": t})


def test_on_event_fires_once_per_rising_edge(tmp_path: pathlib.Path) -> None:
    when = OnEvent(predicate="imu['ax'] < -10")
    writer, pipeline = _writer(tmp_path, when)

    _feed(writer, [(0.0, -0.5), (1.0, -12.0), (2.0, -12.0), (3.0, -0.5), (4.0, -15.0)])

    assert [call.args[0] for call in pipeline.run_at.call_args_list] == [1.0, 4.0]


def test_forward_window_delays_firing_until_post_data_arrived(
    tmp_path: pathlib.Path,
) -> None:
    when = OnEvent(
        predicate="imu['ax'] < -10", forward=Lookback(last=2, unit=Unit.SECOND)
    )
    writer, pipeline = _writer(tmp_path, when)

    _feed(writer, [(0.0, -0.5), (1.0, -12.0), (2.0, -0.5)])
    pipeline.run_at.assert_not_called()  # post-window (until t=3) not yet buffered

    _feed(writer, [(3.5, -0.5)])
    assert [call.args[0] for call in pipeline.run_at.call_args_list] == [1.0]


def test_flush_fires_events_still_awaiting_forward_window(tmp_path: pathlib.Path) -> None:
    when = OnEvent(
        predicate="imu['ax'] < -10", forward=Lookback(last=10, unit=Unit.SECOND)
    )
    writer, pipeline = _writer(tmp_path, when)

    _feed(writer, [(0.0, -0.5), (5.0, -13.0), (6.0, -0.5)])
    pipeline.run_at.assert_not_called()

    writer.flush_pending_events()  # what TopicSink.close() calls
    assert [call.args[0] for call in pipeline.run_at.call_args_list] == [5.0]


def test_frequency_cadence_unaffected_by_event_wiring(tmp_path: pathlib.Path) -> None:
    when = Frequency(every=2, unit=Unit.SECOND)
    writer, pipeline = _writer(tmp_path, when)

    _feed(writer, [(0.0, -0.5), (1.0, -0.5), (2.5, -0.5)])

    # First message always runs; next only after >= 2s elapsed.
    assert [call.args[0] for call in pipeline.run_at.call_args_list] == [0.0, 2.5]
