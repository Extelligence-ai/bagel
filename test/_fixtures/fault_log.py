"""A 10-minute synthetic robot log with two planted faults, shared by the anomaly tests.

`/motor/current` at 10 Hz sits near 1.0 A with a 9.0 A spike at t=401..403 s;
`/heartbeat` at 5 Hz goes silent at t=505..520 s.
"""

import math
import pathlib

from google.protobuf.wrappers_pb2 import DoubleValue
from mcap_protobuf.writer import Writer as ProtobufWriter

EPOCH = 1_700_000_000.0
SECOND_NS = 1_000_000_000
DURATION_SECONDS = 600
SPIKE = (401.0, 403.0)
GAP = (505.0, 520.0)


def current_at(offset: float) -> float:
    """Motor current at `offset` seconds into the log."""
    if SPIKE[0] <= offset <= SPIKE[1]:
        return 9.0
    return 1.0 + 0.05 * math.sin(offset * 3.1)


def write_fault_log(
    path: pathlib.Path, state_topic: bool = False, gap: tuple[float, float] = GAP
) -> pathlib.Path:
    """Write the log to `path`; with `state_topic`, add `/odom` whose `value` is a state that
    steps to a new level at t=70 s and stays there (like a heading after a turn)."""
    with open(path, "wb") as stream, ProtobufWriter(stream) as writer:
        for i in range(DURATION_SECONDS * 10 + 1):
            offset = i / 10
            stamp = int((EPOCH + offset) * SECOND_NS)
            writer.write_message(
                topic="/motor/current",
                message=DoubleValue(value=current_at(offset)),
                log_time=stamp,
                publish_time=stamp,
            )
            if i % 2 == 0 and not (gap[0] <= offset < gap[1]):
                writer.write_message(
                    topic="/heartbeat",
                    message=DoubleValue(value=1.0),
                    log_time=stamp,
                    publish_time=stamp,
                )
            if state_topic:
                writer.write_message(
                    topic="/odom",
                    message=DoubleValue(
                        value=(1.0 if offset >= 70 else 0.0) + 0.001 * math.sin(offset)
                    ),
                    log_time=stamp,
                    publish_time=stamp,
                )
    return path
