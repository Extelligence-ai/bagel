"""End-to-end tests for the MCAP snippet task -- ROS-free, via a protobuf MCAP."""

import pathlib

import pytest
from google.protobuf.wrappers_pb2 import DoubleValue
from mcap.reader import make_reader
from mcap_protobuf.writer import Writer as ProtobufWriter

from bagel.pipeline import base
from bagel.pipeline.tasks.snippet.mcap import SnipMcap
from bagel.settings import settings

EPOCH = 1_700_000_000.0
SECOND_NS = 1_000_000_000

RATE_HZ = 4
DURATION_SECONDS = 12.0
EVENTS = ((4.0, 5.0, -13.0), (8.0, 8.75, -11.5))
PRE_SECONDS, POST_SECONDS = 1.0, 1.0
PREDICATE = "\"/accel\"['value'] < -10"


def _accel_at(offset: float) -> float:
    for start, end, value in EVENTS:
        if start <= offset <= end:
            return value
    return -0.5


@pytest.fixture
def protobuf_mcap(tmp_path: pathlib.Path) -> pathlib.Path:
    path = tmp_path / "flight.mcap"
    with open(path, "wb") as stream, ProtobufWriter(stream) as writer:
        for i in range(int(DURATION_SECONDS * RATE_HZ)):
            offset = i / RATE_HZ
            timestamp_ns = int((EPOCH + offset) * SECOND_NS)
            writer.write_message(
                topic="/accel",
                message=DoubleValue(value=_accel_at(offset)),
                log_time=timestamp_ns,
                publish_time=timestamp_ns,
            )
    return path


@pytest.fixture(autouse=True)
def _isolated_artifacts(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "ARTIFACT_DIRECTORY", str(tmp_path / "artifacts"))


def _read_clip(path: pathlib.Path) -> list[tuple[str, str, float]]:
    """Return (topic, schema name, offset seconds) triples from a clip."""
    with open(path, "rb") as stream:
        return [
            (channel.topic, schema.name, message.log_time / SECOND_NS - EPOCH)
            for schema, channel, message in make_reader(stream).iter_messages()
        ]


def test_on_event_cadence_writes_one_clip_per_event(protobuf_mcap: pathlib.Path) -> None:
    config = {
        "name": "snippet_mcap",
        "site": "test_site",
        "asset": "test_asset",
        "path": str(protobuf_mcap),
        "allow_failure": False,
        "cadence": {
            "topic": "/accel",
            "when": {
                "on_event": {
                    "predicate": PREDICATE,
                    "debounce": {"last": 2, "unit": "second"},
                }
            },
        },
        "tasks": [
            {
                "module": "bagel.pipeline.tasks.snippet.mcap",
                "lookback": {"last": 1, "unit": "second"},
                "args": {"post_seconds": POST_SECONDS},
            }
        ],
    }
    produced = base.Pipeline.build(config).run_all()

    assert len(produced) == len(EVENTS), "one clip per detected event"
    for clip_path, (event_offset, _, _) in zip(sorted(produced), EVENTS, strict=True):
        clip = _read_clip(clip_path)
        assert clip, "clip must not be empty"
        for topic, schema_name, offset in clip:
            assert topic == "/accel"
            assert schema_name == "google.protobuf.DoubleValue", "schema copied verbatim"
            assert event_offset - PRE_SECONDS <= offset <= event_offset + POST_SECONDS + 1e-6


def test_frame_lookback_keeps_last_n_messages(protobuf_mcap: pathlib.Path) -> None:
    task = SnipMcap()
    task.setup(path=str(protobuf_mcap))
    task._pipeline, task._name = "snippet_mcap", "snip_mcap"
    task._site, task._asset, task._log_id = "test_site", "test_asset", "log-1"

    asof = EPOCH + 6.0
    (clip_path,) = task.execute(asof, base.Lookback(last=5, unit=base.Unit.FRAME))

    clip = _read_clip(clip_path)
    assert len(clip) == 5
    assert all(offset <= 6.0 for _, _, offset in clip)
    # The last 5 messages at 4 Hz before t=6 are t = 5.0 .. 6.0.
    assert [offset for _, _, offset in clip] == pytest.approx([5.0, 5.25, 5.5, 5.75, 6.0])


def test_validation_errors() -> None:
    with pytest.raises(ValueError, match="at least one"):
        SnipMcap(topics=[])
    with pytest.raises(ValueError, match="non-negative"):
        SnipMcap(post_seconds=-1.0)
