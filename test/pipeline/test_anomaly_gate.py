"""End-to-end and behavior tests for the Jev anomaly gate (`src.pipeline.gates.anomaly`).

The log is 10 minutes of a motor-current topic at 10 Hz and a heartbeat topic at 5 Hz,
with two planted faults: a current spike at t=401..403 s and a heartbeat gap at
t=505..520 s. A stand-in Jev server labels whatever the on-robot screen reports.
"""

import json
import math
import pathlib
from unittest.mock import MagicMock, patch

import botocore.exceptions
import pytest
from google.protobuf.wrappers_pb2 import DoubleValue
from mcap.reader import make_reader
from mcap_protobuf.writer import Writer as ProtobufWriter

from settings import settings
from src.pipeline import base
from src.pipeline.gates import anomaly
from test._fixtures.decision_server import DecisionServer, decision_server, jev_reply

server = decision_server
EPOCH = 1_700_000_000.0
SECOND_NS = 1_000_000_000
DURATION_SECONDS = 600
SPIKE = (401.0, 403.0)
GAP = (505.0, 520.0)
ANOMALIES = {
    "overcurrent": "motor current far above normal",
    "sensor_dropout": "a topic stops publishing",
}


def _current(offset: float) -> float:
    if SPIKE[0] <= offset <= SPIKE[1]:
        return 9.0
    return 1.0 + 0.05 * math.sin(offset * 3.1)


@pytest.fixture
def log_path(tmp_path: pathlib.Path) -> pathlib.Path:
    path = tmp_path / "robot.mcap"
    with open(path, "wb") as stream, ProtobufWriter(stream) as writer:
        for i in range(DURATION_SECONDS * 10 + 1):
            offset = i / 10
            stamp = int((EPOCH + offset) * SECOND_NS)
            writer.write_message(
                topic="/motor/current",
                message=DoubleValue(value=_current(offset)),
                log_time=stamp,
                publish_time=stamp,
            )
            if i % 2 == 0 and not (GAP[0] <= offset < GAP[1]):
                writer.write_message(
                    topic="/heartbeat",
                    message=DoubleValue(value=1.0),
                    log_time=stamp,
                    publish_time=stamp,
                )
    return path


@pytest.fixture(autouse=True)
def _jev_key(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    monkeypatch.setenv("TYPESAFE_API_KEY", "ts-key")
    monkeypatch.setattr(settings, "ARTIFACT_DIRECTORY", str(tmp_path / "artifacts"))


def _label_from_screen(body: dict) -> tuple[int, dict]:
    """Stand-in Jev: name the anomaly the screen found, else call it normal."""
    kinds = {reason["kind"] for reason in body["state"]["screen_reasons"]}
    choices = body["questions"]["decision"]["criteria"]
    label = (
        "overcurrent"
        if "z_score" in kinds
        else "sensor_dropout"
        if "dropout" in kinds
        else "normal"
    )
    return jev_reply({c: (0.9 if c == label else 0.1 / (len(choices) - 1)) for c in choices})


def _gate_args(server: DecisionServer, **overrides: object) -> dict:
    return {
        "anomalies": ANOMALIES,
        "url": server.url,
        "baseline_window_minutes": 5,
        "warmup_minutes": 1,
        "timeout_seconds": 2,
        **overrides,
    }


def _pipeline(log_path: pathlib.Path, gate_args: dict, tasks: list[dict]) -> base.Pipeline:
    return base.Pipeline.build(
        {
            "name": "anomaly_upload",
            "site": "warehouse",
            "asset": "forklift",
            "path": str(log_path),
            "allow_failure": False,
            "cadence": {"topic": "/motor/current", "when": {"every": 10, "unit": "second"}},
            "gates": [
                {
                    "module": "src.pipeline.gates.anomaly",
                    "lookback": {"last": 10, "unit": "second"},
                    "args": gate_args,
                }
            ],
            "tasks": tasks,
        }
    )


SNIP_AND_WRITE = [
    {"module": "src.pipeline.tasks.snippet.mcap", "lookback": {"last": 10, "unit": "second"}},
    {"module": "src.pipeline.tasks.write_annotations"},
]


def _offset(path: pathlib.Path) -> float:
    return round(float(path.stem) - EPOCH, 3)


def _records(produced: list[pathlib.Path]) -> dict[float, dict]:
    """The anomaly gate's record per kept window; the file nests it under the gate name."""
    return {
        _offset(p): json.loads(p.read_text())["anomaly"] for p in produced if p.suffix == ".json"
    }


# --- end to end --------------------------------------------------------------------------


def test_only_anomalous_windows_are_sliced_and_labelled(
    log_path: pathlib.Path, server: DecisionServer
) -> None:
    server.reply = _label_from_screen
    produced = _pipeline(log_path, _gate_args(server), SNIP_AND_WRITE).run_all()

    records = _records(produced)
    assert sorted(records) == [410.0, 510.0]
    assert records[410.0]["label"] == "overcurrent"
    assert records[510.0]["label"] == "sensor_dropout"
    slices = sorted(_offset(p) for p in produced if p.suffix == ".mcap")
    assert slices == [410.0, 510.0]


def test_slices_hold_the_anomalous_messages(log_path: pathlib.Path, server: DecisionServer) -> None:
    server.reply = _label_from_screen
    produced = _pipeline(log_path, _gate_args(server), SNIP_AND_WRITE).run_all()
    (spike_slice,) = [p for p in produced if p.suffix == ".mcap" and _offset(p) == 410.0]
    with open(spike_slice, "rb") as stream:
        values = [
            DoubleValue.FromString(message.data).value
            for _, channel, message in make_reader(stream).iter_messages()
            if channel.topic == "/motor/current"
        ]
    assert max(values) == 9.0


def test_annotation_record_explains_the_decision(
    log_path: pathlib.Path, server: DecisionServer
) -> None:
    server.reply = _label_from_screen
    produced = _pipeline(log_path, _gate_args(server), SNIP_AND_WRITE).run_all()
    record = _records(produced)[410.0]
    assert record["verified"] is True
    assert record["beta"] is True
    assert record["mode"] == "screen"
    assert record["model"] == "jev-1.13.0"
    assert record["probabilities"]["overcurrent"] == pytest.approx(0.9)
    kinds = {reason["kind"] for reason in record["screen_reasons"]}
    assert kinds == {"mean_shift", "z_score"}  # a 2 s spike shifts the mean and the max
    (extreme,) = [r for r in record["screen_reasons"] if r["kind"] == "z_score"]
    assert extreme["signal"] == "/motor/current.value"
    assert extreme["value"] == 9.0
    assert record["baseline"]["signals"]["/motor/current.value"]["mean"] == pytest.approx(
        1.0, abs=0.05
    )
    assert record["window"]["end_seconds"] == pytest.approx(EPOCH + 410.0)


def test_jev_is_only_asked_about_screened_windows(
    log_path: pathlib.Path, server: DecisionServer
) -> None:
    server.reply = _label_from_screen
    _pipeline(log_path, _gate_args(server), SNIP_AND_WRITE).run_all()
    assert len(server.requests) == 2


def test_jev_request_carries_named_anomalies_and_the_fallback_choices(
    log_path: pathlib.Path, server: DecisionServer
) -> None:
    server.reply = _label_from_screen
    _pipeline(log_path, _gate_args(server), SNIP_AND_WRITE).run_all()
    body = server.requests[0]["body"]
    assert body["model"] == "jev-latest"
    assert list(body["questions"]["decision"]["criteria"]) == [
        "overcurrent",
        "sensor_dropout",
        "other_unusual",
        "normal",
    ]
    assert set(body["state"]) == {"window", "baseline", "screen_reasons"}


def test_always_mode_asks_jev_about_every_window(
    log_path: pathlib.Path, server: DecisionServer
) -> None:
    server.reply = _label_from_screen
    produced = _pipeline(log_path, _gate_args(server, mode="always"), SNIP_AND_WRITE).run_all()
    assert len(server.requests) == DURATION_SECONDS // 10 + 1
    assert sorted(_records(produced)) == [410.0, 510.0]


def test_windows_jev_calls_normal_are_not_uploaded(
    log_path: pathlib.Path, server: DecisionServer
) -> None:
    server.reply = lambda body: jev_reply(
        {c: (0.97 if c == "normal" else 0.01) for c in body["questions"]["decision"]["criteria"]}
    )
    produced = _pipeline(log_path, _gate_args(server), SNIP_AND_WRITE).run_all()
    assert produced == []


def test_low_confidence_answers_are_not_uploaded(
    log_path: pathlib.Path, server: DecisionServer
) -> None:
    server.reply = lambda body: jev_reply(
        {c: 0.25 for c in body["questions"]["decision"]["criteria"]}
    )
    produced = _pipeline(log_path, _gate_args(server), SNIP_AND_WRITE).run_all()
    assert produced == []


def test_unreachable_jev_uploads_screened_windows_as_screen_only(
    log_path: pathlib.Path, server: DecisionServer
) -> None:
    server.reply = lambda body: (503, {"error": "full"})
    produced = _pipeline(log_path, _gate_args(server), SNIP_AND_WRITE).run_all()
    records = _records(produced)
    assert sorted(records) == [410.0, 510.0]
    assert all(r["label"] == "screen_only" and r["verified"] is False for r in records.values())
    assert all(r["model"] is None for r in records.values())


def test_flagged_windows_do_not_enter_the_baseline(
    log_path: pathlib.Path, server: DecisionServer
) -> None:
    # The spike window is flagged, so the baseline one window later still sits near 1.0.
    server.reply = _label_from_screen
    gate = anomaly.Anomaly(**_gate_args(server))
    gate.setup(path=str(log_path))
    gate._name = "anomaly"
    lookback = base.Lookback(last=10, unit=base.Unit.SECOND)
    for offset in range(0, 421, 10):
        gate.evaluate(EPOCH + offset, lookback)
    mean = gate.baseline.stats(EPOCH + 420)["signals"]["/motor/current.value"]["mean"]
    assert mean == pytest.approx(1.0, abs=0.01)  # a leaked spike window would give ~1.05


def test_nothing_is_screened_during_warmup(log_path: pathlib.Path, server: DecisionServer) -> None:
    server.reply = _label_from_screen
    args = _gate_args(server, warmup_minutes=9)  # warm-up ends after both faults
    produced = _pipeline(log_path, args, SNIP_AND_WRITE).run_all()
    assert produced == []
    assert server.requests == []


def test_upload_sends_slices_and_labels_to_any_bucket(
    log_path: pathlib.Path, server: DecisionServer
) -> None:
    server.reply = _label_from_screen
    client = MagicMock()
    client.get_object_attributes.side_effect = botocore.exceptions.ClientError(
        {"Error": {"Code": "NoSuchKey"}}, "GetObjectAttributes"
    )
    artifacts = pathlib.Path(settings.ARTIFACT_DIRECTORY) / "pipeline=anomaly_upload"
    tasks = [
        *SNIP_AND_WRITE,
        {
            "module": "src.pipeline.tasks.upload.s3",
            "args": {"bucket": "robot-anomalies", "source": str(artifacts), "prefix": "fleet"},
        },
    ]
    with patch("src.pipeline.tasks.upload.s3.boto3.client", return_value=client):
        _pipeline(log_path, _gate_args(server), tasks).run_all()
    # Every passing run re-uploads the artifact directory; the mock never reports a match.
    keys = sorted({call.kwargs["Key"] for call in client.upload_file.call_args_list})
    buckets = {call.kwargs["Bucket"] for call in client.upload_file.call_args_list}
    assert buckets == {"robot-anomalies"}
    assert [k.rsplit(".", 1)[-1] for k in keys].count("mcap") == 2
    assert [k.rsplit(".", 1)[-1] for k in keys].count("json") == 2
    assert all(k.startswith("fleet/") for k in keys)


# --- configuration -----------------------------------------------------------------------


def test_needs_at_least_one_named_anomaly(server: DecisionServer) -> None:
    with pytest.raises(ValueError, match="anomalies"):
        anomaly.Anomaly(**_gate_args(server, anomalies={}))


@pytest.mark.parametrize("reserved", ["normal", "other_unusual", "screen_only"])
def test_reserved_labels_cannot_be_named_anomalies(server: DecisionServer, reserved: str) -> None:
    with pytest.raises(ValueError, match=reserved):
        anomaly.Anomaly(**_gate_args(server, anomalies={reserved: "x"}))


def test_unknown_mode_is_rejected(server: DecisionServer) -> None:
    with pytest.raises(ValueError, match="mode"):
        anomaly.Anomaly(**_gate_args(server, mode="sometimes"))


def test_missing_api_key_fails_when_the_pipeline_is_built(
    log_path: pathlib.Path, server: DecisionServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("TYPESAFE_API_KEY")
    with pytest.raises(ValueError, match="TYPESAFE_API_KEY"):
        _pipeline(log_path, _gate_args(server), SNIP_AND_WRITE)


def test_gate_is_marked_beta_in_the_log(
    server: DecisionServer, caplog: pytest.LogCaptureFixture
) -> None:
    anomaly.Anomaly(**_gate_args(server))
    assert "BETA" in caplog.text


def test_sample_pipeline_builds() -> None:
    import yaml

    config = yaml.safe_load(pathlib.Path("pipelines/anomaly_upload.yaml").read_text())
    pipeline = base.Pipeline.build(config)
    assert pipeline.name == "anomaly_upload"


# --- limits (from pre-release review) ----------------------------------------------------


def test_watching_more_signals_than_max_signals_fails_with_guidance(
    log_path: pathlib.Path, server: DecisionServer
) -> None:
    # PX4 exposes ~2000 numeric fields; sending them all to Jev blows its 64k context.
    gate = anomaly.Anomaly(**_gate_args(server, max_signals=1))
    gate.setup(path=str(log_path))
    gate._name = "anomaly"
    with pytest.raises(ValueError, match="max_signals"):
        gate.evaluate(EPOCH + 10, base.Lookback(last=10, unit=base.Unit.SECOND))


def test_lookback_must_be_time_based(log_path: pathlib.Path, server: DecisionServer) -> None:
    gate = anomaly.Anomaly(**_gate_args(server))
    gate.setup(path=str(log_path))
    gate._name = "anomaly"
    with pytest.raises(ValueError, match="lookback"):
        gate.evaluate(EPOCH + 10, None)
    with pytest.raises(ValueError, match="lookback"):
        gate.evaluate(EPOCH + 10, base.Lookback(last=100, unit=base.Unit.FRAME))


def test_state_sent_to_jev_is_small(log_path: pathlib.Path, server: DecisionServer) -> None:
    server.reply = _label_from_screen
    _pipeline(log_path, _gate_args(server), SNIP_AND_WRITE).run_all()
    assert all(len(json.dumps(r["body"]["state"])) < 8_000 for r in server.requests)


def test_live_sources_are_rejected_in_beta(
    server: DecisionServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The Jev call is synchronous; on a live ingest thread it would stall every topic.
    from src.pipeline import messages

    live = MagicMock()
    live.factory = object()  # anything but a BoundedSourceFactory
    monkeypatch.setattr(messages.SourceContext, "build", staticmethod(lambda path, kwargs: live))
    gate = anomaly.Anomaly(**_gate_args(server))
    with pytest.raises(ValueError, match="batch-only"):
        gate.setup(path="live://imu")


def test_label_file_nests_each_gate_under_its_name(
    log_path: pathlib.Path, server: DecisionServer
) -> None:
    server.reply = _label_from_screen
    produced = _pipeline(log_path, _gate_args(server), SNIP_AND_WRITE).run_all()
    payload = json.loads(next(p for p in produced if p.suffix == ".json").read_text())
    assert set(payload) == {"asof_seconds", "anomaly"}
    assert payload["anomaly"]["beta"] is True


def test_dropout_threshold_longer_than_the_window_waits_for_it(
    tmp_path: pathlib.Path, server: DecisionServer
) -> None:
    # 10 s windows, a 25 s heartbeat gap (505..530) and dropout_seconds=12: the window
    # ending at 510 has seen 5 s of silence (not yet a dropout, Codex P2); the one ending
    # at 520 has seen 15 s, measured from the last heartbeat at 505.
    from test._fixtures.fault_log import write_fault_log

    log = write_fault_log(tmp_path / "gap.mcap", gap=(505.0, 530.0))
    server.reply = _label_from_screen
    produced = _pipeline(log, _gate_args(server, dropout_seconds=12), SNIP_AND_WRITE).run_all()
    assert sorted(_records(produced)) == [410.0, 520.0]
    (reason,) = _records(produced)[520.0]["screen_reasons"]
    # the last heartbeat before the gap is at 504.8 s (5 Hz)
    assert reason["kind"] == "dropout" and reason["silent_seconds"] == pytest.approx(15.2, abs=0.01)


def test_an_all_zero_jev_reply_falls_back_to_screen_only(
    log_path: pathlib.Path, server: DecisionServer
) -> None:
    server.reply = lambda body: jev_reply(
        {c: 0.0 for c in body["questions"]["decision"]["criteria"]}
    )
    produced = _pipeline(log_path, _gate_args(server), SNIP_AND_WRITE).run_all()
    records = _records(produced)
    assert sorted(records) == [410.0, 510.0]
    assert all(r["label"] == "screen_only" for r in records.values())


def test_an_explicit_empty_signal_list_watches_only_dropouts(
    log_path: pathlib.Path, server: DecisionServer
) -> None:
    # `signals: []` is a choice, not an omission (Codex P2): no mean-shift screening,
    # so only the heartbeat gap is kept.
    server.reply = _label_from_screen
    produced = _pipeline(log_path, _gate_args(server, signals=[]), SNIP_AND_WRITE).run_all()
    assert sorted(_records(produced)) == [510.0]
