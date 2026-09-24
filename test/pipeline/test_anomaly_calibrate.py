"""Tests for the anomaly dry run (`src.pipeline.decide.calibrate`, MCP `preview_anomalies`)."""

import pathlib

import pytest

import server
from src.pipeline.decide import calibrate
from test._fixtures.fault_log import EPOCH, write_fault_log


@pytest.fixture
def log_path(tmp_path: pathlib.Path) -> pathlib.Path:
    return write_fault_log(tmp_path / "robot.mcap")


@pytest.fixture
def drifting_log_path(tmp_path: pathlib.Path) -> pathlib.Path:
    return write_fault_log(tmp_path / "robot.mcap", state_topic=True)


def _run(path: pathlib.Path, **overrides: object) -> dict:
    return calibrate.calibrate(
        str(path),
        window_seconds=10,
        **{"baseline_window_minutes": 5, "warmup_minutes": 1, **overrides},  # type: ignore[arg-type]
    )


def test_reports_the_two_planted_faults_and_nothing_else(log_path: pathlib.Path) -> None:
    report = _run(log_path)
    assert [round(f["offset_seconds"]) for f in report["flagged"]] == [410, 510]
    assert report["windows"] == 61
    assert report["warmup_windows"] == 6  # t=0..50 s: 60 s of history is ready at t=60
    assert report["flagged_fraction"] == pytest.approx(2 / 55)


def test_each_flag_carries_its_screen_reasons(log_path: pathlib.Path) -> None:
    spike, gap = _run(log_path)["flagged"]
    assert {r["kind"] for r in spike["reasons"]} == {"mean_shift", "z_score"}
    assert spike["asof_seconds"] == pytest.approx(EPOCH + 410)
    assert [r["kind"] for r in gap["reasons"]] == ["dropout"]
    assert gap["reasons"][0]["topic"] == "/heartbeat"


def test_counts_flags_per_signal_and_topic(log_path: pathlib.Path) -> None:
    report = _run(log_path)
    assert report["by_signal"] == {"/motor/current.value": 1}
    assert report["by_topic"] == {"/heartbeat": 1}
    assert sorted(report["signals"]) == ["/heartbeat.value", "/motor/current.value"]


def test_a_signal_that_drifts_by_design_is_called_out(drifting_log_path: pathlib.Path) -> None:
    # /odom.value steps to a new level at t=70 s and stays: a state, not a rate. Like a
    # heading after a turn, it sits "far from the baseline mean" in every later window.
    report = _run(drifting_log_path)
    assert report["by_signal"]["/odom.value"] >= 25  # flagged until the 5-min baseline ages out
    (advice,) = [a for a in report["advice"] if "/odom.value" in a]
    assert "drift" in advice and "signals" in advice


def test_no_advice_when_the_screen_is_quiet(log_path: pathlib.Path) -> None:
    assert _run(log_path)["advice"] == []


def test_warns_when_warmup_swallows_the_log(log_path: pathlib.Path) -> None:
    report = _run(log_path, baseline_window_minutes=9, warmup_minutes=9)
    assert report["flagged"] == []
    assert any("warm-up" in a for a in report["advice"])


def test_explicit_signals_are_honoured(log_path: pathlib.Path) -> None:
    report = _run(log_path, signals=["/heartbeat.value"])
    assert report["signals"] == ["/heartbeat.value"]
    assert report["by_signal"] == {}  # the spike is on the unwatched signal
    assert [round(f["offset_seconds"]) for f in report["flagged"]] == [510]


def test_never_calls_a_decision_backend(
    log_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from src.pipeline.decide import backends

    monkeypatch.setattr(backends, "build", lambda *a, **k: pytest.fail("backend built"))
    monkeypatch.delenv("TYPESAFE_API_KEY", raising=False)
    _run(log_path)


def test_mcp_tool_returns_the_same_report(log_path: pathlib.Path) -> None:
    result = server.preview_anomalies(
        str(log_path), window_seconds=10, baseline_window_minutes=5, warmup_minutes=1
    )
    assert result == _run(log_path)


@pytest.mark.parametrize("window", [2.5, 0.5, 0])
def test_window_must_be_a_positive_whole_number_of_seconds(
    log_path: pathlib.Path, window: float
) -> None:
    # Lookbacks are whole seconds; silently truncating 2.5 -> 2 gave results that could
    # not match the request (Codex P2).
    with pytest.raises(ValueError, match="whole"):
        calibrate.calibrate(str(log_path), window_seconds=window)


def test_an_explicit_empty_signal_list_is_preserved(log_path: pathlib.Path) -> None:
    report = _run(log_path, signals=[])
    assert report["signals"] == []
    assert [round(f["offset_seconds"]) for f in report["flagged"]] == [510]


def test_windows_follow_the_cadence_topic_when_given(log_path: pathlib.Path) -> None:
    # The saved pipeline fires on cadence-topic messages, not on a fixed grid (Codex P2).
    # /heartbeat is silent for 505..520 s, so no window can end inside that gap -- and
    # that is why the cadence topic can never be seen dropping out: the preview shows
    # the user exactly what the pipeline would (and would not) catch.
    report = _run(log_path, cadence_topic="/heartbeat")
    ends = [round(f["offset_seconds"], 1) for f in report["flagged"]]
    assert ends == [410.0]
    assert report["cadence_topic"] == "/heartbeat"
    assert all(not (505 < o < 520) for o in report["asof_offsets_seconds"])


def test_mcp_tool_accepts_a_cadence_topic(log_path: pathlib.Path) -> None:
    result = server.preview_anomalies(
        str(log_path),
        window_seconds=10,
        cadence_topic="/motor/current",
        baseline_window_minutes=5,
        warmup_minutes=1,
    )
    assert result["cadence_topic"] == "/motor/current"


def test_cadence_interval_is_separate_from_the_window(log_path: pathlib.Path) -> None:
    # A 10 s lookback on a 60 s cadence evaluates every 60 s, not every 10 s (Codex P2).
    report = _run(log_path, cadence_seconds=60, cadence_topic="/motor/current")
    assert report["asof_offsets_seconds"] == pytest.approx(
        [0, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600]
    )
    assert report["cadence_seconds"] == 60
    assert report["flagged"] == []  # neither 10 s window ending at a 60 s tick holds a fault


def test_cadence_interval_defaults_to_the_window(log_path: pathlib.Path) -> None:
    assert _run(log_path)["cadence_seconds"] == 10


def test_warmup_longer_than_the_baseline_is_rejected(log_path: pathlib.Path) -> None:
    with pytest.raises(ValueError, match="warmup_minutes"):
        _run(log_path, baseline_window_minutes=1, warmup_minutes=5)


def test_a_cadence_shorter_than_the_window_keeps_the_baseline(log_path: pathlib.Path) -> None:
    # 10 s windows every 5 s overlap; the baseline must not reset on each one (Codex P1).
    report = _run(log_path, cadence_seconds=5)
    assert report["warmup_windows"] == 12  # 60 s of history at a 5 s cadence
    assert [round(f["offset_seconds"]) for f in report["flagged"]] == [405, 410, 510, 515]


@pytest.mark.parametrize(
    "bad",
    [
        {"warmup_minutes": -1},
        {"baseline_window_minutes": 0, "warmup_minutes": 0},
        {"z_threshold": 0},
        {"dropout_seconds": 0},
        {"max_signals": 0, "signals": []},
        {"cadence_seconds": 2.5},
    ],
)
def test_preview_rejects_what_the_gate_would_reject(log_path: pathlib.Path, bad: dict) -> None:
    # A preview that accepts settings Pipeline.build refuses is misleading (Codex P2).
    with pytest.raises(ValueError):
        _run(log_path, **bad)


def test_a_one_off_publication_during_warmup_is_not_an_expected_topic(
    tmp_path: pathlib.Path,
) -> None:
    # 10 s windows every second overlap, so one /event message inside warm-up lands in
    # ten windows and would win the majority vote; it must not later count as a
    # permanent dropout (Codex P2).
    from google.protobuf.wrappers_pb2 import DoubleValue
    from mcap_protobuf.writer import Writer as ProtobufWriter

    from test._fixtures.fault_log import EPOCH, SECOND_NS, write_fault_log

    write_fault_log(tmp_path / "base.mcap", duration_seconds=60)
    with open(tmp_path / "oneoff.mcap", "wb") as stream, ProtobufWriter(stream) as writer:
        stamp = int((EPOCH + 5.0) * SECOND_NS)
        writer.write_message("/event", DoubleValue(value=1.0), stamp, stamp)
    report = calibrate.calibrate(
        str(tmp_path),
        window_seconds=10,
        cadence_seconds=1,
        baseline_window_minutes=5,
        warmup_minutes=0.25,
        dropout_seconds=2,
    )
    assert "/event" not in report["by_topic"]
    assert all(r.get("topic") != "/event" for f in report["flagged"] for r in f["reasons"])


@pytest.mark.parametrize(
    "bad",
    [
        {"warmup_minutes": float("nan")},
        {"z_threshold": float("nan")},
        {"cadence_seconds": float("inf")},
    ],
)
def test_preview_rejects_non_finite_settings(log_path: pathlib.Path, bad: dict) -> None:
    with pytest.raises(ValueError):
        _run(log_path, **bad)


@pytest.mark.parametrize("bad", [{"max_signals": float("nan")}, {"max_signals": 2.5}])
def test_preview_max_signals_must_be_a_finite_integer(log_path: pathlib.Path, bad: dict) -> None:
    with pytest.raises(ValueError):
        _run(log_path, **bad)
