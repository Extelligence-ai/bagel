"""compare_logs (#217): two logs in, ranked deltas out, every number an aggregate."""

import csv
import pathlib

import pytest

from bagel_mcp import server

SAMPLE = pathlib.Path("./data/sample/pyarrow/csv/flight.csv")
ARGS = {"timestamp_column": "t", "timestamp_format": "seconds"}


def _variant(tmp_path: pathlib.Path, name: str, scale: float = 1.0, every: int = 1) -> str:
    """A copy of the sample flight with accel_x scaled and every n-th row kept."""
    rows = list(csv.DictReader(SAMPLE.open()))
    out = tmp_path / name
    with out.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, list(rows[0]))
        writer.writeheader()
        for row in rows[::every]:
            writer.writerow({**row, "accel_x": str(float(row["accel_x"]) * scale)})
    return str(out)


def _compare(baseline: str, candidate: str) -> dict:
    return server.compare_logs(baseline, candidate, baseline_args=ARGS, candidate_args=ARGS)


def test_identical_logs_report_no_changes() -> None:
    result = _compare(str(SAMPLE), str(SAMPLE))
    assert result["signals_changed"] == []
    assert result["topics"] == []
    assert result["signals_unchanged"] > 0


def test_a_changed_signal_is_ranked_with_both_logs_stats(tmp_path: pathlib.Path) -> None:
    result = _compare(str(SAMPLE), _variant(tmp_path, "after.csv", scale=1.8))

    changed = {change["signal"]: change for change in result["signals_changed"]}
    assert list(changed) == ["message.accel_x"]
    accel = changed["message.accel_x"]
    assert accel["spread_ratio"] == pytest.approx(1.8)
    assert accel["candidate"]["min"] == pytest.approx(accel["baseline"]["min"] * 1.8)
    assert result["topics"] == []  # same rows, same rate


def test_a_rate_change_is_reported(tmp_path: pathlib.Path) -> None:
    result = _compare(str(SAMPLE), _variant(tmp_path, "sparse.csv", every=3))

    (topic,) = result["topics"]
    assert topic["topic"] == "message"
    before, after = topic["rate_hz"]
    assert after == pytest.approx(before / 3, rel=0.05)
    assert topic["max_gap_seconds"][1] > topic["max_gap_seconds"][0]


def test_unknown_topic_is_rejected() -> None:
    with pytest.raises(ValueError, match="not present in both logs"):
        server.compare_logs(
            str(SAMPLE), str(SAMPLE), topics=["/nope"], baseline_args=ARGS, candidate_args=ARGS
        )


def test_px4_self_compare_has_no_false_changes() -> None:
    pytest.importorskip("pyulog")
    px4 = "./data/sample/px4/sample.ulg"
    offline = {"download_description": False}  # field descriptions need the network
    result = server.compare_logs(px4, px4, baseline_args=offline, candidate_args=offline)
    assert result["topics_compared"] > 50
    assert result["signals_changed_total"] == 0
    assert result["new_warnings_and_errors"] == []
    assert result["log_levels"]["INFO"][0] == result["log_levels"]["INFO"][1] > 0
