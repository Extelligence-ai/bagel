"""Tests for the 60-second hello-world demo (demo.py)."""

import pathlib

import pytest

import demo

CHECK_NAMES = ("Power", "IMU", "GPS", "Data gaps", "Errors")


def _lines_starting_with(card: str, name: str) -> list[str]:
    return [line for line in card.splitlines() if line.startswith(name)]


def test_report_card_against_px4_sample(capsys: pytest.CaptureFixture[str]) -> None:
    if not demo._ecosystem_importable(demo.DataSource.PX4_ULOG):
        pytest.skip("px4 optional dependency group (pyulog) not installed")

    # GIVEN the bundled PX4 sample log

    # WHEN the demo runs against it directly
    exit_code = demo.main([str(demo.PX4_SAMPLE)])
    out = capsys.readouterr().out

    # THEN it succeeds and every check name shows up exactly once
    assert exit_code == 0
    for name in CHECK_NAMES:
        assert len(_lines_starting_with(out, name)) == 1, out

    # AND a VERDICT line closes the card
    assert "VERDICT:" in out

    # AND the power line is a real, live computation (pins the 2.37V drop
    # documented against this sample), not a hardcoded number
    (power_line,) = _lines_starting_with(out, "Power")
    assert "⚠" in power_line
    assert "2.37" in power_line

    # AND GPS is skipped with a reason (this sample carries no GPS topic)
    (gps_line,) = _lines_starting_with(out, "GPS")
    assert "—" in gps_line
    assert "skipped:" in gps_line
    assert "no GPS topic" in gps_line


def test_default_invocation_runs_with_no_arguments(capsys: pytest.CaptureFixture[str]) -> None:
    # GIVEN no path argument at all

    # WHEN the demo runs against whichever bundled sample this environment
    # can parse (PX4 when pyulog is installed, the MCAP sample otherwise)
    exit_code = demo.main([])
    out = capsys.readouterr().out

    # THEN it still produces a full card and never crashes
    assert exit_code == 0
    assert demo.BANNER in out
    for name in CHECK_NAMES:
        assert _lines_starting_with(out, name)
    assert "VERDICT:" in out


def test_default_invocation_falls_back_to_mcap_sample_without_px4(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # GIVEN an environment without the px4 optional dependency group (the
    # flagship ros2-kilted image doesn't install it) -- CI's host-tests job
    # always has pyulog, so this is the only path that actually exercises the
    # dependency-free MCAP fallback described in _default_sample()
    monkeypatch.setattr(demo, "_ecosystem_importable", lambda ds_type: False)

    # WHEN the demo runs with no path argument
    exit_code = demo.main([])
    out = capsys.readouterr().out

    # THEN it explains the fallback and runs the bundled MCAP sample, not PX4
    assert exit_code == 0
    assert "px4 support isn't installed" in out
    assert demo.MCAP_SAMPLE.name in out

    # AND it still produces a full card
    for name in CHECK_NAMES:
        assert _lines_starting_with(out, name)
    assert "VERDICT:" in out


def test_nonexistent_path_is_a_clean_error(
    tmp_path: pathlib.Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # GIVEN a path that doesn't exist
    missing = str(tmp_path / "does-not-exist-12345.ulg")

    # WHEN the demo runs against it
    exit_code = demo.main([missing])
    captured = capsys.readouterr()

    # THEN it fails cleanly, with a message naming the missing path, and no
    # traceback leaks to the user
    assert exit_code == 1
    assert missing in captured.err
    assert "Traceback" not in captured.err
    assert "Traceback" not in captured.out


def test_unsupported_extension_degrades_gracefully(
    tmp_path: pathlib.Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # GIVEN a file in a format this demo doesn't walk through
    unsupported = tmp_path / "reading.csv"
    unsupported.write_text("a,b,c\n1,2,3\n")

    # WHEN the demo runs against it
    exit_code = demo.main([str(unsupported)])
    out = capsys.readouterr().out

    # THEN it degrades with a clear message instead of crashing
    assert exit_code == 1
    assert "format not supported in demo" in out
    assert "the full agent handles it" in out
