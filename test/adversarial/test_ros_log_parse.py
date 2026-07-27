"""Adversarial/fuzz tests for the ROS text-log parser (issue #134).

A malformed line must produce a CLEAN outcome from ``parse_line`` /
``parse_file`` -- either a sensible ``LogRecord`` or, for garbage that cannot
be sensibly parsed, ``None`` (``parse_line``'s documented "no known format"
contract) / an empty-ish list -- never an uncontrolled crash (``ValueError``,
``OverflowError``, ``OSError``, ``TypeError``) and never a hang.
"""

import pathlib
import time

import pytest

from src.source.ros.parse import LogRecord, parse_file, parse_line

# ---------------------------------------------------------------------------
# Vector 1: a line that MATCHES the rospy LINE_PATTERNS regex (the only
# pattern that uses datetime.strptime instead of a raw unix_seconds float)
# but carries an out-of-range calendar datetime (month 13, hour 25, etc).
# Before hardening: datetime.strptime(...) raises ValueError, which escapes
# parse_line uncaught.
# ---------------------------------------------------------------------------


def test_rospy_line_with_invalid_month_and_hour_returns_none() -> None:
    line = "[rospy.client][INFO] 2020-13-45 25:99:99,000: msg"

    record = parse_line(line, fallback_node="fallback")

    assert record is None


def test_rospy_line_with_invalid_day_returns_none() -> None:
    line = "[rospy.client][ERROR] 2022-02-30 10:00:00,000: Feb 30th does not exist"

    record = parse_line(line, fallback_node="fallback")

    assert record is None


# ---------------------------------------------------------------------------
# Vector 2: a regex-matching rospy datetime that strptime accepts, but whose
# local-timezone .timestamp() conversion falls outside the platform's
# representable range (year 1 underflows below datetime.MINYEAR once the
# local UTC offset is subtracted; observed to raise
# "ValueError: year 0 is out of range" on this platform, OSError on others).
# ---------------------------------------------------------------------------


def test_rospy_line_with_minimum_year_returns_none_not_crash() -> None:
    line = "[rospy.client][INFO] 0001-01-01 00:00:00,000: tiny year"

    record = parse_line(line, fallback_node="fallback")

    assert record is None


def test_rospy_line_with_maximum_year_stays_a_regression_guard() -> None:
    """Year 9999 is accepted -- already clean, kept as a regression guard."""
    line = "[rospy.client][INFO] 9999-12-31 23:59:59,999: huge year"

    record = parse_line(line, fallback_node="fallback")

    assert record is not None
    assert record.level == "INFO"


# ---------------------------------------------------------------------------
# Vector 3: unix_seconds capture -- the regex only permits \d+(\.\d+)? so
# float() always succeeds; a very large digit run is exercised here as a
# regression guard (already clean, no hardening needed).
# ---------------------------------------------------------------------------


def test_huge_unix_seconds_digit_run_parses_cleanly() -> None:
    line = "[INFO] [" + "9" * 200_000 + ".0] [talker]: msg"

    record = parse_line(line, fallback_node="fallback")

    assert record is not None
    assert record.timestamp_seconds == float("9" * 200_000 + ".0")


# ---------------------------------------------------------------------------
# Vector 4: ReDoS / catastrophic backtracking. Feed long pathological lines
# shaped to stress each LINE_PATTERNS entry and confirm parse_line returns
# quickly rather than hanging. (Characterization found no catastrophic
# backtracking in any pattern -- these are regression guards.)
# ---------------------------------------------------------------------------

_REDOS_TIMEOUT_SECONDS = 2.0

_PATHOLOGICAL_LINES = [
    "[" + "0" * 200_000,
    "[INFO] [" + "9" * 200_000 + ".0] [talker]: msg",
    "1" + "0" * 200_000 + " [INFO] [launch]: msg",
    "[" + " " * 100_000 + "INFO] [1.0] [talker]: msg",
    "[rospy.client]" * 50_000 + "[INFO] 2022-09-05 12:00:00,100: msg",
    "[" * 100_000,
    "]" * 100_000,
    "[INFO] [1.0] [" + "a" * 200_000,
]


@pytest.mark.parametrize("line", _PATHOLOGICAL_LINES)
def test_pathological_lines_do_not_hang(line: str) -> None:
    start = time.monotonic()
    parse_line(line, fallback_node="fallback")
    elapsed = time.monotonic() - start

    assert elapsed < _REDOS_TIMEOUT_SECONDS, (
        f"parse_line took {elapsed:.2f}s on a {len(line)}-char pathological "
        "line -- possible catastrophic backtracking"
    )


# ---------------------------------------------------------------------------
# Vector 5: parse_file robustness -- binary garbage, empty file, a file of
# only unmatched lines, and a real log file with one malformed-timestamp
# line mixed in among otherwise-valid lines.
# ---------------------------------------------------------------------------


def test_parse_file_on_binary_garbage_returns_list_without_raising(
    tmp_path: pathlib.Path,
) -> None:
    garbage = tmp_path / "binary.log"
    garbage.write_bytes(bytes(range(256)) * 100)

    records = parse_file(garbage)

    assert isinstance(records, list)


def test_parse_file_on_empty_file_returns_empty_list(tmp_path: pathlib.Path) -> None:
    empty = tmp_path / "empty.log"
    empty.write_text("")

    records = parse_file(empty)

    assert records == []


def test_parse_file_with_only_unmatched_lines_returns_empty_list(
    tmp_path: pathlib.Path,
) -> None:
    unmatched = tmp_path / "unmatched.log"
    unmatched.write_text("random line 1\nrandom line 2\n")

    records = parse_file(unmatched)

    assert records == []


def test_parse_file_with_malformed_timestamp_mixed_in_does_not_raise(
    tmp_path: pathlib.Path,
) -> None:
    """A file with a valid line, an invalid-datetime line, then another valid
    line must parse without raising. The invalid line is not a recognized
    record, so (per parse_file's documented continuation behavior) it is
    folded into the previous record's message rather than dropped or
    crashing the whole file.
    """
    log = tmp_path / "mixed.log"
    log.write_text(
        "[rospy.client][INFO] 2022-09-05 12:00:00,100: first message\n"
        "[rospy.client][INFO] 2020-13-45 25:99:99,000: bad datetime\n"
        "[rospy.client][INFO] 2022-09-05 12:00:05,200: second message\n"
    )

    records = parse_file(log)

    assert isinstance(records, list)
    assert [r.message.splitlines()[0] for r in records] == [
        "first message",
        "second message",
    ]
    # The unparseable line is folded into the preceding record as a
    # continuation line, matching parse_file's documented behavior for
    # lines that match no known format.
    assert records[0].message == (
        "first message\n[rospy.client][INFO] 2020-13-45 25:99:99,000: bad datetime"
    )


def test_parse_file_with_leading_malformed_timestamp_is_dropped(
    tmp_path: pathlib.Path,
) -> None:
    """A malformed-timestamp line with no preceding record is dropped, same
    as any other unmatched leading line.
    """
    log = tmp_path / "leading_bad.log"
    log.write_text(
        "[rospy.client][INFO] 2020-13-45 25:99:99,000: bad datetime\n"
        "[rospy.client][INFO] 2022-09-05 12:00:00,100: ok message\n"
    )

    records = parse_file(log)

    assert len(records) == 1
    assert records[0].message == "ok message"


# ---------------------------------------------------------------------------
# Sanity: valid rospy lines are unaffected by the hardening.
# ---------------------------------------------------------------------------


def test_valid_rospy_line_still_parses_correctly() -> None:
    record = parse_line(
        "[rospy.client][WARNING] 2022-09-05 12:00:00,100: shutdown request received",
        fallback_node="fallback",
    )

    assert record is not None
    assert isinstance(record, LogRecord)
    assert record.level == "WARN"
    assert record.node == "rospy.client"
    assert record.message == "shutdown request received"
