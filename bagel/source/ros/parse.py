"""Parse plain-text log files dumped by ROS (e.g., under ~/.ros/log).

ROS writes several line formats depending on the client library and log file:

- ROS 2 rcutils (per-node files and console output):
  ``[INFO] [1662400000.100000000] [talker]: Publishing: 'Hello World: 1'``
- ROS 2 launch (``launch.log``):
  ``1662400000.0405089 [INFO] [launch]: process started with pid [123]``
- ROS 1 ``rosout.log``:
  ``1662400001.123456789 INFO /rosout [rosout.cpp:100(main)] [topics: /rosout] started``
- ROS 1 roscpp per-node files (no node name in the line; the file name carries it):
  ``[ INFO] [1662400000.100000000]: waiting for service``
- ROS 1 rospy per-node files (wall-clock datetime, Python logging levels):
  ``[rospy.client][INFO] 2022-09-05 12:00:00,100: init_node, name[/talker]``

Lines that match none of the formats are treated as continuations of the previous
record (multi-line messages such as tracebacks); leading continuations with no
previous record are dropped.
"""

import datetime
import pathlib
import re
from dataclasses import dataclass
from typing import Final

# Python logging level names (rospy) mapped to ROS severities.
_LEVEL_ALIASES: Final = {"WARNING": "WARN", "CRITICAL": "FATAL", "ERR": "ERROR"}

_LEVEL = r"DEBUG|INFO|WARN(?:ING)?|ERR(?:OR)?|FATAL|CRITICAL"

# Each pattern must define the named groups "level" and "message", plus either
# "unix_seconds" or "datetime", and optionally "node".
LINE_PATTERNS: Final[tuple[re.Pattern, ...]] = (
    # ROS 2 rcutils: [INFO] [1662400000.100000000] [talker]: message
    re.compile(
        rf"^\[\s*(?P<level>{_LEVEL})\]\s+\[(?P<unix_seconds>\d+(?:\.\d+)?)\]\s+"
        r"\[(?P<node>[^\]]+)\]:\s?(?P<message>.*)$"
    ),
    # ROS 2 launch.log: 1662400000.0405089 [INFO] [launch]: message
    re.compile(
        rf"^(?P<unix_seconds>\d+(?:\.\d+)?)\s+\[\s*(?P<level>{_LEVEL})\]\s+"
        r"\[(?P<node>[^\]]+)\](?::\s?|\s)(?P<message>.*)$"
    ),
    # ROS 1 rosout.log: 1662400001.123456789 INFO /node [file:line(func)] [topics: ...] message
    re.compile(
        rf"^(?P<unix_seconds>\d+(?:\.\d+)?)\s+(?P<level>{_LEVEL})\s+(?P<node>\S+)\s+"
        r"\[[^\]]*\]\s+\[topics:[^\]]*\]\s?(?P<message>.*)$"
    ),
    # ROS 1 roscpp per-node file: [ INFO] [1662400000.100000000]: message
    re.compile(
        rf"^\[\s*(?P<level>{_LEVEL})\]\s+\[(?P<unix_seconds>\d+(?:\.\d+)?)"
        r"(?:,\s*\d+(?:\.\d+)?)?\]:\s?(?P<message>.*)$"
    ),
    # ROS 1 rospy per-node file: [rospy.client][INFO] 2022-09-05 12:00:00,100: message
    re.compile(
        rf"^\[(?P<node>[\w./]+)\]\[(?P<level>{_LEVEL})\]\s+"
        r"(?P<datetime>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}):\s?(?P<message>.*)$"
    ),
)


@dataclass
class LogRecord:
    """One parsed log entry, possibly spanning multiple lines."""

    timestamp_seconds: float
    level: str
    node: str
    message: str
    file: str


def parse_line(line: str, fallback_node: str) -> LogRecord | None:
    """Parse one log line; return None if it matches no known ROS log format.

    Args:
        line (str): The raw log line without the trailing newline.
        fallback_node (str): Node name to use for formats that do not carry one
            (typically the log file's stem).

    """
    for pattern in LINE_PATTERNS:
        match = pattern.match(line)
        if not match:
            continue
        groups = match.groupdict()
        if groups.get("unix_seconds") is not None:
            timestamp_seconds = float(groups["unix_seconds"])
        else:
            # rospy writes local wall-clock time with no timezone; interpret it
            # in the machine's local timezone, mirroring how it was written.
            timestamp_seconds = datetime.datetime.strptime(
                groups["datetime"], "%Y-%m-%d %H:%M:%S,%f"
            ).timestamp()
        level = groups["level"].upper()
        return LogRecord(
            timestamp_seconds=timestamp_seconds,
            level=_LEVEL_ALIASES.get(level, level),
            node=groups.get("node") or fallback_node,
            message=groups["message"],
            file="",
        )
    return None


def parse_file(path: str | pathlib.Path) -> list[LogRecord]:
    """Parse a ROS text log file into records, in file order.

    Unmatched lines are appended to the previous record's message (multi-line
    messages such as tracebacks); unmatched lines before the first record are
    dropped.
    """
    path = pathlib.Path(path)
    records: list[LogRecord] = []
    for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.rstrip()
        if not line:
            continue
        record = parse_line(line, fallback_node=path.stem)
        if record is not None:
            record.file = path.name
            records.append(record)
        elif records:
            records[-1].message += "\n" + line
    return records


def looks_like_ros_log(path: pathlib.Path, sample_lines: int = 50) -> bool:
    """Check whether any of the file's first lines match a known ROS log format."""
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            for _, line in zip(range(sample_lines), f, strict=False):
                if parse_line(line.rstrip(), fallback_node="") is not None:
                    return True
    except OSError:
        return False
    return False
