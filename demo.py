"""Bagel's 60-second hello world: a headless robot-health report card.

Zero MCP client, zero LLM, zero config: ``python demo.py [path]`` runs the
same deterministic checks documented in
``src/agent/diagnose/robot_health.poml`` directly against the describe/query
primitives that back ``server.py``'s MCP tools (``module.provide`` ->
``SourceFactory`` / ``TopicRegistry`` / ``MessageDataset`` /
``LoggingDataset``) -- never through the MCP or LLM layer, and never printing
a number that wasn't actually computed from the log's own messages.

Usage:
    python demo.py                 # bundled sample log
    python demo.py /path/to/log    # your own log
"""

from __future__ import annotations

import argparse
import dataclasses
import importlib
import itertools
import pathlib
import statistics
import sys
from typing import Any

import duckdb
import pandas as pd

from settings import settings
from src.di import module
from src.di.types.base_module import BaseModule
from src.di.types.data_source import DataSource, resolve
from src.logging.base import NoLoggingTopicsFoundError

REPO_ROOT = pathlib.Path(__file__).resolve().parent
TIMESTAMP_COL = settings.TIMESTAMP_SECONDS_COLUMN_NAME

PX4_SAMPLE = REPO_ROOT / "data" / "sample" / "px4" / "sample.ulg"
MCAP_SAMPLE = REPO_ROOT / "data" / "sample" / "ros2" / "mcap"

BANNER = "Bagel gives AI agents deterministic tools for robot data — here's a taste, no setup."

UPSELL = (
    "Your own log:     docker run --rm -it "
    "-v /path/to/your/logs:/home/ubuntu/data "
    "ghcr.io/extelligence-ai/bagel/ros2-kilted:latest demo /home/ubuntu/data/your-log.mcap\n"
    "Full experience:  <your MCP client> mcp add --transport sse bagel "
    "http://localhost:8000/sse  (README has the exact command per client)"
)

# Ecosystems this demo walks through end to end. ArduPilot (.bin) is
# deliberately NOT here: src/topic/ardupilot/bin.py's TopicRegistry.struct()
# unconditionally calls pymavlink's DFMetaData.download() with no opt-out
# (unlike PX4's download_description=False below), so querying even one
# ArduPilot field makes a real network call -- that breaks this demo's "zero
# network calls" rule, so .bin degrades to the same clear message as
# everything else this demo doesn't walk through (Betaflight, automotive
# MF4/CAN, ROS1 bags and ROS2 db3 without a full ROS install, waffleform,
# CSV/JSON, ...): the full agent handles it, this demo doesn't try to.
SUPPORTED_DS_TYPES = {
    DataSource.PX4_ULOG,
    DataSource.MCAP,
    DataSource.ROS1_BAG,
    DataSource.ROS2_DB3,
}
ROS_DS_TYPES = {DataSource.ROS1_BAG, DataSource.ROS2_DB3, DataSource.MCAP}

# Per-ecosystem topic-*name* hints, mirrored from the documented skeleton in
# src/agent/diagnose/robot_health.poml -- treated as hints, not requirements:
# matched by name/prefix first, and only ruled out if none matches.
TOPIC_NAME_HINTS: dict[str, dict[str, list[str]]] = {
    DataSource.PX4_ULOG.value: {
        "power": ["battery_status"],
        "imu": ["sensor_combined", "sensor_accel"],
        "gps": ["vehicle_gps_position"],
    },
}

# ROS 1/2 (bag/db3/mcap): a topic's *type* carries the semantics, names
# don't -- match on the type name's last path component (see robot_health.poml).
ROS_TYPE_HINTS: dict[str, str] = {
    "power": "BatteryState",
    "imu": "Imu",
    "gps": "NavSatFix",
    "status": "Log",
}

# The one field this demo reads per check, by ecosystem and matched topic
# prefix. ROS entries are dotted paths into the nested message struct.
POWER_FIELD: dict[str, dict[str, str]] = {
    DataSource.PX4_ULOG.value: {"battery_status": "voltage_v"},
}
ROS_POWER_FIELD = ["voltage"]

IMU_FIELD: dict[str, dict[str, str]] = {
    DataSource.PX4_ULOG.value: {
        "sensor_combined": "accelerometer_m_s2[2]",
        "sensor_accel": "z",
    },
}
ROS_IMU_FIELD = ["linear_acceleration", "z"]

GPS_FIELD: dict[str, dict[str, str]] = {
    DataSource.PX4_ULOG.value: {"vehicle_gps_position": "fix_type"},
}
ROS_GPS_FIELD = ["status", "status"]
GPS_GOOD_FIX_THRESHOLD: dict[str, float] = {
    DataSource.PX4_ULOG.value: 3,  # fix_type >= 3 is a 3D fix
    "ros": 0,  # NavSatFix.status.status >= 0 is a fix (STATUS_NO_FIX == -1)
}

# PX4's syslog-style severity names, bucketed into the report card's three
# buckets. See pyulog's ULog.Logging.MessageLogging.log_level_str().
PX4_ERROR_LEVELS = {"EMERGENCY", "ALERT", "CRITICAL", "ERROR"}
PX4_WARN_LEVELS = {"WARNING", "NOTICE"}

# rcl_interfaces/msg/Log severity thresholds (ROS2 logging levels).
ROS_LOG_ERROR_LEVEL = 40
ROS_LOG_WARN_LEVEL = 30

# rosgraph_msgs/Log severity thresholds (ROS1 logging levels: DEBUG=1, INFO=2,
# WARN=4, ERROR=8, FATAL=16 -- a disjoint scale from ROS2's, so a ROS1 bag
# must not be classified with the ROS2 thresholds above).
ROS1_LOG_ERROR_LEVEL = 8
ROS1_LOG_WARN_LEVEL = 4

WARN_NUMEROUS_THRESHOLD = 3
POWER_MIN_SAMPLES = 2
GAP_MIN_TIMESTAMPS = 3  # need >= 2 gaps to have a median
POWER_WARN_DROP_RATIO = 3.0
POWER_ERROR_DROP_RATIO = 5.0
POWER_ERROR_RECOVERY_MARGIN = 1.05
IMU_WARN_RATIO = 3.0
IMU_ERROR_RATIO = 6.0
IMU_MIN_SAMPLES = 20
GPS_WARN_FRACTION = 0.9
GPS_ERROR_FRACTION = 0.5
GAP_WARN_RATIO = 3.0
GAP_ERROR_RATIO = 8.0

ICON_OK = "✅"
ICON_WARN = "⚠️"
ICON_ERROR = "❌"
ICON_SKIP = "—"
SEVERITY_RANK = {ICON_ERROR: 0, ICON_WARN: 1, ICON_OK: 2}

CHECK_NAME_WIDTH = 11


class DemoPathNotFoundError(Exception):
    """Raised when the user-supplied path doesn't exist."""


class DemoUnsupportedFormatError(Exception):
    """Raised when the path is a format this demo doesn't walk through."""


@dataclasses.dataclass
class CheckResult:
    """One line of the report card."""

    name: str
    icon: str
    detail: str
    timestamp: float | None = None
    verdict_hint: str | None = None

    def render(self) -> str:
        """Render this result as one report-card line."""
        return f"{self.name:<{CHECK_NAME_WIDTH}}{self.icon}  {self.detail}"


@dataclasses.dataclass
class Context:
    """Everything a check function needs, gathered once per report."""

    ds_type: DataSource
    factory: Any
    registry: Any
    dataset: Any
    logging_dataset: Any
    data_source: Any
    topics: list[str]
    start_seconds: float
    queried_topics: list[str] = dataclasses.field(default_factory=list)

    @property
    def ds_key(self) -> str:
        """Return the DataSource enum's string value, used as a dict key."""
        return self.ds_type.value


def _ecosystem_importable(ds_type: DataSource) -> bool:
    """Return whether the optional dependency behind an ecosystem is installed."""
    try:
        importlib.import_module(f"{BaseModule.SOURCE_FACTORY.value}.{ds_type.value}")
    except (ImportError, ModuleNotFoundError):
        return False
    return True


def _default_sample() -> pathlib.Path:
    """Pick the bundled sample this environment can actually parse.

    PX4's .ulg parsing needs the ``px4`` optional dependency group
    (``pyulog``), which the flagship ``ros2-kilted`` image doesn't install
    (it only syncs the ``ros2`` group). Prefer the richer PX4 walkthrough
    when it's available (e.g. ``uv sync --group px4``, or CI's host-tests
    job); otherwise fall back to the bundled MCAP sample, which needs no
    optional dependency at all.
    """
    if _ecosystem_importable(DataSource.PX4_ULOG):
        return PX4_SAMPLE
    print(
        "(px4 support isn't installed in this image -- showing the bundled "
        "ROS 2 MCAP sample instead. `uv sync --group px4` or "
        "`demo data/sample/px4/sample.ulg` on an image that has pyulog "
        "gets the full PX4 walkthrough.)\n"
    )
    return MCAP_SAMPLE


def _find_topic_by_name(topics: list[str], hints: list[str]) -> str | None:
    """Return the first topic matching a name hint (exact, or ``hint_<id>``)."""
    for hint in hints:
        matches = sorted(t for t in topics if t == hint or t.startswith(f"{hint}_"))
        if matches:
            return matches[0]
    return None


def _find_topic_by_type(ctx: Context, type_suffix: str) -> str | None:
    """Return the first topic whose native type's last segment matches."""
    matches = []
    for topic in ctx.topics:
        try:
            native = ctx.registry.native_type_name(topic, ctx.data_source)
        except Exception:  # noqa: S112 -- a broken topic just isn't a match
            continue
        if native.rsplit("/", 1)[-1] == type_suffix:
            matches.append(topic)
    if not matches:
        return None
    if type_suffix == "Log":
        rosout = [t for t in matches if t.endswith("rosout")]
        if rosout:
            return sorted(rosout)[0]
    return sorted(matches)[0]


def _field_for(field_map: dict[str, dict[str, str]], ds_key: str, topic: str) -> str | None:
    """Return the field name registered for the topic's matched prefix."""
    for prefix, field in field_map.get(ds_key, {}).items():
        if topic == prefix or topic.startswith(f"{prefix}_"):
            return field
    return None


def _locate(
    ctx: Context, kind: str, name_field_map: dict[str, dict[str, str]], ros_field: list[str]
) -> tuple[str, list[str]] | tuple[None, None]:
    """Find the topic and field path for one check kind ("power"/"imu"/"gps")."""
    if ctx.ds_key in TOPIC_NAME_HINTS:
        topic = _find_topic_by_name(ctx.topics, TOPIC_NAME_HINTS[ctx.ds_key].get(kind, []))
        if topic is None:
            return None, None
        field = _field_for(name_field_map, ctx.ds_key, topic)
        return (topic, [field]) if field else (None, None)
    if ctx.ds_type in ROS_DS_TYPES:
        topic = _find_topic_by_type(ctx, ROS_TYPE_HINTS[kind])
        return (topic, ros_field) if topic else (None, None)
    return None, None


def _quote_ident(name: str) -> str:
    """Return ``name`` as a double-quoted DuckDB identifier, escaped for embedded quotes.

    Topic names come from the log's own topic registry, not a fixed schema, so
    an unusual (or crafted) topic name could otherwise break out of a naive
    ``f'"{name}"'`` interpolation and alter the statement.
    """
    return '"' + name.replace('"', '""') + '"'


def _field_expr(topic: str, field_path: list[str]) -> str:
    """Return a quoted DuckDB dotted-path expression into a topic's struct column."""
    quoted = ".".join(_quote_ident(part) for part in field_path)
    return f"{_quote_ident(topic)}.{quoted}"


def _query_field(ctx: Context, topic: str, field_path: list[str]) -> pd.DataFrame:
    """Return a (t, value) dataframe for one field of one topic, t relative to log start."""
    relation = ctx.dataset.to_duckdb(ctx.factory, ctx.registry, [topic])
    duckdb.register(topic, relation)
    expr = _field_expr(topic, field_path)
    # expr/topic are escaped identifiers (see _quote_ident); TIMESTAMP_COL is
    # from settings, never raw user input.
    sql = (
        f"SELECT {TIMESTAMP_COL} AS ts, {expr} AS value "  # noqa: S608
        f"FROM {_quote_ident(topic)} ORDER BY {TIMESTAMP_COL}"
    )
    df = duckdb.sql(sql).df()
    df["t"] = df["ts"] - ctx.start_seconds
    df["value"] = df["value"].astype(float)
    return df


def _median_dt_ratio(timestamps: list[float]) -> tuple[float, float, float]:
    """Return (median inter-message dt, worst dt, worst/median ratio).

    Zero-valued gaps (same-tick duplicate timestamps, common in high-rate
    binary logs like ArduPilot's IMU) are dropped before taking the median so
    a topic logged faster than its own clock resolution doesn't collapse the
    "typical interval" to zero and blow the ratio up to infinity; the worst
    gap is still measured over every gap, zero included.
    """
    all_diffs = [b - a for a, b in itertools.pairwise(timestamps)]
    positive_diffs = [d for d in all_diffs if d > 0] or [0.0]
    median = statistics.median(positive_diffs)
    worst = max(all_diffs)
    ratio = worst / median if median > 0 else 1.0
    return median, worst, ratio


def check_power(ctx: Context) -> CheckResult:
    """Min/max/end voltage and the largest single-sample drop."""
    topic, field_path = _locate(ctx, "power", POWER_FIELD, ROS_POWER_FIELD)
    if topic is None or field_path is None:
        return CheckResult("Power", ICON_SKIP, "skipped: no power/battery topic")

    try:
        df = _query_field(ctx, topic, field_path)
    except Exception:  # unexpected field shape, don't crash the card
        return CheckResult(
            "Power", ICON_SKIP, f"skipped: found {topic} but couldn't read a voltage field"
        )
    # Unknown-voltage samples (e.g. a ROS BatteryState reporting NaN) don't
    # count as data: an all-NaN series would otherwise reach idxmax() below
    # and crash, and partial NaNs would silently print "nanV".
    df = df.dropna(subset=["value"])
    if len(df) < POWER_MIN_SAMPLES:
        return CheckResult("Power", ICON_SKIP, f"skipped: {topic} has too few samples")

    ctx.queried_topics.append(topic)
    values = df["value"]
    min_v, end_v = values.min(), values.iloc[-1]
    diffs = values.shift(1) - values
    worst_idx = diffs.idxmax()
    worst_drop, worst_t = diffs.loc[worst_idx], df.loc[worst_idx, "t"]
    median_abs_diff = values.diff().abs().median() or 1e-9
    drop_ratio = worst_drop / median_abs_diff

    if drop_ratio > POWER_ERROR_DROP_RATIO and end_v <= min_v * POWER_ERROR_RECOVERY_MARGIN:
        icon, hint = ICON_ERROR, "replace or bench-test the pack -- it never recovered"
    elif drop_ratio > POWER_WARN_DROP_RATIO:
        icon, hint = ICON_WARN, "bench-check the pack under load"
    else:
        icon, hint = ICON_OK, None

    detail = (
        f"min {min_v:.2f}V, largest drop {worst_drop:.2f}V at ~t=+{worst_t:.1f}s, "
        f"end {end_v:.2f}V ({topic})"
    )
    return CheckResult("Power", icon, detail, timestamp=worst_t, verdict_hint=hint)


def check_imu(ctx: Context) -> CheckResult:
    """Accel-z stddev in the worst ~1s window vs. the whole log's baseline."""
    topic, field_path = _locate(ctx, "imu", IMU_FIELD, ROS_IMU_FIELD)
    if topic is None or field_path is None:
        return CheckResult("IMU", ICON_SKIP, "skipped: no IMU topic")

    try:
        df = _query_field(ctx, topic, field_path)
    except Exception:  # unexpected field shape, don't crash the card
        return CheckResult(
            "IMU", ICON_SKIP, f"skipped: found {topic} but couldn't read an accel/gyro field"
        )
    # Same NaN-drop rationale as check_power: unknown readings aren't samples.
    df = df.dropna(subset=["value"])
    if len(df) < IMU_MIN_SAMPLES:
        return CheckResult("IMU", ICON_SKIP, f"skipped: {topic} has too few samples to window")

    ctx.queried_topics.append(topic)
    values = df["value"]
    baseline_std = values.std()
    if not baseline_std:
        return CheckResult("IMU", ICON_OK, f"accel_z is flat across the log ({topic})")

    median_dt = df["t"].diff().median()
    window = max(IMU_MIN_SAMPLES, round(1.0 / median_dt)) if median_dt > 0 else IMU_MIN_SAMPLES
    # A high-rate IMU with < ~1s of total data can push the ~1s window past
    # the sample count entirely; cap it so rolling() always has a real
    # min_periods worth of data to produce a non-NaN result. len(values) is
    # always >= IMU_MIN_SAMPLES here (checked above), so the cap never drops
    # below IMU_MIN_SAMPLES.
    window = min(window, len(values))
    rolling = values.rolling(window).std()
    peak_idx = rolling.idxmax()
    ratio = rolling.max() / baseline_std
    peak_t = df.loc[peak_idx, "t"]

    if ratio >= IMU_ERROR_RATIO:
        icon, hint = ICON_ERROR, f"investigate the vibration spike at ~t=+{peak_t:.1f}s"
    elif ratio >= IMU_WARN_RATIO:
        icon, hint = ICON_WARN, f"look at the vibration window at ~t=+{peak_t:.1f}s"
    else:
        icon, hint = ICON_OK, None

    detail = f"accel_z stddev {ratio:.1f}x the log baseline at ~t=+{peak_t:.1f}s ({topic})"
    return CheckResult("IMU", icon, detail, timestamp=peak_t, verdict_hint=hint)


def check_gps(ctx: Context) -> CheckResult:
    """Fraction of samples at a good fix."""
    topic, field_path = _locate(ctx, "gps", GPS_FIELD, ROS_GPS_FIELD)
    if topic is None or field_path is None:
        return CheckResult("GPS", ICON_SKIP, "skipped: no GPS topic")

    try:
        df = _query_field(ctx, topic, field_path)
    except Exception:  # unexpected field shape, don't crash the card
        return CheckResult(
            "GPS", ICON_SKIP, f"skipped: found {topic} but couldn't read a fix-quality field"
        )
    # A missing/NaN fix-quality reading compares false against the threshold
    # either way, which would silently count an unavailable measurement as a
    # poor fix; drop it instead so only real readings feed the fraction.
    df = df.dropna(subset=["value"])
    if df.empty:
        return CheckResult("GPS", ICON_SKIP, f"skipped: {topic} has no valid fix-quality samples")

    ctx.queried_topics.append(topic)
    threshold = GPS_GOOD_FIX_THRESHOLD.get(ctx.ds_key, GPS_GOOD_FIX_THRESHOLD["ros"])
    good_fraction = (df["value"] >= threshold).mean()

    if good_fraction < GPS_ERROR_FRACTION:
        icon, hint = ICON_ERROR, "check the GPS antenna/placement -- most of the log has a poor fix"
    elif good_fraction < GPS_WARN_FRACTION:
        icon, hint = ICON_WARN, "spot-check the low-fix stretches in the GPS log"
    else:
        icon, hint = ICON_OK, None

    detail = f"{good_fraction * 100:.0f}% of samples at a good fix ({topic})"
    return CheckResult("GPS", icon, detail, verdict_hint=hint)


def _busiest_topic(ctx: Context) -> list[str]:
    """Return the log's single highest-message-count topic, as a one-item list."""
    counts = []
    for topic in ctx.topics:
        try:
            counts.append((ctx.registry.message_count(topic, ctx.data_source) or 0, topic))
        except Exception:  # noqa: S112 -- one bad topic must not abort the check
            continue
    return [max(counts)[1]] if counts else []


def _worst_gap(ctx: Context, topics: list[str]) -> tuple[float, str | None, list[str]]:
    """Return (worst ratio, its topic, topics actually checked) across ``topics``."""
    worst_ratio, worst_topic, checked = 0.0, None, []
    for topic in topics:
        try:
            relation = ctx.dataset.to_duckdb(ctx.factory, ctx.registry, [topic])
            timestamps = sorted(relation.to_df()[TIMESTAMP_COL].tolist())
        except Exception:  # noqa: S112 -- one bad topic must not abort the check
            continue
        if len(timestamps) < GAP_MIN_TIMESTAMPS:
            continue
        _, _, ratio = _median_dt_ratio(timestamps)
        checked.append(topic)
        if ratio > worst_ratio:
            worst_ratio, worst_topic = ratio, topic
    return worst_ratio, worst_topic, checked


def check_data_gaps(ctx: Context) -> CheckResult:
    """Worst inter-message gap, as a multiple of the topic's own median."""
    # Reuse whatever Power/IMU/GPS already queried; robot_health.poml also
    # allows "any other topic central to the log", so fall back to the
    # log's busiest topic when nothing else matched.
    topics = list(dict.fromkeys(ctx.queried_topics)) or _busiest_topic(ctx)
    if not topics:
        return CheckResult("Data gaps", ICON_SKIP, "skipped: no topic with enough messages")

    worst_ratio, worst_topic, checked = _worst_gap(ctx, topics)
    if not checked:
        return CheckResult("Data gaps", ICON_SKIP, "skipped: not enough messages to time gaps")

    if worst_ratio > GAP_ERROR_RATIO:
        icon, hint = ICON_ERROR, f"track down the gap on {worst_topic}"
    elif worst_ratio > GAP_WARN_RATIO:
        icon, hint = ICON_WARN, f"keep an eye on the gap on {worst_topic}"
    else:
        icon, hint = ICON_OK, None

    detail = f"no gap > {worst_ratio:.2f}x median interval (checked {', '.join(checked)})"
    return CheckResult("Data gaps", icon, detail, verdict_hint=hint)


def _px4_log_entries(df: pd.DataFrame) -> list[tuple[float, str, str]]:
    """Bucket PX4's syslog-style ``log_level`` names into ERROR/WARN/INFO."""
    entries = []
    for _, row in df.iterrows():
        level = row["message"]["log_level"]
        if level in PX4_ERROR_LEVELS:
            severity = "ERROR"
        elif level in PX4_WARN_LEVELS:
            severity = "WARN"
        else:
            severity = "INFO"
        entries.append((row[TIMESTAMP_COL], severity, row["message"]["message"]))
    return entries


def _ros_log_entries(ds_type: DataSource, df: pd.DataFrame) -> list[tuple[float, str, str]]:
    """Bucket a ROS Log topic's numeric ``level`` into ERROR/WARN/INFO.

    ROS1 (`rosgraph_msgs/Log`) and ROS2 (`rcl_interfaces/msg/Log`) use
    disjoint numeric level scales, so the threshold is picked by ``ds_type``.
    """
    error_level, warn_level = (
        (ROS1_LOG_ERROR_LEVEL, ROS1_LOG_WARN_LEVEL)
        if ds_type is DataSource.ROS1_BAG
        else (ROS_LOG_ERROR_LEVEL, ROS_LOG_WARN_LEVEL)
    )
    entries = []
    for _, row in df.iterrows():
        level = row["message"].get("level", 0)
        if level >= error_level:
            severity = "ERROR"
        elif level >= warn_level:
            severity = "WARN"
        else:
            severity = "INFO"
        entries.append((row[TIMESTAMP_COL], severity, row["message"].get("msg", "")))
    return entries


def _log_entries(ds_type: DataSource, df: pd.DataFrame) -> list[tuple[float, str, str]]:
    """Return (timestamp, severity, text) triples; severity in ERROR/WARN/INFO."""
    if ds_type is DataSource.PX4_ULOG:
        return _px4_log_entries(df)
    if ds_type in ROS_DS_TYPES:
        return _ros_log_entries(ds_type, df)
    return []


def _errors_verdict(ctx: Context, entries: list[tuple[float, str, str]]) -> CheckResult:
    """Turn classified log entries into the Errors check's verdict."""
    errors = [e for e in entries if e[1] == "ERROR"]
    warns = [e for e in entries if e[1] == "WARN"]
    infos = [e for e in entries if e[1] == "INFO"]

    if errors:
        ts, _, text = errors[0]
        rel_t = ts - ctx.start_seconds
        detail = f'{len(errors)} ERROR message(s), first at ~t=+{rel_t:.1f}s: "{text}"'
        hint = f'investigate the ~t=+{rel_t:.1f}s error: "{text}"'
        return CheckResult("Errors", ICON_ERROR, detail, timestamp=rel_t, verdict_hint=hint)

    if len(warns) >= WARN_NUMEROUS_THRESHOLD:
        rel_t = warns[0][0] - ctx.start_seconds
        detail = f"{len(warns)} WARN message(s), 0 errors"
        hint = f"review the {len(warns)} WARN messages, starting at ~t=+{rel_t:.1f}s"
        return CheckResult("Errors", ICON_WARN, detail, timestamp=rel_t, verdict_hint=hint)

    detail = f"{len(infos)} INFO log message(s), 0 errors"
    if warns:
        detail = f"{len(infos)} INFO, {len(warns)} WARN, 0 ERROR log messages"
    return CheckResult("Errors", ICON_OK, detail)


def check_errors(ctx: Context) -> CheckResult:
    """INFO/WARN/ERROR counts from the log's status/text channel."""
    try:
        relation = ctx.logging_dataset.to_duckdb(ctx.factory, ctx.registry)
    except NoLoggingTopicsFoundError:
        return CheckResult("Errors", ICON_SKIP, "skipped: no logging dataset or status/text topic")
    except Exception:  # an unreadable log channel isn't a crash
        return CheckResult("Errors", ICON_SKIP, "skipped: log/status topic present but unreadable")

    df = relation.to_df()
    if df.empty:
        # A logging topic exists but carries no messages: the card's contract
        # is "no status evidence" is skipped, not an all-clear -- an empty
        # relation must not read the same as a verified-clean log.
        return CheckResult(
            "Errors", ICON_SKIP, "skipped: log/status topic present but has no messages"
        )

    entries = sorted(_log_entries(ctx.ds_type, df), key=lambda entry: entry[0])
    if not entries:
        return CheckResult(
            "Errors", ICON_SKIP, "skipped: log topic present but format unrecognized"
        )

    return _errors_verdict(ctx, entries)


def build_verdict(results: list[CheckResult]) -> str:
    """Name the single most actionable item, worst verdict first, ties by earliest t."""
    actionable = [r for r in results if r.icon in SEVERITY_RANK and r.verdict_hint]
    if not actionable:
        return "VERDICT: all clear."

    def _rank(result: CheckResult) -> tuple[int, float]:
        timestamp = result.timestamp if result.timestamp is not None else float("inf")
        return SEVERITY_RANK[result.icon], timestamp

    actionable.sort(key=_rank)
    return f"VERDICT: {actionable[0].verdict_hint}."


def _providers(ds_type: DataSource, path: str) -> tuple[Any, Any, Any, Any]:
    """Build the (factory, registry, message dataset, logging dataset) quartet.

    Mirrors exactly how server.py's describe_data_source/describe_topic/
    query_messages/read_loggings tools construct these via module.provide --
    this demo calls the same underlying functions, not the MCP layer.

    ``download_description=False`` keeps PX4's topic registry from fetching
    field descriptions from the PX4-Autopilot repo over the network (it's
    ignored for every other ecosystem's registry, whose constructors don't
    accept it -- module.construct() filters kwargs by signature); this demo
    makes zero network calls.
    """
    factory = module.provide(f"{BaseModule.SOURCE_FACTORY.value}.{ds_type.value}", {"path": path})
    registry = module.provide(
        f"{BaseModule.TOPIC_REGISTRY.value}.{ds_type.value}", {"download_description": False}
    )
    dataset = module.provide(f"{BaseModule.MESSAGE_DATASET.value}.{ds_type.value}", {})
    logging_dataset = module.provide(f"{BaseModule.LOGGING_DATASET.value}.{ds_type.value}", {})
    return factory, registry, dataset, logging_dataset


def build_report(path_str: str) -> str:
    """Build the full report card for the log at ``path_str``."""
    path = pathlib.Path(path_str)
    if not path.exists():
        raise DemoPathNotFoundError(f"{path_str} does not exist.")

    try:
        ds_type = resolve(str(path))
    except (ValueError, NotImplementedError) as error:
        raise DemoUnsupportedFormatError(
            f"can't tell what {path.name} is: {error} "
            "(format not supported in demo; the full agent handles it)."
        ) from error

    if ds_type not in SUPPORTED_DS_TYPES:
        raise DemoUnsupportedFormatError(
            f"{path.name} is a recognized format ({ds_type.value}), but this demo "
            "doesn't walk through it yet (format not supported in demo; the full "
            "agent handles it)."
        )

    try:
        factory, registry, dataset, logging_dataset = _providers(ds_type, str(path))
    except (ImportError, ModuleNotFoundError) as error:
        raise DemoUnsupportedFormatError(
            f"{ds_type.value} support isn't installed in this image (missing "
            f"{error.name}); format not supported in demo -- the full agent handles it."
        ) from error

    data_source = factory.build()
    topics = registry.available_topics(data_source)
    metadata = factory.metadata

    ctx = Context(
        ds_type=ds_type,
        factory=factory,
        registry=registry,
        dataset=dataset,
        logging_dataset=logging_dataset,
        data_source=data_source,
        topics=topics,
        start_seconds=metadata["start_seconds"],
    )

    results = [
        check_power(ctx),
        check_imu(ctx),
        check_gps(ctx),
        check_data_gaps(ctx),
        check_errors(ctx),
    ]

    header = (
        f"{path.name} - {metadata['duration_seconds']:.1f}s, "
        f"{metadata['total_message_count']} messages, {len(topics)} topics"
    )
    lines = [header, "", *[result.render() for result in results], "", build_verdict(results)]
    return "\n".join(lines)


def main(argv: list[str]) -> int:
    """Run the demo CLI; return a process exit code."""
    parser = argparse.ArgumentParser(
        prog="demo.py",
        description="Bagel's 60-second hello world: a headless robot-health report card.",
    )
    parser.add_argument("path", nargs="?", help="Path to a log. Defaults to a bundled sample.")
    args = parser.parse_args(argv)

    print(BANNER)
    print()

    path = args.path or str(_default_sample())
    try:
        card = build_report(path)
    except DemoPathNotFoundError as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
    except DemoUnsupportedFormatError as error:
        print(f"Can't run this demo on {path}: {error}")
        return 1

    print(card)
    print()
    print(UPSELL)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
