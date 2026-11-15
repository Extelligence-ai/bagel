"""Robot health report card in one call: power, IMU, GPS, data gaps and errors.

The same checks serve three questions (#218, #219): a whole recorded log ("what
happened"), an event's window or captured artifact ("why did this fire"), and a live
sink buffer's recent seconds ("is it safe to run right now"). Topics are matched by
their field names across ecosystems (PX4, ArduPilot, ROS, Copper, ...); a check whose
fields aren't in the log is skipped with a reason, never failed. Every number is a
DuckDB aggregate: code computes, the model narrates.
"""

import re
import statistics
import time
from typing import Any

from bagel_mcp.di import module
from bagel_mcp.di.types.base_module import BaseModule
from bagel_mcp.di.types.data_source import DataSource, resolve
from bagel_mcp.logging.base import NoLoggingTopicsFoundError
from bagel_mcp.pipeline.decide import summary
from bagel_mcp.settings import settings
from bagel_mcp.source.context import SourceContext

OK, WARN, FAIL, SKIP = "ok", "warn", "fail", "skipped"
_RANK = {SKIP: 0, OK: 1, WARN: 2, FAIL: 3}

# Thresholds, returned with the report so the narration can cite them.
RULES = {
    "power_drop_warn": 0.10,  # largest single-sample drop, as a fraction of the median
    "power_drop_fail": 0.25,
    "imu_burst_warn": 4.0,  # worst window's std vs the median window's std
    "gap_warn_ratio": 5.0,  # largest gap vs the median interval ...
    "gap_warn_seconds": 0.5,  # ... and at least this long
    "warnings_warn": 10,  # WARN lines before the errors check warns
}

_VOLTAGE = re.compile(r"^(voltage|voltage_v|volt|volts|vbat|vbatlatest)$", re.I)
_ACCEL_AXES = (
    ("x", "y", "z"),  # PX4 sensor_accel, ROS linear_acceleration.{x,y,z}
    ("accel_x", "accel_y", "accel_z"),  # Copper
    ("accx", "accy", "accz"),  # ArduPilot IMU
)
_GPS_QUALITY = re.compile(r"^(fix_type|satellites_used|nsats|status)$", re.I)
_ERROR_LEVELS = {"ERROR", "FATAL", "CRITICAL", "EMERGENCY", "ALERT"}
_WARN_LEVELS = {"WARN", "WARNING"}


_ROS_LEVELS = ("DEBUG", "INFO", "WARN", "ERROR", "FATAL")
_GPS_FIX_3D = 3
_GPS_MIN_SATELLITES = 6
_GPS_WEAK_WARN, _GPS_WEAK_FAIL = 0.1, 0.5
_BURST_MIN_SECONDS, _BURST_INTERVALS = 1.0, 5
# Event-driven topics publish in bursts by design; their gaps say nothing about health.
_EVENT_DRIVEN = re.compile(r"rosout|parameter_events|^/?events/|tf_static|robot_description")


def log_record(message: dict[str, Any]) -> tuple[str, str]:
    """Return (LEVEL, text) for a logging record from any format.

    PX4 names its level (`log_level`); ROS 1/2 rosout carries a numeric `level` plus
    the level constants themselves (ROS 1: INFO=2, ERROR=8; ROS 2: INFO=20, ERROR=40).
    """
    text = str(message.get("message") or message.get("msg") or "").strip()
    if message.get("log_level"):
        return str(message["log_level"]).upper(), text
    level = message.get("level")
    for name in _ROS_LEVELS:
        if level is not None and message.get(name) == level:
            return name, text
    return "UNKNOWN", text


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _value(column: str, path: tuple[str, ...]) -> str:
    return summary._expression(column, path)  # DOUBLE, NULL for NaN/inf markers


class _Log:
    """One source and window, with per-topic relations and signal maps cached."""

    def __init__(
        self,
        path: str,
        args: dict[str, Any] | None,
        start_seconds: float | None,
        end_seconds: float | None,
        last_seconds: float | None,
    ) -> None:
        self.path = path
        self.source = SourceContext.build(path, args)
        self.topics = self.source.registry.available_topics(self.source.factory.build())
        source_start, source_end = self.source.bounds()
        # A live buffer's "last N seconds" ends now, not at its newest message: a robot
        # that stopped publishing must not pass preflight on stale data.
        live = resolve(path) == DataSource.BAGEL_SINK
        if live and end_seconds is None:
            end_seconds = time.time()
            source_end = max(source_end, end_seconds)
        if last_seconds is not None:
            start_seconds = (end_seconds or source_end) - last_seconds
        self.start = start_seconds
        self.end = end_seconds
        self.window = [
            source_start if start_seconds is None else max(start_seconds, source_start),
            source_end if end_seconds is None else min(end_seconds, source_end),
        ]
        self._signals: dict[str, summary.Signals] = {}

    def relation(self, topic: str) -> Any:  # noqa: ANN401 -- DuckDBPyRelation
        return self.source.dataset.to_duckdb(
            self.source.factory, self.source.registry, [topic], self.start, self.end
        )

    def signals(self, topic: str) -> summary.Signals:
        if topic not in self._signals:
            try:
                self._signals[topic] = summary.numeric_signals(self.relation(topic))
            except Exception:  # an undecodable topic just offers no fields
                self._signals[topic] = {}
        return self._signals[topic]


def _leaf(path: tuple[str, ...]) -> str:
    return path[-1] if path else ""


def _find_voltage(log: _Log) -> tuple[str, tuple[str, ...]] | None:
    # Battery topics first: PX4's px4io_status.voltage_v is the servo rail, not the pack.
    ordered = sorted(log.topics, key=lambda t: (not re.search(r"bat|power", t, re.I), t))
    for topic in ordered:
        for _column, path in log.signals(topic).values():
            if _VOLTAGE.match(_leaf(path)):
                return topic, path
    return None


def _find_accel(log: _Log) -> tuple[str, list[tuple[str, ...]]] | None:
    for topic in sorted(log.topics):
        if not re.search(r"acc|imu", topic, re.I) or re.search(r"status|meta", topic, re.I):
            continue
        signals = log.signals(topic)
        for axes in _ACCEL_AXES:
            found: list[tuple[str, ...]] = []
            for axis in axes:
                match = [
                    path
                    for column, path in signals.values()
                    if _leaf(path).lower() == axis
                    and ("gyro" not in ".".join(path).lower())
                    and ("angular" not in ".".join(path).lower())
                ]
                if match:
                    found.append(match[0])
            if len(found) == len(axes):
                return topic, found
    return None


def _find_gps(log: _Log) -> tuple[str, tuple[str, ...]] | None:
    for topic in sorted(log.topics):
        if not re.search(r"gps|gnss|navsat|fix", topic, re.I):
            continue
        for _column, path in log.signals(topic).values():
            if _GPS_QUALITY.match(_leaf(path)):
                return topic, path
    return None


def _check_power(log: _Log) -> dict[str, Any]:
    found = _find_voltage(log)
    if found is None:
        return {"status": SKIP, "reason": "no battery voltage field in this log"}
    topic, path = found
    value = _value(topic, path)
    ts = _q(settings.TIMESTAMP_SECONDS_COLUMN_NAME)
    row = (
        log.relation(topic)
        .query(
            "m",
            "SELECT min(v), max(v), median(v), arg_max(v, t), max(prev - v), "  # noqa: S608 -- internal expressions
            f"arg_max(t, prev - v), count(v) FROM (SELECT {ts} AS t, {value} AS v, "
            f"lag({value}) OVER (ORDER BY {ts}) AS prev FROM m)",
        )
        .fetchone()
    )
    low, high, median, last, drop, drop_at, count = row
    if not count:
        return {
            "status": SKIP,
            "topic": topic,
            "reason": f"{topic} has no voltage samples in the window",
        }
    drop = max(drop or 0.0, 0.0)
    fraction = drop / median if median else 0.0
    status = (
        FAIL
        if fraction >= RULES["power_drop_fail"]
        else WARN
        if fraction >= RULES["power_drop_warn"]
        else OK
    )
    field = ".".join((topic, *path))
    result = {
        "status": status,
        "topic": topic,
        "signal": field,
        "min": low,
        "max": high,
        "end": last,
        "largest_drop": drop,
        "largest_drop_at_seconds": drop_at if drop > 0 else None,
    }
    if re.search(r"vbat", _leaf(path), re.I):
        result["units"] = "raw (no unit or scale in this format)"
    return result


def _check_imu(log: _Log) -> dict[str, Any]:
    found = _find_accel(log)
    if found is None:
        return {"status": SKIP, "reason": "no 3-axis accelerometer topic in this log"}
    topic, axes = found
    ts = _q(settings.TIMESTAMP_SECONDS_COLUMN_NAME)
    per_axis, worst = {}, None
    for path in axes:
        value = _value(topic, path)
        overall = (
            log.relation(topic)
            .query("m", f"SELECT stddev_pop({value}), max(abs({value})), count({value}) FROM m")  # noqa: S608
            .fetchone()
        )
        (interval,) = (
            log.relation(topic)
            .query(
                "m",
                f"SELECT median(dt) FROM (SELECT {ts} - lag({ts}) OVER (ORDER BY {ts}) AS dt "  # noqa: S608
                "FROM m)",
            )
            .fetchone()
        )
        # Buckets hold several samples even on a slow log (PX4 often logs accel at 1 Hz).
        width = max(_BURST_MIN_SECONDS, _BURST_INTERVALS * (interval or 0.0))
        buckets = (
            log.relation(topic)
            .query(
                "m",
                f"SELECT floor({ts} / {width}) * {width} AS t0, stddev_pop({value}) AS s FROM m "  # noqa: S608
                f"GROUP BY 1 HAVING count({value}) > 2 ORDER BY 1",
            )
            .fetchall()
        )
        stds = [s for _, s in buckets if s is not None]
        baseline = statistics.median(stds) if stds else None
        burst_second, burst = max(buckets, key=lambda b: b[1] or 0.0) if buckets else (None, None)
        ratio = burst / baseline if burst and baseline else None
        label = ".".join((topic, *path))
        per_axis[label] = {
            "std": overall[0],
            "max_abs": overall[1],
            "samples": overall[2],
            "burst_window_seconds": width,
            "worst_window_std_ratio": ratio,
            "worst_window_at_seconds": burst_second if ratio else None,
        }
        if ratio is not None and (worst is None or ratio > worst[1]):
            worst = (label, ratio, burst_second)
    if not any(axis["samples"] for axis in per_axis.values()):
        return {"status": SKIP, "topic": topic, "reason": f"{topic} has no samples in the window"}
    status = WARN if worst and worst[1] >= RULES["imu_burst_warn"] else OK
    result: dict[str, Any] = {"status": status, "topic": topic, "axes": per_axis}
    if worst:
        result["worst_burst"] = {
            "signal": worst[0],
            "std_ratio": worst[1],
            "at_seconds": worst[2],
        }
    return result


def _check_gps(log: _Log) -> dict[str, Any]:
    found = _find_gps(log)
    if found is None:
        return {"status": SKIP, "reason": "no GPS fix-quality field in this log"}
    topic, path = found
    value = _value(topic, path)
    rows = (
        log.relation(topic)
        .query("m", f"SELECT {value} AS q, count(*) FROM m GROUP BY 1 ORDER BY 1")  # noqa: S608
        .fetchall()
    )
    distribution = {str(q): n for q, n in rows}
    total = sum(distribution.values())
    if not total:
        return {"status": FAIL, "signal": ".".join((topic, *path)), "reason": "no GPS samples"}
    leaf = _leaf(path).lower()
    # Fix types: 0/1 = no fix, >= 3 = 3D. ROS NavSatStatus: -1 = no fix. Satellites: < 6 weak.
    weak = sum(
        n
        for q, n in rows
        if q is not None
        and (
            (leaf in {"fix_type", "status"} and q < (_GPS_FIX_3D if leaf == "fix_type" else 0))
            or (leaf in {"satellites_used", "nsats"} and q < _GPS_MIN_SATELLITES)
        )
    )
    share = weak / total
    status = FAIL if share > _GPS_WEAK_FAIL else WARN if share > _GPS_WEAK_WARN else OK
    return {
        "status": status,
        "topic": topic,
        "signal": ".".join((topic, *path)),
        "distribution": distribution,
        "weak_fix_share": share,
    }


def _check_gaps(log: _Log, topics: list[str], silent_matters: bool) -> dict[str, Any]:
    if not topics:
        topics = [t for t in sorted(log.topics) if not _EVENT_DRIVEN.search(t)][:10]
    ts = _q(settings.TIMESTAMP_SECONDS_COLUMN_NAME)
    per_topic, flagged, silent = {}, [], []
    for topic in topics:
        try:
            median, largest, at, count = (
                log.relation(topic)
                .query(
                    "m",
                    f"SELECT median(dt), max(dt), arg_max(t, dt), count(*) + 1 FROM "  # noqa: S608
                    f"(SELECT {ts} AS t, {ts} - lag({ts}) OVER (ORDER BY {ts}) AS dt FROM m) "
                    "WHERE dt IS NOT NULL",
                )
                .fetchone()
            )
        except Exception:  # an undecodable topic is reported, not fatal
            per_topic[topic] = {"error": "could not decode"}
            continue
        if not median:
            silent.append(topic)
            per_topic[topic] = {"messages": count - 1}
            continue
        per_topic[topic] = {
            "median_interval_seconds": median,
            "largest_gap_seconds": largest,
            "largest_gap_at_seconds": at,
        }
        if largest >= RULES["gap_warn_seconds"] and largest >= RULES["gap_warn_ratio"] * median:
            flagged.append(topic)
    # A recorded log may declare topics it never published; live, a silent topic is a fault.
    if not silent_matters:
        status = WARN if flagged else OK
    else:
        status = (
            FAIL if silent and len(silent) == len(topics) else WARN if flagged or silent else OK
        )
    return {
        "status": status,
        "topics": per_topic,
        "gaps": flagged,
        "silent_or_single_message": silent,
    }


def _check_errors(log: _Log) -> dict[str, Any]:
    import_path = f"{BaseModule.LOGGING_DATASET.value}.{resolve(log.path).value}"
    try:
        dataset = module.provide(import_path, {})
        records = (
            dataset.to_duckdb(log.source.factory, log.source.registry, log.start, log.end)
            .to_df()
            .to_dict(orient="records")
        )
    except NoLoggingTopicsFoundError:
        return {"status": SKIP, "reason": "no diagnostic log in this source"}
    except ModuleNotFoundError as error:
        if not error.name or not import_path.startswith(error.name):
            raise
        return {"status": SKIP, "reason": "this format has no diagnostic log"}
    levels: dict[str, int] = {}
    first_error = None
    for record in records:
        level, text = log_record(record.get("message") or {})
        levels[level] = levels.get(level, 0) + 1
        if level in _ERROR_LEVELS and first_error is None:
            first_error = {
                "at_seconds": record.get(settings.TIMESTAMP_SECONDS_COLUMN_NAME),
                "message": text,
            }
    errors = sum(n for level, n in levels.items() if level in _ERROR_LEVELS)
    warnings = sum(n for level, n in levels.items() if level in _WARN_LEVELS)
    status = FAIL if errors else WARN if warnings > RULES["warnings_warn"] else OK
    return {"status": status, "levels": levels, "first_error": first_error}


def check(  # noqa: PLR0913
    path: str,
    args: dict[str, Any] | None = None,
    start_seconds: float | None = None,
    end_seconds: float | None = None,
    last_seconds: float | None = None,
    topics: list[str] | None = None,
) -> dict[str, Any]:
    """Run the health checks on a log, a time window of it, or its last N seconds."""
    if last_seconds is not None and start_seconds is not None:
        raise ValueError("Set 'start_seconds' or 'last_seconds', not both.")
    log = _Log(path, args, start_seconds, end_seconds, last_seconds)
    if topics is not None and (unknown := sorted(set(topics) - set(log.topics))):
        raise ValueError(f"Topics not in the source: {unknown}; available: {log.topics}")
    checks = {"power": _check_power(log), "imu": _check_imu(log), "gps": _check_gps(log)}
    gap_topics = topics or sorted({c["topic"] for c in checks.values() if c.get("topic")})
    checks["data_gaps"] = _check_gaps(
        log,
        [t for t in gap_topics if t in log.topics],
        silent_matters=last_seconds is not None or topics is not None,
    )
    checks["errors"] = _check_errors(log)
    worst = max((c["status"] for c in checks.values()), key=_RANK.__getitem__)
    return {
        "path": path,
        "window_seconds": log.window,
        "topics": len(log.topics),
        "checks": checks,
        "verdict": {FAIL: "no-go", WARN: "investigate", OK: "go", SKIP: "no checks ran"}[worst],
        "rules": RULES,
    }
