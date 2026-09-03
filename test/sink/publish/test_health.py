"""Tests for the pure fleet health report builder (fleet step 8, Task 6).

All pure -- a `_inputs()` factory builds a fully-healthy `HealthInputs`, each
test perturbs exactly the field(s) it needs via keyword overrides. No
threads, no disk, no network; the clock is always the `now=` kwarg the
functions under test take explicitly.
"""

import dataclasses
import datetime
import importlib
import sys

import pytest

from src.sink.publish import health
from src.sink.publish.health import (
    CHECK_STATUSES,
    DISK_FAIL_BYTES,
    DISK_WARN_BYTES,
    EVENTS_BACKLOG_WARN,
    HealthInputs,
    HealthSnapshot,
    build_health_report,
    snapshot_from,
    verdict,
)

NOW = 1_800_000_000.0  # fixed reference instant; every test's clock is this or an offset of it

_CHECK_NAMES = (
    "connection",
    "queue",
    "events_pipeline",
    "spool",
    "events_backlog",
    "disk",
    "certificate",
    "topic_staleness",
    "heartbeat",
    "artifacts",
)


def _lane(**overrides: object) -> dict:
    base = {"bytes": 0, "pending": 0, "last_seq": 0, "acked_seq": 0, "evicted": 0}
    base.update(overrides)
    return base


def _spool(
    *, channels: dict | None = None, events: dict | None = None, heartbeat: dict | None = None
) -> dict:
    return {
        "channels": channels if channels is not None else _lane(),
        "events": events if events is not None else _lane(),
        "heartbeat": heartbeat if heartbeat is not None else _lane(),
    }


def _status(**overrides: object) -> dict:
    base = {
        "online": True,
        "backoff": None,
        "queue": {"depth": 0, "dropped": 0},
        "skipped": 0,
        "spool": _spool(),
        "reconnects": 0,
        "subscriptions": ["imu"],
        "channels_active": 1,
        "router_alive": True,
        "router_error": None,
        "heartbeat_spool_failures": 0,
        "heartbeat_alive": True,
        "heartbeat_error": None,
        "cert_expires_at": None,
    }
    base.update(overrides)
    return base


def _iso_offset(now: float, days: float) -> str:
    dt = datetime.datetime.fromtimestamp(now + days * 86400, tz=datetime.timezone.utc)
    return dt.isoformat()


_UNSET = object()


def _inputs(  # noqa: PLR0913 -- one field per HealthInputs knob, kept explicit for tests
    *,
    status: dict | None = None,
    topic_last_seen: dict | None = None,
    cert_expires_at: object = _UNSET,
    enrolled: bool = True,
    disk_free_bytes: int = DISK_WARN_BYTES * 10,
    spool_cap_bytes: int = 1_000_000_000,
    artifacts: dict | None = None,
    artifacts_cap_bytes: int = 1000,
    events_counters: dict | None = None,
    uptime_s: float = 123.4,
) -> HealthInputs:
    return HealthInputs(
        status=status if status is not None else _status(),
        topic_last_seen=topic_last_seen if topic_last_seen is not None else {"imu": NOW},
        cert_expires_at=(_iso_offset(NOW, 180) if cert_expires_at is _UNSET else cert_expires_at),
        enrolled=enrolled,
        disk_free_bytes=disk_free_bytes,
        spool_cap_bytes=spool_cap_bytes,
        artifacts=artifacts if artifacts is not None else {"bytes": 100, "files": 1},
        artifacts_cap_bytes=artifacts_cap_bytes,
        events_counters=(
            events_counters
            if events_counters is not None
            else {
                "queue_depth": 0,
                "dropped": 0,
                "predicate_errors": 0,
                "fired": 0,
                "suppressed": 0,
            }
        ),
        uptime_s=uptime_s,
    )


def _checks_by_name(summary: dict) -> dict:
    return {c["name"]: c for c in summary["checks"]}


def test_health_module_does_not_import_paho_or_cryptography_eagerly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """health.py must not drag paho or cryptography at import time."""
    for name in [
        m
        for m in sys.modules
        if m == "paho"
        or m.startswith("paho.")
        or m == "cryptography"
        or m.startswith("cryptography.")
    ]:
        monkeypatch.delitem(sys.modules, name)
    monkeypatch.delitem(sys.modules, "src.sink.publish.health", raising=False)
    importlib.import_module("src.sink.publish.health")
    assert not any(
        m == "paho" or m.startswith("paho.") or m == "cryptography" or m.startswith("cryptography.")
        for m in sys.modules
    )


def test_check_statuses_pinned_exactly() -> None:
    assert CHECK_STATUSES == ("pass", "warn", "fail", "skipped")


class TestHealthy:
    def test_all_ten_checks_pass_verdict(self) -> None:
        summary, _snapshot = build_health_report(_inputs(), previous=None, now=NOW)
        checks = summary["checks"]
        assert len(checks) == 10
        assert {c["name"] for c in checks} == set(_CHECK_NAMES)
        assert all(c["status"] == "pass" for c in checks)
        assert summary["verdict"] == "all 10 checks pass"

    def test_schema_rev_and_source_shape(self) -> None:
        summary, _snapshot = build_health_report(_inputs(uptime_s=42.0), previous=None, now=NOW)
        assert summary["schema_rev"] == 1
        assert set(summary["source"]) == {"component", "bagel_version", "uptime_s"}
        assert summary["source"]["component"] == "bagel"
        assert summary["source"]["uptime_s"] == 42.0
        assert isinstance(summary["source"]["bagel_version"], str)

    def test_reason_key_absent_on_every_pass_check(self) -> None:
        summary, _snapshot = build_health_report(_inputs(), previous=None, now=NOW)
        for check in summary["checks"]:
            assert check["status"] == "pass"
            assert "reason" not in check

    def test_unenrolled_and_no_artifact_store_skip_variant_verdict(self) -> None:
        inputs = _inputs(enrolled=False, cert_expires_at=None, artifacts={})
        summary, _snapshot = build_health_report(inputs, previous=None, now=NOW)
        by_name = _checks_by_name(summary)
        assert by_name["certificate"]["status"] == "skipped"
        assert by_name["artifacts"]["status"] == "skipped"
        assert summary["verdict"] == "all 10 checks pass (2 skipped)"

    def test_reason_present_on_every_non_pass_check(self) -> None:
        inputs = _inputs(enrolled=False, cert_expires_at=None, artifacts={})
        summary, _snapshot = build_health_report(inputs, previous=None, now=NOW)
        for check in summary["checks"]:
            if check["status"] != "pass":
                assert "reason" in check
                assert check["reason"]
            else:
                assert "reason" not in check


class TestConnection:
    def test_router_dead_fails_with_router_error_reason(self) -> None:
        inputs = _inputs(status=_status(router_alive=False, router_error="broker unreachable"))
        summary, _snapshot = build_health_report(inputs, previous=None, now=NOW)
        check = _checks_by_name(summary)["connection"]
        assert check["status"] == "fail"
        assert check["reason"] == "broker unreachable"

    def test_offline_warns(self) -> None:
        inputs = _inputs(status=_status(online=False, backoff=4.0))
        summary, _snapshot = build_health_report(inputs, previous=None, now=NOW)
        check = _checks_by_name(summary)["connection"]
        assert check["status"] == "warn"
        assert check["metrics"]["backoff"] == 4.0

    def test_metrics_shape(self) -> None:
        inputs = _inputs(status=_status(reconnects=3))
        summary, _snapshot = build_health_report(inputs, previous=None, now=NOW)
        metrics = _checks_by_name(summary)["connection"]["metrics"]
        assert set(metrics) == {"online", "backoff", "reconnects", "reconnects_delta"}
        assert metrics["reconnects"] == 3
        assert metrics["reconnects_delta"] == 3  # previous=None -> delta equals cumulative


class TestQueueAndDeltas:
    def test_dropped_grown_warns(self) -> None:
        inputs = _inputs(status=_status(queue={"depth": 2, "dropped": 5}))
        summary, _snapshot = build_health_report(inputs, previous=None, now=NOW)
        check = _checks_by_name(summary)["queue"]
        assert check["status"] == "warn"
        assert check["metrics"]["dropped_delta"] == 5

    def test_previous_none_delta_equals_cumulative(self) -> None:
        inputs = _inputs(status=_status(queue={"depth": 0, "dropped": 7}))
        summary, _snapshot = build_health_report(inputs, previous=None, now=NOW)
        metrics = _checks_by_name(summary)["queue"]["metrics"]
        assert metrics["dropped"] == 7
        assert metrics["dropped_delta"] == 7

    def test_chained_snapshot_zero_delta_after_quiet_period(self) -> None:
        inputs = _inputs(status=_status(queue={"depth": 0, "dropped": 7}))
        first_summary, snapshot1 = build_health_report(inputs, previous=None, now=NOW)
        assert _checks_by_name(first_summary)["queue"]["status"] == "warn"

        second_summary, _snapshot2 = build_health_report(inputs, previous=snapshot1, now=NOW + 60)
        check = _checks_by_name(second_summary)["queue"]
        assert check["metrics"]["dropped_delta"] == 0
        assert check["status"] == "pass"
        assert check["metrics"]["dropped"] == 7  # cumulative counter itself is unchanged


class TestEventsPipeline:
    def test_predicate_errors_warn(self) -> None:
        inputs = _inputs(
            events_counters={
                "queue_depth": 0,
                "dropped": 0,
                "predicate_errors": 2,
                "fired": 0,
                "suppressed": 0,
            }
        )
        summary, _snapshot = build_health_report(inputs, previous=None, now=NOW)
        check = _checks_by_name(summary)["events_pipeline"]
        assert check["status"] == "warn"
        assert check["metrics"]["predicate_errors"] == 2

    def test_emitter_dropped_grown_warns(self) -> None:
        inputs = _inputs(
            events_counters={
                "queue_depth": 3,
                "dropped": 4,
                "predicate_errors": 0,
                "fired": 1,
                "suppressed": 0,
            }
        )
        summary, _snapshot = build_health_report(inputs, previous=None, now=NOW)
        check = _checks_by_name(summary)["events_pipeline"]
        assert check["status"] == "warn"
        assert check["metrics"]["dropped_delta"] == 4
        assert check["metrics"]["fired"] == 1


class TestSpool:
    def test_evicted_delta_fails(self) -> None:
        inputs = _inputs(status=_status(spool=_spool(channels=_lane(evicted=3))))
        summary, _snapshot = build_health_report(inputs, previous=None, now=NOW)
        check = _checks_by_name(summary)["spool"]
        assert check["status"] == "fail"
        assert check["metrics"]["evicted_delta"] == 3

    def test_bytes_over_warn_fraction_warns(self) -> None:
        cap = 1000
        inputs = _inputs(
            status=_status(spool=_spool(channels=_lane(bytes=801))), spool_cap_bytes=cap
        )
        summary, _snapshot = build_health_report(inputs, previous=None, now=NOW)
        check = _checks_by_name(summary)["spool"]
        assert check["status"] == "warn"
        assert check["metrics"]["cap_bytes"] == cap

    def test_bytes_at_warn_fraction_passes(self) -> None:
        cap = 1000
        inputs = _inputs(
            status=_status(spool=_spool(channels=_lane(bytes=800))), spool_cap_bytes=cap
        )
        summary, _snapshot = build_health_report(inputs, previous=None, now=NOW)
        assert _checks_by_name(summary)["spool"]["status"] == "pass"

    def test_evicted_takes_priority_over_warn_bytes(self) -> None:
        cap = 1000
        inputs = _inputs(
            status=_status(spool=_spool(channels=_lane(bytes=999, evicted=1))),
            spool_cap_bytes=cap,
        )
        summary, _snapshot = build_health_report(inputs, previous=None, now=NOW)
        assert _checks_by_name(summary)["spool"]["status"] == "fail"


class TestEventsBacklog:
    def test_at_threshold_passes(self) -> None:
        inputs = _inputs(status=_status(spool=_spool(events=_lane(pending=EVENTS_BACKLOG_WARN))))
        summary, _snapshot = build_health_report(inputs, previous=None, now=NOW)
        assert _checks_by_name(summary)["events_backlog"]["status"] == "pass"

    def test_above_threshold_warns(self) -> None:
        inputs = _inputs(
            status=_status(spool=_spool(events=_lane(pending=EVENTS_BACKLOG_WARN + 1)))
        )
        summary, _snapshot = build_health_report(inputs, previous=None, now=NOW)
        check = _checks_by_name(summary)["events_backlog"]
        assert check["status"] == "warn"
        assert check["metrics"]["pending"] == EVENTS_BACKLOG_WARN + 1


class TestDiskBoundaries:
    def test_exactly_at_fail_threshold_warns_not_fails(self) -> None:
        inputs = _inputs(disk_free_bytes=DISK_FAIL_BYTES)
        summary, _snapshot = build_health_report(inputs, previous=None, now=NOW)
        assert _checks_by_name(summary)["disk"]["status"] == "warn"

    def test_one_byte_below_fail_threshold_fails(self) -> None:
        inputs = _inputs(disk_free_bytes=DISK_FAIL_BYTES - 1)
        summary, _snapshot = build_health_report(inputs, previous=None, now=NOW)
        assert _checks_by_name(summary)["disk"]["status"] == "fail"

    def test_one_byte_below_warn_threshold_warns(self) -> None:
        inputs = _inputs(disk_free_bytes=DISK_WARN_BYTES - 1)
        summary, _snapshot = build_health_report(inputs, previous=None, now=NOW)
        assert _checks_by_name(summary)["disk"]["status"] == "warn"

    def test_exactly_at_warn_threshold_passes(self) -> None:
        inputs = _inputs(disk_free_bytes=DISK_WARN_BYTES)
        summary, _snapshot = build_health_report(inputs, previous=None, now=NOW)
        assert _checks_by_name(summary)["disk"]["status"] == "pass"


class TestCertificate:
    def test_unenrolled_skips(self) -> None:
        inputs = _inputs(enrolled=False, cert_expires_at=None)
        summary, _snapshot = build_health_report(inputs, previous=None, now=NOW)
        check = _checks_by_name(summary)["certificate"]
        assert check["status"] == "skipped"
        assert check["reason"] == "not enrolled"

    def test_expired_fails(self) -> None:
        inputs = _inputs(cert_expires_at=_iso_offset(NOW, -1))
        summary, _snapshot = build_health_report(inputs, previous=None, now=NOW)
        assert _checks_by_name(summary)["certificate"]["status"] == "fail"

    def test_29_days_left_warns(self) -> None:
        inputs = _inputs(cert_expires_at=_iso_offset(NOW, 29))
        summary, _snapshot = build_health_report(inputs, previous=None, now=NOW)
        assert _checks_by_name(summary)["certificate"]["status"] == "warn"

    def test_far_future_passes(self) -> None:
        inputs = _inputs(cert_expires_at=_iso_offset(NOW, 180))
        summary, _snapshot = build_health_report(inputs, previous=None, now=NOW)
        assert _checks_by_name(summary)["certificate"]["status"] == "pass"

    def test_unparsable_warns_and_names_it(self) -> None:
        inputs = _inputs(cert_expires_at="not-a-date")
        summary, _snapshot = build_health_report(inputs, previous=None, now=NOW)
        check = _checks_by_name(summary)["certificate"]
        assert check["status"] == "warn"
        assert "not-a-date" in check["reason"]


class TestTopicStaleness:
    def test_no_tapped_topics_skips(self) -> None:
        inputs = _inputs(topic_last_seen={})
        summary, _snapshot = build_health_report(inputs, previous=None, now=NOW)
        assert _checks_by_name(summary)["topic_staleness"]["status"] == "skipped"

    def test_stale_and_never_seen_both_collected(self) -> None:
        inputs = _inputs(
            topic_last_seen={
                "imu": NOW - health.TOPIC_STALE_AFTER_S - 1,
                "odom": None,
                "fresh": NOW,
            }
        )
        summary, _snapshot = build_health_report(inputs, previous=None, now=NOW)
        check = _checks_by_name(summary)["topic_staleness"]
        assert check["status"] == "warn"
        assert check["metrics"]["stale"] == ["imu", "odom"]
        assert check["metrics"]["topics"] == 3

    def test_all_fresh_passes(self) -> None:
        inputs = _inputs(topic_last_seen={"imu": NOW, "odom": NOW - 1})
        summary, _snapshot = build_health_report(inputs, previous=None, now=NOW)
        assert _checks_by_name(summary)["topic_staleness"]["status"] == "pass"


class TestHeartbeat:
    def test_not_alive_fails(self) -> None:
        inputs = _inputs(status=_status(heartbeat_alive=False))
        summary, _snapshot = build_health_report(inputs, previous=None, now=NOW)
        assert _checks_by_name(summary)["heartbeat"]["status"] == "fail"

    def test_spool_failures_grown_warns(self) -> None:
        inputs = _inputs(status=_status(heartbeat_spool_failures=2))
        summary, _snapshot = build_health_report(inputs, previous=None, now=NOW)
        check = _checks_by_name(summary)["heartbeat"]
        assert check["status"] == "warn"
        assert check["metrics"]["spool_failures_delta"] == 2

    def test_heartbeat_error_set_warns(self) -> None:
        inputs = _inputs(status=_status(heartbeat_error="spool full"))
        summary, _snapshot = build_health_report(inputs, previous=None, now=NOW)
        check = _checks_by_name(summary)["heartbeat"]
        assert check["status"] == "warn"
        assert check["reason"] == "spool full"


class TestArtifacts:
    def test_no_store_wired_skips(self) -> None:
        inputs = _inputs(artifacts={})
        summary, _snapshot = build_health_report(inputs, previous=None, now=NOW)
        check = _checks_by_name(summary)["artifacts"]
        assert check["status"] == "skipped"
        assert check["reason"] == "no artifact rules"

    def test_over_warn_fraction_warns(self) -> None:
        inputs = _inputs(artifacts={"bytes": 801, "files": 5}, artifacts_cap_bytes=1000)
        summary, _snapshot = build_health_report(inputs, previous=None, now=NOW)
        assert _checks_by_name(summary)["artifacts"]["status"] == "warn"

    def test_at_warn_fraction_passes(self) -> None:
        inputs = _inputs(artifacts={"bytes": 800, "files": 5}, artifacts_cap_bytes=1000)
        summary, _snapshot = build_health_report(inputs, previous=None, now=NOW)
        assert _checks_by_name(summary)["artifacts"]["status"] == "pass"


class TestVerdict:
    def test_fail_beats_warn_and_lists_sorted_names(self) -> None:
        checks = [
            {"name": "zeta", "status": "warn", "metrics": {}},
            {"name": "spool", "status": "fail", "metrics": {}},
            {"name": "alpha", "status": "fail", "metrics": {}},
            {"name": "disk", "status": "pass", "metrics": {}},
        ]
        assert verdict(checks) == "fail: alpha, spool"

    def test_warn_only_lists_sorted_names(self) -> None:
        checks = [
            {"name": "zeta", "status": "warn", "metrics": {}},
            {"name": "alpha", "status": "warn", "metrics": {}},
            {"name": "disk", "status": "pass", "metrics": {}},
        ]
        assert verdict(checks) == "warn: alpha, zeta"

    def test_skipped_never_drives_verdict(self) -> None:
        checks = [
            {"name": "certificate", "status": "skipped", "metrics": {}},
            {"name": "connection", "status": "pass", "metrics": {}},
        ]
        assert verdict(checks) == "all 2 checks pass (1 skipped)"

    def test_all_pass_no_skips(self) -> None:
        checks = [
            {"name": "connection", "status": "pass", "metrics": {}},
            {"name": "disk", "status": "pass", "metrics": {}},
        ]
        assert verdict(checks) == "all 2 checks pass"


class TestSnapshotFrom:
    def test_fields_extracted_from_inputs(self) -> None:
        inputs = _inputs(
            status=_status(
                queue={"depth": 1, "dropped": 3},
                spool=_spool(channels=_lane(evicted=2)),
                heartbeat_spool_failures=4,
                reconnects=5,
            ),
            events_counters={
                "queue_depth": 0,
                "dropped": 6,
                "predicate_errors": 0,
                "fired": 0,
                "suppressed": 0,
            },
        )
        snapshot = snapshot_from(inputs, NOW)
        assert snapshot == HealthSnapshot(
            queue_dropped=3,
            events_queue_dropped=6,
            spool_evicted=2,
            heartbeat_spool_failures=4,
            reconnects=5,
            taken_at=NOW,
        )


@pytest.mark.parametrize(
    "mutate",
    [
        lambda inputs: inputs,
        lambda inputs: dataclasses.replace(inputs, disk_free_bytes=0),
        lambda inputs: dataclasses.replace(inputs, enrolled=False, cert_expires_at=None),
        lambda inputs: dataclasses.replace(inputs, artifacts={}),
        lambda inputs: dataclasses.replace(inputs, topic_last_seen={}),
        lambda inputs: dataclasses.replace(inputs, cert_expires_at=_iso_offset(NOW, -5)),
        lambda inputs: dataclasses.replace(
            inputs,
            status=_status(router_alive=False, router_error="down"),
        ),
        lambda inputs: dataclasses.replace(
            inputs,
            status=_status(spool=_spool(events=_lane(pending=EVENTS_BACKLOG_WARN + 5))),
        ),
    ],
)
def test_every_check_status_is_in_the_closed_enum(mutate: object) -> None:
    inputs = mutate(_inputs())
    summary, _snapshot = build_health_report(inputs, previous=None, now=NOW)
    for check in summary["checks"]:
        assert check["status"] in CHECK_STATUSES


def test_boot_report_previous_none_deltas_equal_cumulative_across_all_delta_checks() -> None:
    inputs = _inputs(
        status=_status(
            queue={"depth": 0, "dropped": 9},
            spool=_spool(channels=_lane(evicted=8)),
            heartbeat_spool_failures=7,
            reconnects=6,
        ),
        events_counters={
            "queue_depth": 0,
            "dropped": 5,
            "predicate_errors": 0,
            "fired": 0,
            "suppressed": 0,
        },
    )
    summary, _snapshot = build_health_report(inputs, previous=None, now=NOW)
    by_name = _checks_by_name(summary)
    assert by_name["queue"]["metrics"]["dropped_delta"] == 9
    assert by_name["spool"]["metrics"]["evicted_delta"] == 8
    assert by_name["heartbeat"]["metrics"]["spool_failures_delta"] == 7
    assert by_name["connection"]["metrics"]["reconnects_delta"] == 6
    assert by_name["events_pipeline"]["metrics"]["dropped_delta"] == 5


def test_settle_time_smoke_import() -> None:
    """Not health.py's concern, but confirms Task 6's settings.py addition landed."""
    from settings import settings

    assert settings.FLEET_HEALTH_INTERVAL_S == 21_600.0
    assert settings.FLEET_HEALTH_SETTLE_S == 60.0


def test_health_snapshot_is_a_dataclass_with_expected_fields() -> None:
    fields = {f.name for f in dataclasses.fields(HealthSnapshot)}
    assert fields == {
        "queue_dropped",
        "events_queue_dropped",
        "spool_evicted",
        "heartbeat_spool_failures",
        "reconnects",
        "taken_at",
    }
