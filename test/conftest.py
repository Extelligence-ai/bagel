"""Keep tests independent of user caches, capabilities, and output directories."""

import os
import pathlib

import pytest

from bagel_mcp.settings import settings


def pytest_sessionfinish(session: pytest.Session) -> None:
    """Make optional jobs fail if their promised tests are skipped."""
    strict = os.environ.get("BAGEL_REQUIRE_NO_SKIPS") == "1"
    optional = os.environ.get("BAGEL_REQUIRE_OPTIONAL_TESTS") == "1"
    if not (strict or optional):
        return
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    allowed = {
        "test/source/test_postgres.py::test_end_to_end_over_live_database",
        "test/source/test_postgres.py::test_preview_pipeline_detects_events_in_database",
        "test/source/test_postgres.py::test_bounds_ignore_tables_without_a_timestamp_column",
        "test/source/test_influxdb.py::test_end_to_end_over_live_influxdb",
        "test/source/test_influxdb.py::test_preview_pipeline_detects_events_in_influxdb",
        "test/pipeline/integration/test_ros2_write_paths.py",
        "test/pipeline/integration/test_bag_backend_parity.py",
        # Need a real MQTT broker (MQTT_TEST_BROKER); the `fleet-broker` job in
        # test.yaml runs these four files against mosquitto.
        *(
            f"test/sink/publish/{test}"
            for test in (
                "test_enroll_e2e.py::test_first_boot_enroll_to_stream",
                "test_enroll_e2e.py::test_renewal_against_server_without_renewal_offered_returns_none_with_no_crash",
                "test_mqtt_integration.py::test_channels_round_trip",
                "test_mqtt_integration.py::test_schema_is_retained_for_late_subscriber",
                "test_mqtt_integration.py::test_clean_close_publishes_stopped_heartbeat",
                "test_mqtt_integration.py::test_lwt_fires_on_unclean_disconnect",
                "test_selftest.py::test_selftest_e2e_round_trip",
                "test_selftest.py::test_selftest_e2e_leaves_no_retained_residue",
                "test_selftest.py::test_selftest_e2e_refuses_while_a_live_session_is_connected",
                "test_selftest.py::test_selftest_e2e_pre_existing_retained_schema_does_not_false_abort",
                "test_selftest.py::test_selftest_e2e_new_schema_publish_mid_run_aborts",
                "test_selftest.py::test_selftest_e2e_online_true_beat_mid_run_aborts",
                "test_selftest.py::test_selftest_e2e_clean_run_is_stable_across_repeats",
                "test_stream_e2e.py::test_chaos_broker_start_cleans_up_on_unreachable_broker",
                "test_stream_e2e.py::test_reconnect_now_retries_a_stillborn_tcp_connection",
                "test_stream_e2e.py::test_full_path_streams_batches_schema_and_heartbeat",
                "test_stream_e2e.py::test_chaos_kill_and_restart_broker_drains_spool_in_order",
                "test_stream_e2e.py::test_event_rule_fires_with_mcap_artifact_and_health_report_arrives",
                "test_stream_e2e.py::test_stop_publishes_stopped_heartbeat_before_disconnect",
            )
        ),
    }
    unexpected = [
        report.nodeid
        for report in reporter.stats.get("skipped", [])
        if strict or report.nodeid not in allowed
    ]
    if unexpected:
        reporter.write_sep("!", f"Unexpected CI skips: {unexpected}")
        session.exitstatus = pytest.ExitCode.TESTS_FAILED


def pytest_collection_finish(session: pytest.Session) -> None:
    """The optional-dependency CI job must collect the suites it promises to run."""
    if os.environ.get("BAGEL_REQUIRE_OPTIONAL_TESTS") != "1":
        return
    required = {
        "test/adversarial/test_can_parse.py",
        "test/adversarial/test_mdf4_parse.py",
        "test/adversarial/test_secrets_redaction.py",
        "test/sink/test_mqtt.py",
        "test/pipeline/test_upload_clouds.py",
        "test/pipeline/test_rerun_export.py",
    }
    collected = {item.nodeid.split("::")[0] for item in session.items}
    if missing := required - collected:
        raise pytest.UsageError(f"Required optional suites were not collected: {sorted(missing)}")


@pytest.fixture(autouse=True)
def isolate_storage(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Each test owns its storage; cache lifecycle tests share it explicitly."""
    tmp_path = tmp_path / ".bagel-test"
    monkeypatch.setattr(settings, "CACHE_DIRECTORY", str(tmp_path / "cache"))
    monkeypatch.setattr(settings, "ARTIFACT_DIRECTORY", str(tmp_path / "artifacts"))
    monkeypatch.setattr(settings, "USER_CAPABILITIES_DIRECTORY", str(tmp_path / "capabilities"))
