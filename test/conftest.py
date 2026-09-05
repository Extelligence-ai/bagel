"""Keep tests independent of user caches, capabilities, and output directories."""

import os
import pathlib

import pytest

from settings import settings


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
        "test/source/test_influxdb.py::test_end_to_end_over_live_influxdb",
        "test/source/test_influxdb.py::test_preview_pipeline_detects_events_in_influxdb",
        "test/pipeline/integration/test_ros2_write_paths.py",
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
