"""PX4 field descriptions: one small cached HTTPS fetch per message type, never a clone."""

import pathlib
import urllib.error

import pytest

from bagel_mcp.settings import settings
from bagel_mcp.topic.px4 import parse

SHA = "1c8ab2a0d7db2d14a6f320ebd8766b5ffaea28fa"


@pytest.fixture(autouse=True)
def _isolated(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "CACHE_DIRECTORY", str(tmp_path))
    monkeypatch.setattr(parse, "_offline", False)


def _serve(monkeypatch: pytest.MonkeyPatch, files: dict[str, str]) -> list[str]:
    requested: list[str] = []

    def fetch(ref: str, path: str) -> str | None:
        requested.append(f"{ref}/{path}")
        return files.get(f"{ref}/{path}")

    monkeypatch.setattr(parse, "_fetch", fetch)
    return requested


def test_old_firmware_uses_the_snake_case_file(monkeypatch: pytest.MonkeyPatch) -> None:
    requested = _serve(monkeypatch, {f"{SHA}/msg/battery_status.msg": "float32 voltage_v"})

    assert parse.msg_definition("battery_status", SHA, overwrite=False) == "float32 voltage_v"
    assert requested == [
        f"{SHA}/msg/BatteryStatus.msg",
        f"{SHA}/msg/versioned/BatteryStatus.msg",
        f"{SHA}/msg/battery_status.msg",
    ]


def test_found_and_missing_files_are_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    requested = _serve(monkeypatch, {"main/msg/versioned/BatteryStatus.msg": "float32 v"})

    for _ in range(2):
        assert parse.msg_definition("battery_status", None, overwrite=False) == "float32 v"
        assert parse.msg_definition("no_such_type", None, overwrite=False) is None

    assert len(requested) == 2 + 3  # found on the 2nd path; missing after all 3, once each


def test_offline_costs_one_failure_per_process(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []

    def unreachable(url: str, timeout: float) -> None:
        calls.append(url)
        raise urllib.error.URLError("no network")

    monkeypatch.setattr(parse.urllib.request, "urlopen", unreachable)
    with pytest.raises(urllib.error.URLError):
        parse.msg_definition("battery_status", SHA, overwrite=False)
    assert parse.msg_definition("sensor_accel", SHA, overwrite=False) is None
    assert len(calls) == 1


@pytest.mark.parametrize(("type_name", "ref"), [("../etc/passwd", None), ("imu", "../../x")])
def test_odd_names_and_refs_never_reach_the_network(
    monkeypatch: pytest.MonkeyPatch, type_name: str, ref: str | None
) -> None:
    requested = _serve(monkeypatch, {})
    assert parse.msg_definition(type_name, ref, overwrite=False) is None
    assert requested == []
