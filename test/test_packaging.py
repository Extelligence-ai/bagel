"""Packaging invariants for the optional fleet extra."""

import pathlib

import pytest

tomllib = pytest.importorskip("tomllib")  # Python 3.11+

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _extras() -> dict[str, list[str]]:
    return tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["optional-dependencies"]


def test_fleet_extra_has_only_the_mqtt_client_and_cryptography() -> None:
    fleet = _extras()["fleet"]
    assert len(fleet) == 2
    assert fleet[0].startswith("paho-mqtt")
    assert fleet[1].startswith("cryptography")


def test_fleet_extra_is_opt_in_per_image() -> None:
    for name in ("Dockerfile.iot", "Dockerfile.ros2"):
        text = (ROOT / "docker" / name).read_text()
        assert "ARG BAGEL_FLEET=true" in text, name
        assert 'groups="${groups} fleet"' in text, name
