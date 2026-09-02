"""Packaging invariants for the optional fleet group."""

import pathlib

import tomllib

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _groups() -> dict[str, list[str]]:
    return tomllib.loads((ROOT / "pyproject.toml").read_text())["dependency-groups"]


def test_fleet_group_exists_with_only_the_mqtt_client_and_cryptography() -> None:
    fleet = _groups()["fleet"]
    assert len(fleet) == 2
    assert fleet[0].startswith("paho-mqtt")
    assert fleet[1].startswith("cryptography")


def test_fleet_group_is_opt_in_per_image() -> None:
    for name in ("Dockerfile.iot", "Dockerfile.ros2"):
        text = (ROOT / "docker" / name).read_text()
        assert "ARG BAGEL_FLEET=true" in text, name
        assert "--group fleet" in text, name
