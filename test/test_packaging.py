"""Packaging invariants for the optional fleet extra."""

import importlib.metadata
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _fleet_requirements() -> list[str]:
    # Installed metadata, not pyproject.toml: tomllib is stdlib only from Python 3.11.
    return [
        requirement.split(";")[0].strip()
        for requirement in importlib.metadata.requires("bagel-mcp") or []
        if "extra == 'fleet'" in requirement.replace('"', "'")
    ]


def test_fleet_extra_has_only_the_mqtt_client_and_cryptography() -> None:
    fleet = sorted(_fleet_requirements())
    assert len(fleet) == 2
    assert fleet[0].startswith("cryptography")
    assert fleet[1].startswith("paho-mqtt")


def test_fleet_extra_is_opt_in_per_image() -> None:
    for name in ("Dockerfile.iot", "Dockerfile.ros2"):
        text = (ROOT / "docker" / name).read_text()
        assert "ARG BAGEL_FLEET=true" in text, name
        assert 'groups="${groups} fleet"' in text, name
