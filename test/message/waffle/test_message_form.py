"""Tests for the WaffleForm adapter family (hardware as code). EXPERIMENTAL BETA."""

import os
import pathlib

import pytest

from src.di import module
from src.di.types.base_module import BaseModule
from src.di.types.data_source import DataSource, resolve
from src.topic import base as topic_base

SAMPLE = "data/sample/waffle/robot.waffleform.yaml"


def _provide() -> tuple:
    ds_type = resolve(SAMPLE)
    factory = module.provide(f"{BaseModule.SOURCE_FACTORY.value}.{ds_type.value}", {"path": SAMPLE})
    registry = module.provide(f"{BaseModule.TOPIC_REGISTRY.value}.{ds_type.value}", {})
    dataset = module.provide(f"{BaseModule.MESSAGE_DATASET.value}.{ds_type.value}", {})
    return factory, registry, dataset


def test_resolves_waffleform_files() -> None:
    assert resolve(SAMPLE) is DataSource.WAFFLE_FORM


def test_component_categories_are_topics() -> None:
    factory, registry, _ = _provide()
    form = factory.build()

    assert registry.available_topics(form) == [
        "actuators",
        "calibration",
        "compute",
        "robot",
        "sensors",
        "software",
    ]
    assert registry.message_count("sensors", form) == 2
    assert registry.native_type_name("robot", form) == "waffle/component"
    with pytest.raises(topic_base.TopicNotFoundError):
        registry.struct("propulsion", form)


def test_union_schema_pads_missing_fields() -> None:
    factory, registry, _ = _provide()
    struct = registry.struct("sensors", factory.build())

    # The lidar has a mount pose, the camera does not: union schema, numeric where
    # every present value is numeric.
    assert set(struct.names) == {"name", "model", "firmware", "mount.x", "mount.y", "mount.z"}
    assert str(struct.field("mount.z").type) == "double"
    assert str(struct.field("firmware").type) == "string"


def test_software_inventory_is_normalized() -> None:
    factory, _, _ = _provide()
    rows = factory.build().rows["software"]

    assert {"name": "ros", "version": "humble"} in rows
    assert {"name": "nav2", "version": "1.1.12"} in rows


def test_sql_over_hardware_state() -> None:
    factory, registry, dataset = _provide()

    relation = dataset.to_duckdb(factory, registry, ["sensors"])
    row = relation.query(
        "sensors",
        "SELECT \"sensors\"['model'] FROM sensors WHERE \"sensors\"['firmware'] = '5.14.0'",
    ).fetchone()
    assert row == ("realsense-d435i",)


def test_snapshot_carries_the_snap_time() -> None:
    factory, registry, dataset = _provide()

    relation = dataset.to_duckdb(factory, registry, ["robot"])
    timestamp, robot = relation.fetchone()
    assert timestamp == factory.build().snap_seconds
    assert robot["name"] == "warehouse-amr-07"
    assert robot["platform"] == "clearpath-jackal"


def test_metadata_summarizes_the_form() -> None:
    factory, _, _ = _provide()
    metadata = factory.metadata

    assert metadata["robot"]["name"] == "warehouse-amr-07"
    assert metadata["categories"]["sensors"] == 2
    assert metadata["categories"]["software"] == 3


def test_resnapped_form_is_a_new_snapshot(tmp_path: pathlib.Path) -> None:
    # Same content re-snapped later must not reuse the older snapshot's cached
    # rows: snap identity is content + snap time, not content alone.
    sample = tmp_path / "robot.waffleform.yaml"
    sample.write_text(pathlib.Path(SAMPLE).read_text(encoding="utf-8"), encoding="utf-8")
    ds_type = resolve(str(sample))

    def _snap() -> tuple[str, float]:
        factory = module.provide(
            f"{BaseModule.SOURCE_FACTORY.value}.{ds_type.value}", {"path": str(sample)}
        )
        registry = module.provide(f"{BaseModule.TOPIC_REGISTRY.value}.{ds_type.value}", {})
        dataset = module.provide(f"{BaseModule.MESSAGE_DATASET.value}.{ds_type.value}", {})
        timestamp, _ = dataset.to_duckdb(factory, registry, ["robot"]).fetchone()
        return factory.uuid, timestamp

    first_uuid, first_timestamp = _snap()
    later = sample.stat().st_mtime + 3600
    os.utime(sample, (later, later))
    second_uuid, second_timestamp = _snap()

    assert second_uuid != first_uuid
    assert second_timestamp == pytest.approx(later)
    assert first_timestamp != second_timestamp
