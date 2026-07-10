"""Tests for the WaffleForm adapter family (hardware as code). EXPERIMENTAL BETA."""

import duckdb
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
    duckdb.register("sensors", relation)
    row = duckdb.sql(
        "SELECT \"sensors\"['model'] FROM sensors WHERE \"sensors\"['firmware'] = '5.14.0'"
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
