"""Tests for the ASAM MDF (.mf4) adapter family, over a generated measurement file."""

import pathlib

import duckdb
import pytest

asammdf = pytest.importorskip("asammdf", reason="asammdf is optional (uv sync --group automotive)")
import numpy as np  # noqa: E402
from asammdf import MDF, Signal  # noqa: E402

from src.di import module  # noqa: E402
from src.di.types.base_module import BaseModule  # noqa: E402
from src.di.types.data_source import DataSource, resolve  # noqa: E402
from src.topic import base as topic_base  # noqa: E402


@pytest.fixture(scope="module")
def mf4_file(tmp_path_factory: pytest.TempPathFactory) -> pathlib.Path:
    """A deterministic two-group measurement: engine data at 2 Hz, vehicle at 2 Hz."""
    path = tmp_path_factory.mktemp("mdf") / "measurement.mf4"
    t = np.arange(0, 10, 0.5)
    mdf = MDF(version="4.10")
    mdf.append(
        [
            Signal(samples=3000.0 - 100 * t, timestamps=t, name="EngineSpeed", unit="rpm"),
            Signal(samples=np.linspace(0, 100, len(t)), timestamps=t, name="ThrottlePos", unit="%"),
        ],
        acq_name="EngineData",
        comment="engine group",
    )
    mdf.append(
        [Signal(samples=2.0 * t, timestamps=t, name="VehicleSpeed", unit="km/h")],
        acq_name="Vehicle",
    )
    mdf.save(path, overwrite=True)
    return path


def test_resolves_mdf_files(mf4_file: pathlib.Path) -> None:
    assert resolve(str(mf4_file)) is DataSource.MDF


def _provide(mf4_file: pathlib.Path) -> tuple:
    ds_type = resolve(str(mf4_file))
    factory = module.provide(
        f"{BaseModule.SOURCE_FACTORY.value}.{ds_type.value}", {"path": str(mf4_file)}
    )
    registry = module.provide(f"{BaseModule.TOPIC_REGISTRY.value}.{ds_type.value}", {})
    dataset = module.provide(f"{BaseModule.MESSAGE_DATASET.value}.{ds_type.value}", {})
    return factory, registry, dataset


def test_factory_metadata_and_bounds(mf4_file: pathlib.Path) -> None:
    factory, _, _ = _provide(mf4_file)

    assert factory.total_message_count == 40  # 20 cycles in each of 2 groups
    assert factory.end_seconds - factory.start_seconds == 9.5

    metadata = factory.metadata
    assert metadata["mdf_version"] == "4.10"
    groups = {g["name"]: g for g in metadata["channel_groups"]}
    assert set(groups) == {"EngineData", "Vehicle"}
    assert groups["EngineData"]["cycles"] == 20


def test_topic_registry_groups_channels_and_units(mf4_file: pathlib.Path) -> None:
    factory, registry, _ = _provide(mf4_file)
    mdf = factory.build()

    assert registry.available_topics(mdf) == ["EngineData", "Vehicle"]
    assert registry.native_type_name("Vehicle", mdf) == "mdf/channel_group"
    assert registry.message_count("EngineData", mdf) == 20

    struct = registry.struct("EngineData", mdf)
    assert struct.names == ["EngineSpeed", "ThrottlePos"]
    assert struct.field("EngineSpeed").metadata[b"units"] == b"rpm"

    assert "EngineSpeed" in registry.describe("EngineData", mdf)
    with pytest.raises(topic_base.TopicNotFoundError):
        registry.struct("Gearbox", mdf)


def test_sql_over_channel_group(mf4_file: pathlib.Path) -> None:
    factory, registry, dataset = _provide(mf4_file)

    relation = dataset.to_duckdb(factory, registry, ["EngineData"])
    duckdb.register("engine", relation)
    row = duckdb.sql(
        "SELECT COUNT(*) AS n, MIN(\"EngineData\"['EngineSpeed']) AS low FROM engine"
    ).fetchone()
    assert row == (20, 3000.0 - 100 * 9.5)


def test_time_window_uses_absolute_epoch_seconds(mf4_file: pathlib.Path) -> None:
    factory, registry, dataset = _provide(mf4_file)

    # The file's timestamps are relative; Bagel exposes measurement-start + offset.
    start = factory.start_seconds
    relation = dataset.to_duckdb(
        factory,
        registry,
        ["Vehicle"],
        start_seconds=start + 2.0,
        end_seconds=start + 4.0,
    )
    assert relation.shape[0] == 5  # 2.0, 2.5, 3.0, 3.5, 4.0


def test_multi_topic_relation_merges_by_time(mf4_file: pathlib.Path) -> None:
    factory, registry, dataset = _provide(mf4_file)

    relation = dataset.to_duckdb(factory, registry, ["EngineData", "Vehicle"])
    assert relation.shape[0] == 40
    columns = set(relation.columns)
    assert {"EngineData", "Vehicle"} <= columns
