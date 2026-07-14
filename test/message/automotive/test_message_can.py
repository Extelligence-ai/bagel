"""Tests for the DBC-decoded CAN adapter family, over a generated BLF capture."""

import pathlib

import duckdb
import pytest

can = pytest.importorskip("can", reason="python-can is optional (uv sync --group automotive)")
cantools = pytest.importorskip("cantools")

from src.di import module  # noqa: E402
from src.di.types.base_module import BaseModule  # noqa: E402
from src.di.types.data_source import DataSource, resolve  # noqa: E402
from src.topic import base as topic_base  # noqa: E402

DBC = """
VERSION ""
BO_ 256 EngineData: 8 ECU
 SG_ EngineSpeed : 0|16@1+ (0.25,0) [0|16383] "rpm" Vector__XXX
 SG_ ThrottlePos : 16|8@1+ (0.4,0) [0|100] "%" Vector__XXX
BO_ 512 Brakes: 8 ECU
 SG_ BrakePressure : 0|16@1+ (0.1,0) [0|6553] "bar" Vector__XXX
"""
T0 = 1662400000.0


@pytest.fixture(scope="module")
def capture(tmp_path_factory: pytest.TempPathFactory) -> tuple[pathlib.Path, pathlib.Path]:
    """A BLF capture with two DBC messages plus one frame the DBC doesn't know."""
    root = tmp_path_factory.mktemp("can")
    dbc_file = root / "vehicle.dbc"
    dbc_file.write_text(DBC)
    database = cantools.database.load_file(dbc_file)

    blf_file = root / "drive.blf"
    with can.BLFWriter(blf_file) as writer:
        for i in range(10):
            engine = database.encode_message(
                "EngineData", {"EngineSpeed": 3000 - 100 * i, "ThrottlePos": 40.0}
            )
            writer.on_message_received(
                can.Message(
                    arbitration_id=0x100, data=engine, is_extended_id=False, timestamp=T0 + i
                )
            )
        brakes = database.encode_message("Brakes", {"BrakePressure": 12.5})
        writer.on_message_received(
            can.Message(arbitration_id=0x200, data=brakes, is_extended_id=False, timestamp=T0 + 5)
        )
        # A frame whose ID is not in the DBC: skipped, not fatal.
        writer.on_message_received(
            can.Message(arbitration_id=0x7FF, data=b"\x00" * 8, timestamp=T0 + 6)
        )
    return blf_file, dbc_file


def _provide(capture: tuple[pathlib.Path, pathlib.Path]) -> tuple:
    blf_file, dbc_file = capture
    ds_type = resolve(str(blf_file))
    factory = module.provide(
        f"{BaseModule.SOURCE_FACTORY.value}.{ds_type.value}",
        {"path": str(blf_file), "dbc": str(dbc_file)},
    )
    registry = module.provide(f"{BaseModule.TOPIC_REGISTRY.value}.{ds_type.value}", {})
    dataset = module.provide(f"{BaseModule.MESSAGE_DATASET.value}.{ds_type.value}", {})
    return factory, registry, dataset


def test_resolves_blf_captures(capture: tuple[pathlib.Path, pathlib.Path]) -> None:
    assert resolve(str(capture[0])) is DataSource.CAN


def test_factory_decodes_and_skips_unknown_ids(
    capture: tuple[pathlib.Path, pathlib.Path],
) -> None:
    factory, _, _ = _provide(capture)

    assert factory.total_message_count == 11  # 10 engine + 1 brakes; unknown ID skipped
    assert factory.start_seconds == T0
    assert factory.end_seconds == T0 + 9
    assert factory.metadata["dbc_messages"] == ["EngineData", "Brakes"]


def test_topics_struct_and_units(capture: tuple[pathlib.Path, pathlib.Path]) -> None:
    factory, registry, _ = _provide(capture)
    log = factory.build()

    assert registry.available_topics(log) == ["Brakes", "EngineData"]
    assert registry.message_count("EngineData", log) == 10

    struct = registry.struct("EngineData", log)
    assert struct.names == ["EngineSpeed", "ThrottlePos"]
    assert struct.field("EngineSpeed").metadata[b"units"] == b"rpm"
    assert "BrakePressure (bar)" in registry.describe("Brakes", log)

    with pytest.raises(topic_base.TopicNotFoundError):
        registry.struct("Gearbox", log)


def test_sql_over_decoded_physical_values(capture: tuple[pathlib.Path, pathlib.Path]) -> None:
    factory, registry, dataset = _provide(capture)

    relation = dataset.to_duckdb(factory, registry, ["EngineData"])
    duckdb.register("engine", relation)
    row = duckdb.sql(
        "SELECT MAX(\"EngineData\"['EngineSpeed']) AS top, "
        "MIN(\"EngineData\"['EngineSpeed']) AS low FROM engine"
    ).fetchone()
    assert row == (3000.0, 2100.0)  # physical values: DBC scaling applied


def test_time_window(capture: tuple[pathlib.Path, pathlib.Path]) -> None:
    factory, registry, dataset = _provide(capture)

    relation = dataset.to_duckdb(
        factory, registry, ["EngineData"], start_seconds=T0 + 2, end_seconds=T0 + 4
    )
    assert relation.shape[0] == 3


def test_missing_dbc_fails_cleanly(capture: tuple[pathlib.Path, pathlib.Path]) -> None:
    from src.source.automotive.can import SourceFactory

    with pytest.raises(FileNotFoundError, match="DBC"):
        SourceFactory(str(capture[0]), dbc="./no/such.dbc")
