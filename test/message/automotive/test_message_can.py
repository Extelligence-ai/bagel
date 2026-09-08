"""Tests for the DBC-decoded CAN adapter family, over a generated BLF capture."""

import pathlib

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
    row = relation.query(
        "engine",
        "SELECT MAX(\"EngineData\"['EngineSpeed']) AS top, "
        "MIN(\"EngineData\"['EngineSpeed']) AS low FROM engine",
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


def test_cache_identity_does_not_decode_the_capture(
    capture: tuple[pathlib.Path, pathlib.Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Computing the cache key must not pay for a full capture decode.

    A new factory is constructed per query, so if identity computation decodes
    the capture, a cache *hit* still pays the decode cost just to build the key
    that would have found the hit -- largely defeating the cache for large logs.
    """
    from src.source.automotive.can import CanLog

    calls = []
    original = CanLog._decoded_frames

    def counting(self: CanLog):  # noqa: ANN202
        calls.append(True)
        yield from original(self)

    monkeypatch.setattr(CanLog, "_decoded_frames", counting)
    factory, _, _ = _provide(capture)

    factory.cache_identity  # noqa: B018 -- property access is the point of the test
    assert calls == [], "cache_identity must not decode capture frames"

    factory.cache_identity_for(factory.uuid)
    assert calls == [], "cache_identity_for must not decode capture frames"

    # Sanity check the probe itself: full metadata *does* decode (it reports
    # message counts and bounds), so a passing assertion above isn't vacuous.
    factory.metadata  # noqa: B018
    assert calls, "expected the sanity-check metadata access to decode the capture"


def _write_scaled_dbc(path: pathlib.Path, scale: float) -> None:
    path.write_text(
        f'VERSION ""\nBO_ 256 EngineData: 8 ECU\n'
        f' SG_ EngineSpeed : 0|16@1+ ({scale},0) [0|16383] "rpm" Vector__XXX\n'
    )


def test_editing_dbc_in_place_invalidates_the_cache(tmp_path: pathlib.Path) -> None:
    """A DBC edited in place (same path) must not keep serving the old scaling.

    `to_duckdb` builds a fresh `SourceFactory` per query (dependency injection
    constructs one from the raw path/dbc args each time), so this reproduces the
    real failure mode: editing a signal's scale and re-querying the same capture
    path must decode with the new DBC, not replay a cached result keyed only by
    the capture's own content.
    """
    dbc_file = tmp_path / "vehicle.dbc"
    blf_file = tmp_path / "drive.blf"
    _write_scaled_dbc(dbc_file, scale=1.0)
    database = cantools.database.load_file(dbc_file)
    with can.BLFWriter(blf_file) as writer:
        engine = database.encode_message("EngineData", {"EngineSpeed": 100})
        writer.on_message_received(
            can.Message(arbitration_id=0x100, data=engine, is_extended_id=False, timestamp=T0)
        )

    def decoded_engine_speed() -> float:
        factory, registry, dataset = _provide((blf_file, dbc_file))
        relation = dataset.to_duckdb(factory, registry, ["EngineData"])
        (value,) = relation.query(
            "engine", "SELECT \"EngineData\"['EngineSpeed'] FROM engine"
        ).fetchone()
        return value

    assert decoded_engine_speed() == 100.0  # raw counts * scale 1.0

    _write_scaled_dbc(dbc_file, scale=2.0)  # same path, edited in place
    assert decoded_engine_speed() == 200.0  # must re-decode with the new scale
